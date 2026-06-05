import argparse
import logging
import json
from google.cloud import bigquery
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io import fileio
from apache_beam.io.filesystems import FileSystems

class TriggerGCSLoadJobDoFn(beam.DoFn):
    """
    DoFn that triggers a single BigQuery Load Job with autodetect and ALLOW_FIELD_ADDITION.
    """
    def __init__(self, project_id):
        self.project_id = project_id
        self.client = None

    def setup(self):
        self.client = bigquery.Client(project=self.project_id)

    def process(self, element):
        table_ref, gcs_uris = element
        uris_list = list(gcs_uris)
        if not uris_list:
            return

        logging.info(f"[LoadJobTrigger] Triggering BigQuery Load Job for {len(uris_list)} GCS files to {table_ref}...")
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=True,
                schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
            )
            # Run the load job pointing to all GCS URIs
            load_job = self.client.load_table_from_uri(uris_list, table_ref, job_config=job_config)
            load_job.result() # Wait for completion
            logging.info(f"[LoadJobTrigger] Successfully loaded GCS files into BigQuery with auto-schema evolution!")
            yield table_ref
        except Exception as e:
            logging.error(f"[LoadJobTrigger] GCS Load Job failed: {e}")

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_subscription', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--bq_project_id', required=True)
    parser.add_argument('--dataset_id', required=True)
    parser.add_argument('--table_name', required=True)
    parser.add_argument('--temp_gcs_dir', required=True)
    parser.add_argument('--window_size', type=int, default=300)
    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)

    with beam.Pipeline(options=options) as p:
        # 1. Read from Pub/Sub
        messages = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription)

        # 2. Decode bytes and parse to json dicts
        parsed_rows = (
            messages
            | "DecodeBytes" >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
        )

        # 3. Window elements
        windowed_rows = parsed_rows | "WindowRows" >> beam.WindowInto(FixedWindows(args.window_size))

        # 4. Write windowed rows as JSON files to GCS
        gcs_write = (
            windowed_rows
            | "FormatAsJsonString" >> beam.Map(lambda row: json.dumps(row))
            | "WriteToGCS" >> fileio.WriteToFiles(
                path=args.temp_gcs_dir,
                destination=lambda x: "batch",
                sink=lambda dest: fileio.TextSink()
            )
        )

        # 5. Extract GCS URIs of written files
        gcs_uris = (
            gcs_write 
            | "ExtractPaths" >> beam.Map(
                lambda file_result: FileSystems.join(args.temp_gcs_dir, file_result.file_name)
            )
        )

        # 6. Group GCS URIs in this window by Table Spec
        windowed_uris = (
            gcs_uris
            | "WindowURIs" >> beam.WindowInto(FixedWindows(args.window_size))
            | "KeyByTableSpec" >> beam.Map(lambda path, table=f"{args.bq_project_id}.{args.dataset_id}.{args.table_name}": (table, path))
            | "GroupURIs" >> beam.GroupByKey()
        )

        # 7. Trigger the Load Job
        _ = (
            windowed_uris
            | "TriggerLoadJob" >> beam.ParDo(TriggerGCSLoadJobDoFn(args.bq_project_id))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
