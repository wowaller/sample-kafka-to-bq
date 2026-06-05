import argparse
import logging
import json
import random
from google.cloud import bigquery
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, Repeatedly
from apache_beam.transforms.window import FixedWindows
from apache_beam.io import fileio
from apache_beam.transforms.util import WaitOn

# Tags for multi-outputs
NORMAL_TAG = 'normal_rows'
DRIFTED_TAG = 'drifted_rows'
SCHEMA_UPDATE_TAG = 'schema_updates'

class PreWriteInspectorDoFn(beam.DoFn):
    """
    DoFn that inspects incoming elements against compile-time and run-time schemas.
    Normal rows (matching compile-time cache) are sent directly to the fast path.
    Drifted rows are routed to GCS staging (Safe Path), with schema updates triggered as needed.
    """
    def __init__(self, project_id, dataset_id, table_name, compile_time_schema):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.table_ref = f"{project_id}.{dataset_id}.{table_name}"
        self.compile_time_schema = {f.lower() for f in compile_time_schema}
        self.cache = set()
        self.client = None

    def setup(self):
        self.client = bigquery.Client(project=self.project_id)
        try:
            table = self.client.get_table(self.table_ref)
            self.cache = {field.name.lower() for field in table.schema}
            logging.info(f"[Inspector] Pre-warmed schema cache: {self.cache}")
        except Exception as e:
            logging.warning(f"[Inspector] Could not pre-warm cache, table might not exist yet: {e}")
            self.cache = set()

    def process(self, element_str):
        try:
            element = json.loads(element_str)
        except Exception as e:
            logging.error(f"[Inspector] Failed to parse message JSON: {element_str}. Error: {e}")
            return

        element_keys = {k.lower() for k in element.keys()}
        
        # Check if the row matches the static schema compiled into WriteToBigQuery
        drifted_from_compile_time = element_keys - self.compile_time_schema

        if not drifted_from_compile_time:
            # Fast Path - all keys fit the compile-time schema of the writer
            yield beam.pvalue.TaggedOutput(NORMAL_TAG, element)
            return

        # Safe Path - elements contain fields not writable by static STORAGE_WRITE_API.
        # Must write to GCS staging first.
        yield beam.pvalue.TaggedOutput(DRIFTED_TAG, element)

        # Decide if we need to emit a schema update DDL request
        missing_in_bq = element_keys - self.cache
        if missing_in_bq:
            # Double check BQ table metadata in case another worker already updated it
            try:
                table = self.client.get_table(self.table_ref)
                self.cache = {field.name.lower() for field in table.schema}
                real_missing_keys = element_keys - self.cache
            except Exception as e:
                logging.error(f"[Inspector] Failed to query BQ schema: {e}")
                real_missing_keys = missing_in_bq

            if real_missing_keys:
                logging.info(f"[Inspector] New columns detected: {real_missing_keys}. Emitting schema update request.")
                for key in real_missing_keys:
                    yield beam.pvalue.TaggedOutput(SCHEMA_UPDATE_TAG, (self.table_ref, key))



class SchemaUpdaterDoFn(beam.DoFn):
    """
    DoFn that executes schema updates. Since the input collection is grouped by table,
    updates for a table are serialized through a single worker thread shard.
    """
    def __init__(self, project_id):
        self.project_id = project_id
        self.client = None

    def setup(self):
        self.client = bigquery.Client(project=self.project_id)

    def process(self, element):
        table_ref, keys_iterable = element
        keys_to_add = list(set(keys_iterable)) # Deduplicate keys in this window
        
        logging.info(f"[SchemaUpdater] Received update requests for table {table_ref} with fields: {keys_to_add}")
        
        table = self.client.get_table(table_ref)
        current_columns = {field.name.lower() for field in table.schema}
        
        new_fields = []
        for key in keys_to_add:
            if key.lower() not in current_columns:
                # Default to nullable STRING for dynamic columns
                new_fields.append(bigquery.SchemaField(key, 'STRING', mode='NULLABLE'))
                
        if new_fields:
            logging.info(f"[SchemaUpdater] Updating BigQuery schema for {table_ref} with: {[f.name for f in new_fields]}")
            table.schema = list(table.schema) + new_fields
            self.client.update_table(table, ['schema'])
            logging.info(f"[SchemaUpdater] Schema successfully evolved for {table_ref}")
        else:
            logging.info(f"[SchemaUpdater] Schema already updated. No action required for {table_ref}")

        # Yield a completion signal element
        yield "UPDATED"


class TriggerGCSLoadJobDoFn(beam.DoFn):
    """
    DoFn that executes a single BigQuery Load Job pointing to all GCS files
    containing the drifted rows in this window.
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
            )
            # Run the load job pointing to all GCS URIs
            load_job = self.client.load_table_from_uri(uris_list, table_ref, job_config=job_config)
            load_job.result() # Wait for completion
            logging.info(f"[LoadJobTrigger] Successfully loaded GCS files into BigQuery!")
            
            # (Optional: Clean up GCS files here using GCS API if desired)
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
    parser.add_argument('--window_size', type=int, default=15)
    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)

    # Dynamic schema query for STORAGE_WRITE_API initialization
    client = bigquery.Client(project=args.bq_project_id)
    table_ref = f"{args.bq_project_id}.{args.dataset_id}.{args.table_name}"
    try:
        table = client.get_table(table_ref)
        schema_fields = []
        for field in table.schema:
            schema_fields.append({
                'name': field.name,
                'type': field.field_type,
                'mode': field.mode
            })
        bq_schema = {'fields': schema_fields}
        logging.info(f"[PipelineSetup] Retrieved schema for STORAGE_WRITE_API: {bq_schema}")
    except Exception as e:
        logging.warning(f"[PipelineSetup] Could not fetch schema. Fallback to basic. Error: {e}")
        bq_schema = {
            'fields': [
                {'name': 'ID', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'SENDER_NAME', 'type': 'STRING', 'mode': 'NULLABLE'}
            ]
        }

    with beam.Pipeline(options=options) as p:
        # Read from Pub/Sub
        messages = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription)

        # Inspect and split paths
        inspected = (
            messages
            | "DecodeAndInspect" >> beam.ParDo(
                PreWriteInspectorDoFn(
                    args.bq_project_id,
                    args.dataset_id,
                    args.table_name,
                    [f['name'] for f in bq_schema['fields']]
                )
            ).with_outputs(NORMAL_TAG, DRIFTED_TAG, SCHEMA_UPDATE_TAG)
        )

        # FAST PATH: Write normal rows directly to BigQuery
        _ = (
            inspected[NORMAL_TAG]
            | "WriteNormalToBQ" >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=bq_schema,
                create_disposition=beam.io.gcp.bigquery.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND,
                method='STORAGE_WRITE_API'
            )
        )

        # SAFE PATH:
        # 1. Write drifted rows to GCS files
        gcs_write = (
            inspected[DRIFTED_TAG]
            | "WindowDrifted" >> beam.WindowInto(FixedWindows(args.window_size))
            | "FormatAsJsonString" >> beam.Map(lambda row: json.dumps(row))
            | "WriteDriftedToGCS" >> fileio.WriteToFiles(
                path=args.temp_gcs_dir,
                destination=lambda x: "drifted",
                sink=lambda dest: fileio.TextSink()
            )
        )
        
        # Extract the resulting GCS file URIs
        from apache_beam.io.filesystems import FileSystems
        gcs_uris = (
            gcs_write 
            | "ExtractPaths" >> beam.Map(
                lambda file_result: FileSystems.join(args.temp_gcs_dir, file_result.file_name)
            )
        )

        # 2. Window and Group schema updates to run in a single worker
        schema_updates = (
            inspected[SCHEMA_UPDATE_TAG]
            | "WindowUpdates" >> beam.WindowInto(FixedWindows(args.window_size))
            | "GroupUpdates" >> beam.GroupByKey()
            | "ExecuteSchemaUpdate" >> beam.ParDo(SchemaUpdaterDoFn(args.bq_project_id))
        )

        # 3. Window GCS URIs so they match the schema update window
        windowed_uris = (
            gcs_uris
            | "WindowURIs" >> beam.WindowInto(FixedWindows(args.window_size))
            | "KeyByTableSpec" >> beam.Map(lambda path, table=f"{args.bq_project_id}.{args.dataset_id}.{args.table_name}": (table, path))
            | "GroupURIs" >> beam.GroupByKey()
        )

        # 4. Wait for the Schema Updater to emit the completion signal before loading GCS files
        _ = (
            windowed_uris
            | "WaitForSchema" >> WaitOn(schema_updates)
            | "TriggerLoadJob" >> beam.ParDo(TriggerGCSLoadJobDoFn(args.bq_project_id))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
