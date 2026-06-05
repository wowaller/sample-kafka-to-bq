import json
import logging
import os
import sys
import time

# Disable client certificate mTLS configuration to prevent OpenSSL conflicts
os.environ['GOOGLE_API_USE_CLIENT_CERTIFICATE'] = 'false'

from google.cloud import bigquery
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery_tools import RetryStrategy

class GroupWindowsIntoBatches(beam.PTransform):
    def __init__(self, window_size_seconds):
        self.window_size = window_size_seconds

    def expand(self, pcoll):
        return (pcoll
                | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, time.time()))
                | "Window into Fixed Intervals" >> beam.WindowInto(window.FixedWindows(self.window_size))
                | "Groupby" >> beam.GroupByKey()
                )

class SchemaHealerDoFn(beam.DoFn):
    """
    DoFn that catches failed BQ writes as a table-keyed batch, updates the BigQuery table schema
    dynamically using the BQ API (double-checked to avoid redundant updates), 
    and inserts the batch of rows directly via a single Load Job.
    """
    def __init__(self, default_project_id):
        self.default_project_id = default_project_id
        self.client = None

    def setup(self):
        self.client = bigquery.Client(project=self.default_project_id)

    def process(self, element):
        # element is (table_spec, list_of_failed_rows)
        table_spec, batch = element
        if not batch:
            return
            
        # Parse table spec dynamically
        if ':' in table_spec:
            project_id, rest = table_spec.split(':')
            dataset_id, table_name = rest.split('.')
        else:
            project_id = self.default_project_id
            dataset_id, table_name = table_spec.split('.')
            
        table_ref = f"{project_id}.{dataset_id}.{table_name}"
        logging.info(f"[SingleWorkerHealer] Received batch of {len(batch)} failed rows for table {table_ref}.")
        
        # 1. Fetch current schema to find fields that are missing
        table = self.client.get_table(table_ref)
        current_columns = {field.name.lower() for field in table.schema}
        
        # 2. Extract new fields from the failed rows in this batch
        new_fields = []
        new_field_names = set()
        
        for row in batch:
            for key, value in row.items():
                key_lower = key.lower()
                if key_lower not in current_columns and key_lower not in new_field_names:
                    # Infer type (default to STRING for demo)
                    new_fields.append(bigquery.SchemaField(key, 'STRING', mode='NULLABLE'))
                    new_field_names.add(key_lower)
                    
        # 3. Double-Checked Update: Only call the API if new fields are actually missing
        if new_fields:
            logging.info(f"[SingleWorkerHealer] Updating table schema to add: {[f.name for f in new_fields]}")
            table.schema = list(table.schema) + new_fields
            try:
                self.client.update_table(table, ['schema'])
                logging.info("[SingleWorkerHealer] Table schema successfully updated.")
            except Exception as e:
                logging.error(f"[SingleWorkerHealer] Failed to update table schema: {e}")
            
        # 4. Re-ingest the batch via a single Load Job (which bypasses ingestion cache lag)
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            load_job = self.client.load_table_from_json(batch, table_ref, job_config=job_config)
            load_job.result()  # Blocks until load completes
            logging.info(f"[SingleWorkerHealer] Successfully re-ingested batch of {len(batch)} rows.")
            for r in batch:
                yield r
        except Exception as e:
            logging.error(f"[SingleWorkerHealer] Re-ingestion failed: {e}")

class ParsePubSubMessage(beam.DoFn):
    def process(self, payload_bytes):
        try:
            payload_str = payload_bytes.decode('utf-8')
            row = json.loads(payload_str)
            yield row
        except Exception as e:
            logging.error(f"Error parsing message: {e}")

def run():
    import argparse
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument('--input_subscription', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--project_id', required=True)
    parser.add_argument('--dataset_id', required=True)
    parser.add_argument('--table_name', required=True)
    parser.add_argument('--bq_window_size', type=int, default=15)
    
    args, pipeline_args = parser.parse_known_args()
    
    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    
    with beam.Pipeline(options=options) as p:
        write_result = (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription)
            | "ParseJSON" >> beam.ParDo(ParsePubSubMessage())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=args.output_table,
                schema=None, # Rely on existing schema & DLQ evolution
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                insert_retry_strategy=RetryStrategy.RETRY_NEVER
            )
        )
        
        # Batch failed rows by window (which outputs (table_spec, Iterable[row_dict]))
        batched_failures = (
            write_result.failed_rows
            | "WindowAndBatchFailures" >> GroupWindowsIntoBatches(args.bq_window_size)
        )
        
        # Ingest batched failures via SchemaHealerDoFn
        healed = (
            batched_failures
            | "SchemaHealer" >> beam.ParDo(
                SchemaHealerDoFn(args.project_id)
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
