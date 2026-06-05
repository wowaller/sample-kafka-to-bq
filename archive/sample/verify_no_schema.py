import json
import logging
import os
import sys

# Disable client certificate mTLS configuration to prevent OpenSSL / pyOpenSSL conflicts
os.environ['GOOGLE_API_USE_CLIENT_CERTIFICATE'] = 'false'

from google.cloud import bigquery
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery_tools import RetryStrategy

PROJECT_ID = 'binggang-lab'
DATASET_ID = 'bwo_test_pipeline'
TABLE_NAME = 'no_schema_demo'
TABLE_SPEC = f"{PROJECT_ID}:{DATASET_ID}.{TABLE_NAME}"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
TEMP_LOCATION = f"gs://bwo-lab-us-central/temp"

class SchemaHealerDoFn(beam.DoFn):
    """
    DoFn that catches failed BQ writes, updates the BigQuery table schema
    dynamically using the BQ API, and inserts the row directly via Load Job.
    Supports table creation if it doesn't exist.
    """
    def __init__(self, project_id, dataset_id, table_name):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.table_ref = f"{project_id}.{dataset_id}.{table_name}"
        self.client = None

    def setup(self):
        self.client = bigquery.Client(project=self.project_id)

    def process(self, element):
        destination, row = element
        logging.info(f"[DLQ Healer] Received failed row from destination {destination}: {row}")
        
        # 1. Inspect row to find fields that are not in the table schema
        try:
            table = self.client.get_table(self.table_ref)
            current_columns = {field.name for field in table.schema}
            original_schema = list(table.schema)
        except Exception:
            logging.info(f"[DLQ Healer] Table {self.table_ref} does not exist yet. Proceeding with empty schema.")
            current_columns = set()
            original_schema = []
        
        new_fields = []
        for key, value in row.items():
            if key not in current_columns:
                # Infer type (for demo, default to STRING)
                new_fields.append(bigquery.SchemaField(key, 'STRING', mode='NULLABLE'))
                
        # 2. Alter schema if table exists, or prepare initial schema for creation
        target_schema = original_schema + new_fields
        if original_schema and new_fields:
            logging.info(f"[DLQ Healer] Detected new fields: {[f.name for f in new_fields]}. Updating BigQuery table schema...")
            table.schema = target_schema
            self.client.update_table(table, ['schema'])
            logging.info("[DLQ Healer] Table schema updated successfully.")
            
        # 3. Insert row directly to BigQuery using a Load Job (with schema to support creation if needed)
        logging.info(f"[DLQ Healer] Retrying row ingestion via BigQuery Load Job...")
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                schema=target_schema # Provide schema so load job can create table if missing
            )
            load_job = self.client.load_table_from_json([row], self.table_ref, job_config=job_config)
            load_job.result()  # Wait for the load job to complete
            logging.info("[DLQ Healer] Row ingested successfully after schema healing via Load Job!")
            yield row
        except Exception as e:
            logging.error(f"[DLQ Healer] Failed to insert row via Load Job: {e}")

def cleanup_table():
    client = bigquery.Client(project=PROJECT_ID)
    client.delete_table(TABLE_REF, not_found_ok=True)
    print(f"Cleanup: Deleted table {TABLE_REF}")

def run_healer_pipeline():
    print("\n=== Running Pipeline WITHOUT Schema and WITHOUT Table Pre-creation ===")
    
    test_rows = [
        {'ID': 'order_1', 'SENDER_NAME': 'Hans Müller'},
        {'ID': 'order_2', 'SENDER_NAME': 'Max Mustermann', 'NEW_MOBILE_PHONE': '491751234567'}
    ]
    
    options = PipelineOptions(flags=[], temp_location=TEMP_LOCATION)
    with beam.Pipeline(options=options) as p:
        # Write to BigQuery without schema parameter!
        write_result = (
            p
            | "Create Test Data" >> beam.Create(test_rows)
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=TABLE_SPEC,
                schema=None, # NO SCHEMA SPECIFIED!
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                insert_retry_strategy=RetryStrategy.RETRY_NEVER
            )
        )
        
        # Capture write failures and send to SchemaHealerDoFn
        healed = (
            write_result.failed_rows
            | "SchemaHealer" >> beam.ParDo(
                SchemaHealerDoFn(PROJECT_ID, DATASET_ID, TABLE_NAME)
            )
        )

def verify_results():
    print("\n=== Final Verification ===")
    client = bigquery.Client(project=PROJECT_ID)
    table = client.get_table(TABLE_REF)
    print("Table Schema Fields:")
    for field in table.schema:
        print(f"  - {field.name} ({field.field_type})")
        
    print("\nTable Contents:")
    query_job = client.query(f"SELECT * FROM `{TABLE_REF}` ORDER BY ID")
    results = query_job.result()
    for row in results:
        print("  ", dict(row.items()))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    
    # 1. Cleanup
    cleanup_table()
    
    # 2. Run pipeline
    run_healer_pipeline()
    
    # 3. Verify
    verify_results()
