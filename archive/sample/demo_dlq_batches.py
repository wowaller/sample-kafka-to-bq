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
TABLE_NAME = 'dlq_batches_demo'
TABLE_SPEC = f"{PROJECT_ID}:{DATASET_ID}.{TABLE_NAME}"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
TEMP_LOCATION = f"gs://bwo-lab-us-central/temp"

class SchemaHealerDoFn(beam.DoFn):
    """
    DoFn that catches failed BQ writes, updates the BigQuery table schema
    dynamically using the BQ API, and inserts the row directly via a Load Job.
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
        logging.info(f"[DLQ Healer] Received failed row: {row}")
        
        # 1. Fetch current schema to find fields that are missing
        table = self.client.get_table(self.table_ref)
        current_columns = {field.name for field in table.schema}
        
        new_fields = []
        for key, value in row.items():
            if key not in current_columns:
                # Infer type (default to STRING for demo)
                new_fields.append(bigquery.SchemaField(key, 'STRING', mode='NULLABLE'))
                
        if new_fields:
            logging.info(f"[DLQ Healer] Detected new fields: {[f.name for f in new_fields]}. Updating BQ schema...")
            table.schema = list(table.schema) + new_fields
            self.client.update_table(table, ['schema'])
            logging.info("[DLQ Healer] Table schema updated successfully.")
            
        # 2. Re-ingest row via Load Job to bypass streaming schema propagation cache delay
        logging.info(f"[DLQ Healer] Retrying row ingestion via BigQuery Load Job...")
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            load_job = self.client.load_table_from_json([row], self.table_ref, job_config=job_config)
            load_job.result()  # Blocks until the load job completes
            logging.info("[DLQ Healer] Row ingested successfully after schema healing!")
            yield row
        except Exception as e:
            logging.error(f"[DLQ Healer] Failed to insert row: {e}")

def cleanup_table():
    client = bigquery.Client(project=PROJECT_ID)
    client.delete_table(TABLE_REF, not_found_ok=True)
    print(f"Cleanup: Deleted table {TABLE_REF}")

def pre_create_table():
    client = bigquery.Client(project=PROJECT_ID)
    schema = [
        bigquery.SchemaField('ID', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('SENDER_NAME', 'STRING', mode='NULLABLE')
    ]
    table = bigquery.Table(TABLE_REF, schema=schema)
    client.create_table(table, exists_ok=True)
    print(f"Pre-created table {TABLE_REF} with schema: ID:STRING, SENDER_NAME:STRING")

def run_batch_pipeline(batch_id, rows):
    print(f"\n=== Running Pipeline for Batch {batch_id} ===")
    options = PipelineOptions(flags=[], temp_location=TEMP_LOCATION)
    with beam.Pipeline(options=options) as p:
        write_result = (
            p
            | f"Create Batch {batch_id} Data" >> beam.Create(rows)
            | f"WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=TABLE_SPEC,
                schema=None, # Set schema=None so it queries existing table schema
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                insert_retry_strategy=RetryStrategy.RETRY_NEVER
            )
        )
        
        # Capture write failures and send to SchemaHealerDoFn
        healed = (
            write_result.failed_rows
            | f"SchemaHealer_Batch_{batch_id}" >> beam.ParDo(
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
    
    # 1. Start clean
    cleanup_table()
    pre_create_table()
    
    # 2. Batch 1: Ingest normal row + row with 'FIELD_BATCH_1'
    batch_1_rows = [
        {'ID': 'order_1', 'SENDER_NAME': 'Alice'},
        {'ID': 'order_2', 'SENDER_NAME': 'Bob', 'FIELD_BATCH_1': 'val_1'}
    ]
    run_batch_pipeline(1, batch_1_rows)
    
    # 3. Batch 2: Ingest row with 'FIELD_BATCH_2'
    batch_2_rows = [
        {'ID': 'order_3', 'SENDER_NAME': 'Charlie', 'FIELD_BATCH_2': 'val_2'}
    ]
    run_batch_pipeline(2, batch_2_rows)
    
    # 4. Batch 3: Ingest row with 'FIELD_BATCH_3'
    batch_3_rows = [
        {'ID': 'order_4', 'SENDER_NAME': 'David', 'FIELD_BATCH_3': 'val_3'}
    ]
    run_batch_pipeline(3, batch_3_rows)
    
    # 5. Final validation query
    verify_results()
