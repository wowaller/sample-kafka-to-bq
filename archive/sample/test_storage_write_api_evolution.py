import logging
import os
import sys

# Disable client certificate mTLS configuration to prevent OpenSSL conflicts
os.environ['GOOGLE_API_USE_CLIENT_CERTIFICATE'] = 'false'

from google.cloud import bigquery
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery_tools import RetryStrategy

PROJECT_ID = 'binggang-lab'
DATASET_ID = 'bwo_test_pipeline'
TABLE_NAME = 'storage_write_api_evolution_test'
TABLE_SPEC = f"{PROJECT_ID}:{DATASET_ID}.{TABLE_NAME}"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
TEMP_LOCATION = f"gs://bwo-lab-us-central/temp"

def cleanup_table():
    client = bigquery.Client(project=PROJECT_ID)
    client.delete_table(TABLE_REF, not_found_ok=True)
    print(f"Cleanup: Deleted table {TABLE_REF}")

def pre_create_table():
    client = bigquery.Client(project=PROJECT_ID)
    schema = [
        bigquery.SchemaField('ID', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('SENDER_NAME', 'STRING', mode='NULLABLE')
    ]
    table = bigquery.Table(TABLE_REF, schema=schema)
    client.create_table(table, exists_ok=True)
    print(f"Pre-created table {TABLE_REF} with schema: ID:STRING, SENDER_NAME:STRING")

def run_pipeline():
    print(f"\n=== Running STORAGE_WRITE_API Pipeline with schema=None ===")
    
    test_rows = [
        {'ID': 'order_1', 'SENDER_NAME': 'Hans Müller'},
        {'ID': 'order_2', 'SENDER_NAME': 'Max Mustermann', 'NEW_MOBILE_PHONE': '491751234567'}
    ]
    
    options = PipelineOptions(flags=[], temp_location=TEMP_LOCATION)
    # Using STORAGE_WRITE_API runner-side write method
    with beam.Pipeline(options=options) as p:
        write_result = (
            p
            | "Create Test Data" >> beam.Create(test_rows)
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=TABLE_SPEC,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                # Try passing schemaUpdateOptions
                additional_bq_parameters={'schemaUpdateOptions': ['ALLOW_FIELD_ADDITION']},
                insert_retry_strategy=RetryStrategy.RETRY_NEVER
            )
        )
        
        # Capture and log failures if any
        _ = (
            write_result.failed_rows
            | "Log Failures" >> beam.Map(lambda x: logging.error(f"[Failure Output] Row failed: {x}"))
        )

def verify_results():
    print("\n=== Final Verification ===")
    client = bigquery.Client(project=PROJECT_ID)
    try:
        table = client.get_table(TABLE_REF)
        print("Table Schema Fields:")
        for field in table.schema:
            print(f"  - {field.name} ({field.field_type})")
            
        print("\nTable Contents:")
        query_job = client.query(f"SELECT * FROM `{TABLE_REF}` ORDER BY ID")
        results = query_job.result()
        for row in results:
            print("  ", dict(row.items()))
    except Exception as e:
        print(f"Error fetching table or querying: {e}")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    cleanup_table()
    pre_create_table()
    try:
        run_pipeline()
    except Exception as e:
        print(f"Pipeline crashed: {e}")
    verify_results()
