import json
import argparse
import logging
import os

# Disable client certificate mTLS configuration to prevent OpenSSL / pyOpenSSL conflicts on gLinux
os.environ['GOOGLE_API_USE_CLIENT_CERTIFICATE'] = 'false'

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions

def get_initial_schema():
    return {
        'fields': [
            {'name': 'ID', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'SENDER_JSON', 'type': 'JSON', 'mode': 'NULLABLE'},
            {'name': 'SENDER_RECORD', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
                {'name': 'SENDER_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'SENDER_COMPANY', 'type': 'STRING', 'mode': 'NULLABLE'}
            ]}
        ]
    }

def get_updated_schema():
    return {
        'fields': [
            {'name': 'ID', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'SENDER_JSON', 'type': 'JSON', 'mode': 'NULLABLE'},
            {'name': 'SENDER_RECORD', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
                {'name': 'SENDER_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'SENDER_COMPANY', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'SENDER_MOBILE_PHONE', 'type': 'STRING', 'mode': 'NULLABLE'}  # New nested sub-field
            ]},
            {'name': 'NEW_TOP_LEVEL_FIELD', 'type': 'STRING', 'mode': 'NULLABLE'}     # New top-level field
        ]
    }

def get_versioned_schema():
    return {
        'fields': [
            {'name': 'ID', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'SENDER_JSON', 'type': 'JSON', 'mode': 'NULLABLE'},
            {'name': 'SENDER_RECORD', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
                {'name': 'SENDER_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'SENDER_COMPANY', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'SENDER_MOBILE_PHONE', 'type': 'STRING', 'mode': 'NULLABLE'}
            ]},
            {'name': 'NEW_TOP_LEVEL_FIELD', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'NEW_TOP_LEVEL_FIELD_V2', 'type': 'INTEGER', 'mode': 'NULLABLE'}  # New version with different type!
        ]
    }

class ParsePubSubMessage(beam.DoFn):
    def process(self, payload_bytes):
        try:
            payload_str = payload_bytes.decode('utf-8')
            row = json.loads(payload_str)
            yield row
        except Exception as e:
            logging.error(f"Error parsing message: {e}")

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--schema_type', required=True, choices=['initial', 'updated', 'versioned'])
    parser.add_argument('--input_subscription', required=True)
    parser.add_argument('--output_table', required=True)
    
    # Parse known args, leaving beam pipeline options to pipeline_options
    args, pipeline_args = parser.parse_known_args()
    
    # Set streaming mode explicitly
    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    
    if args.schema_type == 'initial':
        schema = get_initial_schema()
    elif args.schema_type == 'updated':
        schema = get_updated_schema()
    else:
        schema = get_versioned_schema()
        
    logging.info(f"Running streaming pipeline with schema_type: {args.schema_type}")
    
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription)
            | "ParseJSON" >> beam.ParDo(ParsePubSubMessage())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=args.output_table,
                schema=schema,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                triggering_frequency=15, # trigger loads frequently for testing
                additional_bq_parameters={'schemaUpdateOptions': ['ALLOW_FIELD_ADDITION']}
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
