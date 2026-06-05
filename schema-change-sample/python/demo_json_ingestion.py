import argparse
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def prepare_row_for_json_col(element_str):
    """
    Parses incoming JSON string and dynamically splits fields:
    - Primitives 'id' and 'sender_name' mapped to flat BQ columns.
    - All other dynamic/nested fields mapped to a single 'METADATA' JSON column.
    """
    try:
        element = json.loads(element_str)
    except Exception as e:
        logging.error(f"[Parser] Failed to parse message JSON: {element_str}. Error: {e}")
        return

    flat_keys = {'id', 'sender_name'}
    row = {}
    metadata = {}

    for key, value in element.items():
        if key.lower() in flat_keys:
            # Store in corresponding uppercase flat column
            row[key.upper()] = value
        else:
            # Group any drifted/dynamic/nested fields into the metadata dictionary
            metadata[key] = value

    # BigQuery JSON column expects a JSON-serialized string for Storage Write API / Streaming Inserts
    row['METADATA'] = json.dumps(metadata)
    return row

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_subscription', required=True)
    parser.add_argument('--output_table', required=True)
    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)

    # Define the static BigQuery schema containing the native JSON column
    bq_schema = {
        'fields': [
            {'name': 'ID', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'SENDER_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'METADATA', 'type': 'JSON', 'mode': 'NULLABLE'} # Native BQ JSON column type
        ]
    }

    with beam.Pipeline(options=options) as p:
        _ = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription)
            | "PrepareRow" >> beam.Map(prepare_row_for_json_col)
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=bq_schema,
                create_disposition=beam.io.gcp.bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND,
                method='STREAMING_INSERTS'
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
