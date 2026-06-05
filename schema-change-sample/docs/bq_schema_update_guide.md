# BigQuery Schema Updates & Evolution in Apache Beam Pipelines

When ingesting semi-structured data (such as JSON messages from Pub/Sub) into BigQuery using Apache Beam's streaming pipelines, there are three main architectural approaches to handling schema changes and dynamic fields. 

---

## Architectural Patterns

### Pattern 1: Native BigQuery `JSON` Column (Highly Recommended)

*   **Implementation:** Define a static destination table schema with a column of type `JSON` (e.g., `METADATA`). Map any dynamic, nested, or evolved fields into this single column inside the pipeline.
*   **How it handles updates:** Completely transparent. If the incoming Pub/Sub message contains new nested fields, they are loaded directly into the `JSON` column without any pipeline restarts or BigQuery table schema changes.
*   **Querying:** Consumers query the nested fields using standard BigQuery JSON functions: `JSON_VALUE(METADATA.new_field)`.
*   **Pros:** Zero downtime, zero code changes, zero operational overhead. Extremely robust at high scale.
*   **Cons:** No strict schema typing enforced on write; schema validation happens on-read.

### Pattern 2: Dead-Letter Queue (DLQ) with Schema Auto-Healer (Inline JSON Load)

*   **Implementation:** Define a strict schema in `WriteToBigQuery`. Capture write failures using the `.failed_rows` attribute of the write result, route them to a single worker in memory to determine new fields, update the BigQuery table schema using the BigQuery Client library, and re-ingest the rows via batch Load Jobs.
*   **Pros:** Automates schema updates without pipeline restarts while maintaining fully typed columns inside BigQuery.
*   **Cons:** High complexity; OOM risk on worker memory if failure rate is very high. Can lead to transient out-of-order data ingestion during update window.

### Pattern 3: Tagged Multi-Output Ingestion with GCS Buffering & WaitOn (Enterprise Scale)

*   **Implementation:** Use a dynamic check on each worker. Routinely check row keys against a local schema cache. Route normal rows to the **Fast Path** (immediate streaming writes), and route drifted rows to the **Safe Path** where they are written to temporary GCS shards in parallel. Group update notifications to run sequentially on a single thread to update the table, and trigger batch Load Jobs only after updates are finalized (`WaitOn`).
*   **Pros:** Scales horizontally without memory overhead; avoids BigQuery rate limits and concurrent update race conditions.
*   **Cons:** High complexity; requires temporary GCS storage.

---

## Pattern 1 Code Snippets: Native JSON Column Ingestion

Here are copy-pasteable implementations of the recommended **Pattern 1: Native JSON Ingestion** where dynamic fields are mapped to a `JSON` column.

### Python Ingestion Code
```python
import argparse
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def prepare_row_for_json_col(element_str):
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
            row[key.upper()] = value
        else:
            metadata[key] = value

    # BigQuery JSON column accepts a dictionary directly (do not json.dumps serialize it!)
    row['METADATA'] = metadata
    return row

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_subscription', required=True)
    parser.add_argument('--output_table', required=True)
    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)

    bq_schema = {
        'fields': [
            {'name': 'ID', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'SENDER_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'METADATA', 'type': 'JSON', 'mode': 'NULLABLE'} 
        ]
    }

    with beam.Pipeline(options=options) as p:
        _ = (
            p
            | "Read" >> beam.io.ReadFromPubSub(subscription=args.input_subscription)
            | "PrepareRow" >> beam.Map(prepare_row_for_json_col)
            | "Write" >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=bq_schema,
                create_disposition=beam.io.gcp.bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND,
                method='STREAMING_INSERTS'
            )
        )
```

### Java Ingestion Code
```java
package com.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import java.util.ArrayList;
import java.util.List;

public class JsonApp {
    public static class ParseAndMapJsonFn extends DoFn<String, TableRow> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @ProcessElement
        public void processElement(@Element String message, OutputReceiver<TableRow> receiver) {
            try {
                Map<String, Object> map = mapper.readValue(message, Map.class);
                TableRow row = new TableRow();
                Map<String, Object> metadata = new HashMap<>();

                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    String key = entry.getKey().toLowerCase();
                    if (key.equals("id") || key.equals("sender_name")) {
                        row.set(entry.getKey().toUpperCase(), entry.getValue());
                    } else {
                        metadata.put(entry.getKey(), entry.getValue());
                    }
                }
                row.set("METADATA", metadata);
                receiver.output(row);
            } catch (IOException e) {
                // Handle parsing errors
            }
        }
    }

    public static void main(String[] args) {
        DemoOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DemoOptions.class);
        Pipeline p = Pipeline.create(options);

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("ID").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("SENDER_NAME").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("METADATA").setType("JSON").setMode("NULLABLE"));
        TableSchema schema = new TableSchema().setFields(fields);

        p.apply("Read", PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
         .apply("ParseAndMapJson", ParDo.of(new ParseAndMapJsonFn()))
         .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
             .to(options.getOutputTable())
             .withSchema(schema)
             .withMethod(Method.STORAGE_WRITE_API)
             .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
             .withWriteDisposition(WriteDisposition.WRITE_APPEND)
         );

        p.run();
    }
}
```

---

## Architectural Comparison Table

To assist in choosing the correct ingestion architecture, use the following comparison of the key schema-update strategies:

| Dimension / Metric | Native JSON Column (Pattern 1) | DLQ Healer (Pattern 2) | Tagged GCS Buffer & WaitOn (Pattern 3) |
| :--- | :--- | :--- | :--- |
| **Data Loss Prevention** |   **Zero Data Loss**. Schema-on-read ensures all fields are preserved. |   **Zero Data Loss**. Catches failures and retries. |   **Zero Data Loss**. Safe staging prior to table write. |
| **Max Scale Throughput** |   **High**. Standard BQ streaming writes. | ⚠️ **Medium**. OOM risk on worker memory if failure rate is very high. |   **High**. Parallel GCS writes and offloaded BQ file loads. |
| **Implementation Complexity**|   **Low**. Just map nested objects directly. | ⚠️ **Medium** (DoFn with BQ API client). | ❌ **High** (Dynamic tagging, GCS writes, WaitOn sequencing). |
| **BQ API Rate Limits Risk** |   **Low** (no schema changes invoked). | ⚠️ **Medium** (requires Table-Keyed sharding to serialize updates). |   **Low** (Updates grouped per window, executed sequentially by single worker). |
| **Latency (Normal Rows)** |   **Near-Zero**. Streaming inserts. |   **Near-Zero**. Streaming inserts. |   **Near-Zero**. Normal rows follow Fast Path directly. |
| **Latency (Drifted Rows)** |   **Near-Zero** (loaded instantly). | ⚠️ **Low** (15-second DLQ retry loop). | ⚠️ **Low** (15-second GCS file load window). |
| **GCS Dependency** | No | No | Yes (Requires temporary file storage bucket). |
| **Best Suited For** | Teams that want zero operational overhead and don't need strict write-time typing. | Low/Mid volume pipelines where schema drift is infrequent. | High-throughput enterprise pipelines requiring absolute reliability. |
