# BigQuery Streaming Ingestion & Schema Evolution Guide (GCP Dataflow)

This guide provides best-practice architectures for streaming data pipelines on GCP to automatically handle schema changes (schema drift) in BigQuery tables while ensuring **Zero Data Loss**.

---

## Core Challenges: Cache Latency & Data Loss

When using BigQuery's streaming APIs (like the Storage Write API) in Apache Beam/Dataflow, schema evolution faces two primary bottlenecks:
1.  **Client-Side Serialization Constraints:** Dataflow worker nodes serialize rows using a static schema compiled at pipeline startup. If upstream messages contain new fields, the workers' serializer silently filters and discards these fields before they reach BigQuery.
2.  **Server-Side Metadata Propagation Delay:** When a table schema is updated in BigQuery, the metadata changes take minutes to propagate to all streaming ingestion worker instances. Ingesting rows containing new fields before propagation completes causes failures (or null values if `ignoreUnknownValues` is enabled), leading to data loss.

To solve these challenges, this guide provides two core architectural designs.

---

## Pattern 1: Native BigQuery `JSON` Column

*   **Mechanism:** Map all evolving, nested, or dynamically changing fields to a single column configured with the native `JSON` type (e.g., `METADATA`).
*   **How it handles updates:** Completely transparent. Any new nested fields are automatically loaded directly into the JSON column as key-value pairs. The pipeline does not require restarts, and BigQuery does not require any DDL schema changes.
*   **Data Consumption:** Downstream users query sub-fields directly in SQL using BigQuery's JSON extraction functions: `JSON_VALUE(METADATA.new_field)`.
*   **Pros:** Zero operational overhead, zero code changes, zero downtime. Extremely stable under high throughput.
*   **Cons:** No strict type enforcement at write time; validation logic shifts to the query side.

### Code Example: Native JSON Ingestion (Python)
When using `STORAGE_WRITE_API` or `STREAMING_INSERTS` for streaming writes, the value of the JSON column must be passed as a **serialized JSON string** (`json.dumps(dict)`), rather than a raw Python dictionary:

```python
import json
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery

# 1. Define schema specifying the JSON data type
json_schema = {
    'fields': [
        {'name': 'ID', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'SENDER_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'METADATA', 'type': 'JSON', 'mode': 'NULLABLE'}
    ]
}

# 2. Row mapper: serialize dynamic/nested keys into METADATA JSON column
def prepare_streaming_row(element):
    flat_keys = {'id', 'sender_name'}
    row = {}
    metadata = {}
    
    for key, value in element.items():
        if key.lower() in flat_keys:
            row[key.upper()] = value
        else:
            metadata[key] = value
            
    # For streaming insertion, pass a stringified JSON to the JSON column
    row['METADATA'] = json.dumps(metadata)
    return row

# 3. Write to BigQuery using STORAGE_WRITE_API
(
    pcoll
    | "PrepareRow" >> beam.Map(prepare_streaming_row)
    | "WriteToBQ" >> WriteToBigQuery(
        table="your-project:your_dataset.your_table",
        schema=json_schema,
        method=WriteToBigQuery.Method.STORAGE_WRITE_API,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )
)
```

*(Note: If using `Method.FILE_LOADS` for batch file loading, keep the `METADATA` value as a raw Python `dict`. Do not serialize it with `json.dumps`, as this will cause double-encoding.)*

### Code Example: Native JSON Ingestion (Java)
In Java, when using the Storage Write API to write to a JSON column, represent the nested data as a Java `Map`:

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
                // Handle parse failure
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

## Pattern 2: Pure GCS Staging & Micro-Batch Load

When handling high-throughput streams with frequent schema changes (schema drift), using streaming APIs (like the Storage Write API) is prone to data loss and `NULL` values due to metadata propagation latency and static client-side serializers.

This pattern abandons complex dual-path filtering and manual schema updating. Instead, it routes **all data** through a **"GCS Staging + Micro-Batch Load Job"** pipeline. By batch-loading files through BigQuery's load API with auto-detection enabled, the target table schema is evolved automatically at load time, with zero delay and zero data loss.

### Architectural Design
```
                      [Input Stream]
                            │
                            ▼
                [Windowing Buffer (5 min)]
                            │
                            ▼
                      [WriteToFiles]
                            │
                      [GCS JSON Files]
                            │
                            ▼
                     [Extract URIs]
                            │
                            ▼
                    (TriggerLoadJob)
                   (BigQuery Load Job)
                            │
         ┌──────────────────┴──────────────────┐
         ▼                                     ▼
(Auto-Detect Schema)                     (Append Data)
(ALLOW_FIELD_ADDITION)                   (No write-null delay)
```

### Key Highlights & Benefits
1. **Zero DDL Maintenance:** The pipeline does not require any custom code to fetch schemas, compare fields, or execute DDL updates (`update_table`). BigQuery's Load Job automatically handles detecting new columns and updating the schema.
2. **Eliminates Write-Null Vulnerabilities:** Because BigQuery Load Jobs are transactional, BigQuery performs schema detection and loading atomically. There is no cache propagation delay, ensuring all new column values are fully loaded (never written as `NULL`).
3. **Zero Ingestion Costs:** Loading data from GCS into BigQuery via Load Jobs is **completely free** (it does not incur BigQuery streaming insert charges).
4. **Quota Protection:** BigQuery limits table load jobs to **100,000 per table per day**. A window size of **1 minute to 5 minutes** is highly recommended. At a 5-minute interval, the pipeline runs only 288 load jobs per day, leaving you extremely safe from rate limit quotas.

### Complete Python Implementation:

```python
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
```

---

## Key Production Limitations: Out-of-Order Ingestion & State Inversion

While both the native JSON column (Pattern 1) and GCS Staging (Pattern 2) guarantee **zero data loss**, you must keep in mind how the ingestion pathway affects message ordering:

1. **Ordering Fidelity Comparison**:
   * **Native JSON Column (Pattern 1)**: Since rows are streamed directly to BigQuery, the insertion order matches the upstream publishing order exactly.
   * **GCS Micro-Batch Loading (Pattern 2)**: Since **all rows are routed sequentially through GCS files and loaded in order of windows**, it **completely eliminates the state-inversion risk caused by split-path routing** (Fast/Safe channel bypass). However, minor ordering changes can still occur in failure retry paths or across window boundaries.

2. **Recommended Mitigations (For Mutable Streams, like CDC)**:
   If your downstream consumption logic relies strictly on chronological ordering, you should implement **Deduplicate on Read (SQL Views)**:
   Include a timestamp column (e.g. `event_timestamp`) or incrementing version ID (`version_id`) in your source data. Downstream analytics should query a unified BigQuery view that extracts the latest state using analytic functions:
   ```sql
   SELECT * EXCEPT(row_num)
   FROM (
     SELECT *,
            ROW_NUMBER() OVER(PARTITION BY entity_id ORDER BY event_timestamp DESC) as row_num
     FROM `project.dataset.table`
   )
   WHERE row_num = 1
   ```

---
## Repository Structure & Usage Instructions

### 1. File Structure Description

```
schema-change-sample/
├── docs/
│   ├── bq_schema_update_guide.md       # English Architectural Guide
│   └── bq_schema_update_guide_cn.md    # Chinese Architectural Guide
├── python/
│   ├── demo_json_ingestion.py          # Pattern 1: JSON Column writes (Python)
│   └── demo_gcs_batch_load.py          # Pattern 2: GCS Staging writes (Python)
└── java/
    └── src/main/java/com/demo/
        ├── JsonApp.java                # Pattern 1: JSON Column writes (Java)
        └── GcsBatchLoadApp.java        # Pattern 2: GCS Staging writes (Java)
```

### 2. Run Commands

#### Run Python Pattern 2 (GCS Micro-Batch Load)

1. Activate your python virtual environment and install dependencies:
   ```bash
   pip install -r schema-change-sample/python/requirements.txt
   ```
2. Execute the following command to run the local DirectRunner pipeline:
   ```bash
   python3 -m schema-change-sample.python.demo_gcs_batch_load \
     --input_subscription projects/<PROJECT_ID>/subscriptions/<SUB_NAME> \
     --output_table <PROJECT_ID>:<DATASET_ID>.<TABLE_NAME> \
     --bq_project_id <PROJECT_ID> \
     --dataset_id <DATASET_ID> \
     --table_name <TABLE_NAME> \
     --temp_gcs_dir gs://<BUCKET_NAME>/temp/ \
     --window_size 300 \
     --runner DirectRunner
   ```

#### Run Java Pattern 2 (GCS Micro-Batch Load)

1. Compile and execute the local DirectRunner pipeline using Maven:
   ```bash
   mvn compile exec:java \
     -Dexec.mainClass=com.demo.GcsBatchLoadApp \
     -Dexec.args="--inputSubscription=projects/<PROJECT_ID>/subscriptions/<SUB_NAME> \
                  --outputTable=<PROJECT_ID>:<DATASET_ID>.<TABLE_NAME> \
                  --bqProjectId=<PROJECT_ID> \
                  --datasetId=<DATASET_ID> \
                  --tableName=<TABLE_NAME> \
                  --tempGcsDir=gs://<BUCKET_NAME>/temp/ \
                  --windowSize=300 \
                  --runner=DirectRunner"
   ```

---
## Architectural Comparison Table

To assist in choosing the correct ingestion architecture, use the following comparison of the key schema-update strategies:

| Dimension / Metric | Native JSON Column (Pattern 1) | GCS Staging & Auto-Evolution (Pattern 2) |
| :--- | :--- | :--- |
| **Data Loss Prevention** | **Zero Data Loss**. Schema-on-read preserves all fields. | **Zero Data Loss**. Staged to GCS before being atomically loaded into BQ. |
| **Max Scale Throughput** | **High**. Standard streaming writes to JSON column. | **High**. Distributed local GCS writes and offloaded BQ file loads. |
| **Implementation Complexity** | **Low**. Single dynamic row serializer. | **Low**. Straightforward streaming line with no DDL execution. |
| **BQ API Rate Limits Risk** | **Low** (no schema changes invoked). | **Low** (Jobs bound by window size; 5-min intervals are completely safe). |
| **Page Latency (Normal)** | **Near-Zero**. Streaming inserts. | ⚠️ **Window-based** (5-minute latency). |
| **Page Latency (Drift)** | **Near-Zero** (loaded instantly). | ⚠️ **Window-based** (5-minute latency). |
| **GCS Dependency** | No | Yes (Requires temporary file storage bucket). |
| **Data Ingestion Cost** | ⚠️ **Medium** (Standard Streaming API ingestion costs). | **Free** (No ingestion cost for GCS batch loading). |
| **Best Suited For** | Teams requiring real-time sub-second latency and query-side JSON handling. | Enterprise analytics pipelines requiring physical column schema evolution, zero costs, and bulletproof robustness. |
