# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

```bash
mvn clean package -Pdataflow-runner -DskipTests
```

The `dataflow-runner` Maven profile is required — it pulls in the Dataflow runner and GCP IO dependencies that are scoped as `runtime`. The build produces a fat jar at `target/test-kafka2bq-bundled-1.0.0.jar` via the shade plugin. The main class configured in the manifest is `SampleKafkaBQPipeline`.

Run tests:
```bash
mvn test
```

## Architecture

This is an Apache Beam streaming pipeline (Java 11) that reads JSON messages from Kafka and writes them to BigQuery, designed to run on Google Cloud Dataflow.

### Four pipeline variants

| Class | Purpose |
|---|---|
| `SampleKafkaBQPipeline` | Primary pipeline: Kafka JSON → `TableRow` → BigQuery (FILE_LOADS or STORAGE_WRITE_API) |
| `SampleKafkaBQSqlPipeline` | Same as above but with a Beam SQL filter step in between (demonstrates `SqlTransform`) |
| `TestUpsertKafkaBQPipeline` | Upsert variant using `STORAGE_API_AT_LEAST_ONCE` + `RowMutationInformation` for BigQuery row-level mutations; requires `trace_id` as primary key |
| `OmsCdcKafkaBQPipeline` | CDC variant for OMS (Order Management System) Debezium-style messages; uses `DynamicDestinations` to route `database.table` keys to different BQ tables via `STORAGE_WRITE_API` |

All pipelines share a single `ExamplePipelineOptions` interface. The main class in the jar manifest points to `SampleKafkaBQPipeline`; to run a different variant, invoke it explicitly with `-cp` (see `bin/run_workflow.sh`).

### Key design decisions

- **BQ write method toggle**: `--useFileLoads=true` → `FILE_LOADS`; `false` → `STORAGE_WRITE_API`. The upsert/CDC variants hardcode `STORAGE_API_AT_LEAST_ONCE` or `STORAGE_WRITE_API` regardless of this flag.
- **Kafka auth**: If `--kafkaComsumerProps` points to a `.properties` file, all entries are loaded and passed as consumer config. Otherwise, falls back to inline SASL_PLAIN using `--kafkaUsername`/`--kafkaPassword`. The `sample/producer.properties` file is an example properties file.
- **CDC tables**: `OmsCdcKafkaBQPipeline` expects BigQuery tables pre-created with primary key constraints (`CREATE_NEVER` disposition). DDL messages in the CDC stream are silently skipped (`isDdl == true`).
- **Schema**: `SampleKafkaBQPipeline` and `TestUpsertKafkaBQPipeline` define the BQ schema inline in `main()`. `SampleKafkaBQSqlPipeline` derives schema from a Beam `Schema` object via `useBeamSchema()`. A JSON-based schema parser (`parseSchema`) is also present but not wired into the main flow.
- **Sharding**: Auto-sharding is used when `--fileShades=0` (default); a fixed number of file shards is used otherwise.

### Deployment options

**Custom pipeline (direct jar submission):**
```bash
java -jar ./target/test-kafka2bq-bundled-1.0.0.jar \
  --runner=DataflowRunner \
  --project=<project> \
  --region=<region> \
  --bootstrapServer=<kafka-host:port> \
  --topic=<topic> \
  --bqOutputTable=<project:dataset.table> \
  --gcsTempLocation=<gs://bucket/path> \
  [--useFileLoads=true|false] \
  [--triggeringFrequency=600] \
  [--kafkaComsumerProps=<path-to-props-file>]
```

**Google-managed Flex Template (Kafka to BigQuery):** See `bin/run_template.sh`. This uses the pre-built `Kafka_to_BigQuery_Flex` template with a JavaScript UDF (`udf.js`) for field transformation. The UDF flattens nested JSON objects to strings.

### Archive

The `archive/bindiego/` directory contains an older Beam pipeline implementation (Elasticsearch, GCS windowed output). It is not built or used; ignore it when making changes.
