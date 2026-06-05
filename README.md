# Sample-kafka-to-bq

The code was built against following JSON data:
```json
{
"timestamp": "2025-08-07T22:15:30.123Z",
"severity": "INFO",
"service": "api-gateway-service",
"message": "User request processed successfully.",
"trace_id": "7f13b9c8d1e2f3a4b5c6d7e8f9a0b1c2",
"request_details": {
"method": "GET",
"url": "/api/v1/users/123",
"http_status": 200,
"user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
"ip_address": "192.0.2.146"
},
"latency_ms": 45
}
```

## Compilation:
```bash
mvn clean package -Pdataflow-runner -DskipTests
```


## Submit the Job
Gcloud authentication:
The cloud can be submit through cloud shell. If not, you may want to use gcloud to authenticate first.
```bash
gcloud auth application-default login
```
Job submission:
```bash
java -jar ./target/test-kafka2bq-bundled-1.0.0.jar \
--runner=DataflowRunner \
--project=[your-project-name] \
--region=[your-region-here] \
--usePublicIps=false \
--enableStreamingEngine=true \
--numWorkers=[your-initial-worker] \
--subnetwork=https://www.googleapis.com/compute/v1/projects/[your-project-id]/regions/[your-region-id]/subnetworks/[your-subnet-name] \
--tempLocation=[your-gcs-temp-location] \
--streaming \
--experiments==use_runner_v2 \
--dataflowServiceOptions=enable_streaming_engine_resource_based_billing \
--bootstrapServer=[your-kafka-server] \
--topic=[your-kafka-topic] \
--bqOutputTable=[your-bq-table] \
--gcsTempLocation=[your-gcs-temp-location] \
--triggeringFrequency=600 \
--useFileLoads=true \
--kafkaComsumerProps=[your-kafka-consumer-properties]
```


## Program parameters:

### Parameter
```text
--bootstrapServer: Kafka connection information. Required
--topic: Kafka topic. Required
--bqOutputTable: BQ table to write to. Required.
--gcsTempLocation: Temp gcs location for. Required.
--useFileLoads: Use the file load options to write to bq. If not specified, it will use the storage write API. Optional.
--triggeringFrequency: The writing interval in seconds to write to BQ. Optional.
--useFileLoads: Default true. If true, use FILE_LOADS when writing to BQ. If false, use STORAGE_WRITE_API instead. Optional.
--kafkaComsumerProps: Extra kafka consumer properties file. Especially useful when different authentication methods. Optional.
```


### Dataflow/Beam parameters:
```text
--runner: Run with dataflow. Do not change. Required.
--project: The Google cloud project name. Required.
--region: The Google cloud region. Required.
--usePublicIps: Set to false if you do not need to access internet. Optional.
--enableStreamingEngine: Necessary for resource_based_billing and better auto scaling. Do not change. Required.
--numWorkers: The initial number of workers to avoid cold start. Auto scaling will then change number of workers according to CPU and backlog. Optional.
--subnetwork: If not using default VPC/subnet, the subnet is required. The format is https://www.googleapis.com/compute/v1/projects/[your-project-id]/regions/[your-region-id]/subnetworks/[your-subnet-name] . Optional.
--tempLocation: Temp location of the dataflow job. Optional.
--experiments: Add use_runner_v2 to enable runner v2. Reference: https://cloud.google.com/dataflow/docs/runner-v2. Optional.
--dataflowServiceOptions: Use the enable_streaming_engine_resource_based_billing to optimize the billing. You can add more options but keep the enable_streaming_engine_resource_based_billing option for costs. Optional.
```

---

## Schema Evolution Samples

This repository contains verified, production-ready implementation patterns for automatically updating the BigQuery destination table schema when upstream schema drift occurs, guaranteeing **Zero Data Loss**.

For detailed architecture descriptions, comparison tables, and implementation notes, please reference the guides:
*   [English Guide: BigQuery Schema Evolution Walkthrough](file:///usr/local/google/home/binggangwo/project/sample-kafka-to-bq/schema-evolve-sample/docs/bq_schema_update_guide.md)
*   [中文指南: BigQuery 表结构演进与零丢失写入设计](file:///usr/local/google/home/binggangwo/project/sample-kafka-to-bq/schema-evolve-sample/docs/bq_schema_update_guide_cn.md)

### Folder Structure Overview
*   [schema-evolve-sample/docs/](file:///usr/local/google/home/binggangwo/project/sample-kafka-to-bq/schema-evolve-sample/docs): Documentation guides.
*   [schema-evolve-sample/python/](file:///usr/local/google/home/binggangwo/project/sample-kafka-to-bq/schema-evolve-sample/python):
    *   `demo_tagged_healer.py`: Advanced multi-worker Python pipeline utilizing tagged multi-output streams to partition normal rows from drifted rows, buffer drifted records safely to Cloud Storage (GCS), serialize schema updates, and load GCS files via batch load jobs after update finalization (`WaitOn`).
    *   `demo_dlq_streaming.py`: Basic DLQ schema healer that routes write failures to a single worker in memory to run table schema updates and retries rows via inline JSON Load Jobs.
*   [schema-evolve-sample/java/](file:///usr/local/google/home/binggangwo/project/sample-kafka-to-bq/schema-evolve-sample/java):
    *   `App.java`: Java Apache Beam pipeline utilizing the Storage Write API with failures captured from `getFailedStorageApiInserts()`, batched in 15-second windows, healed sequentially via the BigQuery Client API, and loaded via BigQuery File Loads.

### Relocated JavaScript UDFs
All JavaScript UDF helpers and tests have been grouped under the [udf/](file:///usr/local/google/home/binggangwo/project/sample-kafka-to-bq/udf) folder at the project root.


