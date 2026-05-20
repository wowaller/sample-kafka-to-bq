#!/bin/bash


# Define parameters as an array for better readability
PARAMS=(
  "readBootstrapServerAndTopic=projects/binggang-lab/locations/us-central1/clusters/test/topics/test-json"
  "persistKafkaKey=false"
  "writeMode=SINGLE_TABLE_NAME"
  "numStorageWriteApiStreams=0"
  "enableCommitOffsets=true"
  "kafkaReadOffset=latest"
  "kafkaReadAuthenticationMode=SASL_PLAIN"
  "messageFormat=JSON"
  "useBigQueryDLQ=false"
  "javascriptTextTransformReloadIntervalMinutes=0"
  "outputTableSpec=binggang-lab:jt_poc.cdc_oms_order_log"
  "kafkaReadUsernameSecretId=projects/330770447392/secrets/KAFKA_USER_NAME/versions/1"
  "kafkaReadPasswordSecretId=projects/330770447392/secrets/KAFKA_PASSWORD/versions/1"
  "javascriptTextTransformFunctionName=process"
  "consumerGroupId=bwo-lab"
  "javascriptTextTransformGcsPath=gs://bwo-lab/dataflow/udf.js"
)
# "javascriptTextTransformGcsPath=gs://bwo-lab/dataflow/udf.js"

# Join the array elements with a comma
JOINED_PARAMS=$(IFS=, ; echo "${PARAMS[*]}")

gcloud dataflow flex-template run test3 \
  --template-file-gcs-location gs://dataflow-templates-us-central1/latest/flex/Kafka_to_BigQuery_Flex \
  --region us-central1 \
  --temp-location gs://bwo-lab/temp \
  --subnetwork regions/us-central1/subnetworks/local-lab-us \
  --enable-streaming-engine \
  --additional-user-labels "" \
  --additional-experiments=enable_data_sampling \
  --update \
  --parameters "${JOINED_PARAMS}"
