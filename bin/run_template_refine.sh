# ==========================================
# 1. 基础配置 (根据您的实际环境修改)
# ==========================================
export PROJECT_ID="binggang-lab"           # 您的项目 ID
export PROJECT_NUMBER="330770447392"       # 您的项目编号 (用于 Secret 路径)
export REGION="us-central1"                # 任务运行区域
export WORKER_SA="dataflow-worker@${PROJECT_ID}.iam.gserviceaccount.com"    # 任务运行账号
export JOB_NAME="kafka-to-bq-cdc"
export GCS_PATH="gs://bwo-lab/temp"        # 任务会使用的临时路径
export SUBNET_NAME="local-lab-us"           # 任务会使用的子网，这里按照子网名称修改最后的子网名字
export SUBNET="regions/${REGION}/subnetworks/${SUBNET_NAME}"
export SERVER_AND_TOPIC="projects/binggang-lab/locations/us-central1/clusters/test/topics/test-json"  # 修改为 Kafka Bootstrap地址和TOPIC名字，格式为{KAFAK BOOTSTRAP};{TOPIC}
export GROUP_ID="bwo-lab"
export BIGQUERY_DB="jt_poc" # <--- 修改为您的 BigQuery 目标表
export BIGQUERY_TBL="cdc_oms_order_log" # <--- 修改为您的 BigQuery 目标表
export KAFKA_USER_NAME_SECRET="projects/${PROJECT_NUMBER}/secrets/KAFKA_USER_NAME/versions/1" # <--- 修改为您的 Kafka Username Secret
export KAFKA_PASSWORD_SECRET="projects/${PROJECT_NUMBER}/secrets/KAFKA_PASSWORD/versions/1" # <--- 修改为您的 Kafka Password Secret
export UDF_PATH="gs://bwo-lab/dataflow/udf.js"  # <--- UDF 函数，Javascript语句，包含一个process方法 


# ==========================================
# 2. 模板参数 (标注 # <--- 的项必须检查或修改)
# ==========================================
PARAMS=(
  "readBootstrapServerAndTopic=${SERVER_AND_TOPIC}"
  "persistKafkaKey=false"
  "writeMode=SINGLE_TABLE_NAME"
  "numStorageWriteApiStreams=0"
  "enableCommitOffsets=true"
  "kafkaReadOffset=latest"
  "kafkaReadAuthenticationMode=SASL_PLAIN"
  "messageFormat=JSON"
  "useBigQueryDLQ=false"
  "javascriptTextTransformReloadIntervalMinutes=0"
  "outputTableSpec=${PROJECT_ID}:${BIGQUERY_DB}.${BIGQUERY_TBL}"
  "kafkaReadUsernameSecretId=${KAFKA_USER_NAME_SECRET}"
  "kafkaReadPasswordSecretId=${KAFKA_PASSWORD_SECRET}"
  "javascriptTextTransformFunctionName=process"
  "consumerGroupId=${GROUP_ID}"
  "javascriptTextTransformGcsPath=${UDF_PATH}"
)

JOINED_PARAMS=$(IFS='~' ; echo "${PARAMS[*]}")

# ==========================================
# 3. 提交任务
# ==========================================
gcloud dataflow flex-template run "${JOB_NAME}" \
  --template-file-gcs-location "gs://dataflow-templates-${REGION}/latest/flex/Kafka_to_BigQuery_Flex" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --service-account-email "${WORKER_SA}" \
  --temp-location "${GCS_PATH}" \
  --subnetwork "${SUBNET}" \
  --enable-streaming-engine \
  --disable-public-ips \
  --parameters "^~^${JOINED_PARAMS}"