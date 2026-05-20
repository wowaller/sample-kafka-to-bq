#/bin/bash

java -cp ./target/test-kafka2bq-bundled-1.0.0.jar com.google.bwo.sample.OmsCdcKafkaBQPipeline \
  --runner=DataflowRunner \
  --project=binggang-lab \
  --region=us-central1 \
  --usePublicIps=false \
  --enableStreamingEngine=true \
  --numWorkers=8 \
  --subnetwork=https://www.googleapis.com/compute/v1/projects/binggang-lab/regions/us-central1/subnetworks/local-lab-us \
  --tempLocation=gs://bwo-lab/temp/ \
  --streaming \
  --experiments==use_runner_v2 \
  --dataflowServiceOptions=enable_streaming_engine_resource_based_billing \
  --bootstrapServer=bootstrap.test-log-sink-bq.us-central1.managedkafka.binggang-lab.cloud.goog:9092 \
  --topic=test-log \
  --bqOutputTable=binggang-lab.bwo_test_pipeline.test_log_sink \
  --gcsTempLocation=gs://bwo-lab/temp \
  --triggeringFrequency=600 \
  --useFileLoads=true \
  --kafkaComsumerProps=./sample/bq2kafka.properties \
  --fileShades=0
