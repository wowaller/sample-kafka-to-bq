package com.google.bwo.sample;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleKafkaBQSqlPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(SampleKafkaBQSqlPipeline.class);

    // Define the Beam Schema that matches our data structure
    public static final Schema LOG_SCHEMA = Schema.builder()
            .addDateTimeField("timestamp")
            .addStringField("severity")
            .addStringField("service")
            .addStringField("message")
            .addStringField("trace_id")
            .addStringField("request_details")
            .addInt64Field("latency_ms")
            .build();

    @DefaultCoder(AvroCoder.class)
    public static class RequestDetails {
        String method;
        String url;
        Integer http_status;
        String user_agent;
        String ip_address;
    }

    @DefaultCoder(AvroCoder.class)
    public static class LogEntry {
        String timestamp;
        String severity;
        String service;
        String message;
        String trace_id;
        RequestDetails request_details;
        Long latency_ms;
    }

    // A DoFn to parse JSON strings into Beam Row objects for Beam SQL.
    public static class JsonToRowFn extends DoFn<KafkaRecord<Long, String>, Row> {
        private transient Gson gson;
        
        @Setup
        public void setup() {
            gson = new Gson();
        }

        @ProcessElement
        public void processElement(@Element KafkaRecord<Long, String> record, OutputReceiver<Row> out) {
            try {
                String json = record.getKV().getValue();
                LogEntry entry = gson.fromJson(json, LogEntry.class);
                if (entry != null) {
                    Instant eventTime = Instant.parse(entry.timestamp);

                    Row row = Row.withSchema(LOG_SCHEMA)
                            .addValue(eventTime)
                            .addValue(entry.severity)
                            .addValue(entry.service)
                            .addValue(entry.message)
                            .addValue(entry.trace_id)
                            .addValue(gson.toJson(entry.request_details))
                            .addValue(entry.latency_ms)
                            .build();
                    out.output(row);
                }
            } catch (JsonSyntaxException e) {
                // For production, push malformed JSON to a dead-letter queue for analysis.
                LOG.error("Failed to parse JSON record: {}", record.getKV().getValue(), e);
            }
        }
    }

    public static void main(String[] args) {
        // Parse options
        PipelineOptionsFactory.register(ExamplePipelineOptions.class);
        ExamplePipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(ExamplePipelineOptions.class);

        Map<String, Object> mapFromProps = new HashMap<>();
        // Load extra properties
        if(!options.getKafkaComsumerProps().isEmpty()) {
            Properties prop = new Properties();
            try (InputStream input = new FileInputStream(options.getKafkaComsumerProps())) {
                prop.load(input);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            mapFromProps.putAll(prop.entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> String.valueOf(e.getKey()),
                            e -> String.valueOf(e.getValue())
                    )));
        } else {
            mapFromProps.put("security.protocol", "SASL_SSL");
            mapFromProps.put("sasl.mechanism", "PLAIN");
            if (!options.getKafkaUsername().isEmpty()) {
                mapFromProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"" + options.getKafkaUsername() + "\" " +
                        "password=\"" + options.getKafkaPassword() + "\";");
            }
        }

        // Build the pipeline.
        Pipeline pipeline = Pipeline.create(options);

        int shades = options.getFileShades();
        BigQueryIO.Write<Row> bqIo = BigQueryIO.<Row>write()
                .to(options.getBqOutputTable())
                .useBeamSchema()
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withMethod(
                        options.getUseFileLoads()?
                                BigQueryIO.Write.Method.FILE_LOADS:BigQueryIO.Write.Method.STORAGE_WRITE_API
                )
                .withTriggeringFrequency(Duration.standardSeconds(options.getTriggeringFrequency()))
                .withCustomGcsTempLocation(options.getGcsTempLocation());

        // Enable auto sharding by default.
        if (shades == 0) {
            bqIo = bqIo.withAutoSharding();
        } else {
            bqIo = bqIo.withNumFileShards(shades);
        }

        pipeline
                // 1. Read from Kafka
                .apply(
                        "ReadFromKafka",
                        KafkaIO.<Long, String>read()
                                .withBootstrapServers(options.getBootstrapServer())
                                .withTopics(List.of(options.getTopic()))
                                .withKeyDeserializer(LongDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                                .withConsumerConfigUpdates(mapFromProps)
                                .commitOffsetsInFinalize()
                )
                // 2. Parse JSON into Beam Rows and set the Row schema
                .apply("ParseJsonToRow", ParDo.of(new JsonToRowFn())).setRowSchema(LOG_SCHEMA)
                // 3. Apply Beam SQL (e.g. filter out 'DEBUG' messages, just to demonstrate SQL)
                .apply(
                        "FilterAndTransformWithSQL",
                        SqlTransform.query("SELECT * FROM PCOLLECTION WHERE severity != 'DEBUG'")
                )
                // 4. Write to BigQuery using useBeamSchema()
                .apply("WriteToBigQuery", bqIo);

        pipeline.run();
    }
}
