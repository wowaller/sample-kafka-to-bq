package com.google.bwo.sample;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleKafkaBQPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(SampleKafkaBQPipeline.class);

    // A POJO (Plain Old Java Object) to represent the structure of your JSON log messages.
    // This makes parsing type-safe and easy to manage.
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

    // A DoFn to parse JSON strings into TableRow objects for BigQuery.
    public static class JsonToTableRowFn extends DoFn<KafkaRecord<Long, String>, TableRow> {
        private transient Gson gson;
        // Formatter for BigQuery DATETIME format (YYYY-MM-DD'T'HH:MI:SS.ssssss).
        // Using the 'T' separator is the canonical format for BigQuery.
        private static final DateTimeFormatter BQ_DATETIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        @Setup
        public void setup() {
            gson = new Gson();
        }

        @ProcessElement
        public void processElement(@Element KafkaRecord<Long, String> record, OutputReceiver<TableRow> out) {
            try {
                String json = record.getKV().getValue();
                LogEntry entry = gson.fromJson(json, LogEntry.class);
                if (entry != null) {
                    // 1. Parse the timestamp string to a Joda Instant.
                    Instant eventTime = Instant.parse(entry.timestamp);
                    // 2. Format the timestamp into the canonical string for BigQuery's DATETIME type.
                    String formattedTimestamp = BQ_DATETIME_FORMATTER.print(eventTime);

                    TableRow row =
                            new TableRow()
                                    .set("timestamp", formattedTimestamp)
                                    .set("severity", entry.severity)
                                    .set("service", entry.service)
                                    .set("message", entry.message)
                                    .set("trace_id", entry.trace_id)
                                    .set("request_details", gson.toJson(entry.request_details))
                                    .set("latency_ms", entry.latency_ms);
                    out.output(row);
                }
            } catch (JsonSyntaxException e) {
                // For production, push malformed JSON to a dead-letter queue for analysis.
                LOG.error("Failed to parse JSON record: {}", record.getKV().getValue(), e);
            }
        }
    }

    public static void main(String[] args) {
        // Parse the pipeline options passed into the application. Example:
        //   --bootstrapServer=host:port --topic1=my-topic --bqOutputTable=project:dataset.table
        // For more information, see https://beam.apache.org/documentation/programming-guide/#configuring-pipeline-options
        PipelineOptionsFactory.register(ExamplePipelineOptions.class);
        ExamplePipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(ExamplePipelineOptions.class);

        // Define the BigQuery table schema to match your target table.
        TableSchema bqSchema =
                new TableSchema()
                        .setFields(
                                List.of(
                                        new TableFieldSchema().setName("timestamp").setType("DATETIME"),
                                        new TableFieldSchema().setName("severity").setType("STRING"),
                                        new TableFieldSchema().setName("service").setType("STRING"),
                                        new TableFieldSchema().setName("message").setType("STRING"),
                                        new TableFieldSchema().setName("trace_id").setType("STRING"),
                                        new TableFieldSchema().setName("request_details").setType("STRING"),
                                        new TableFieldSchema().setName("latency_ms").setType("INTEGER")));

        Map<String, Object> mapFromProps = new HashMap<>();
        // Load extra properties
        if(!options.getKafkaComsumerProps().isEmpty()) {
            Properties prop = new Properties();
            try (InputStream input = new FileInputStream(options.getKafkaComsumerProps())) {
                // load a properties file
                prop.load(input);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            mapFromProps.putAll(prop.entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> String.valueOf(e.getKey()),
                            e -> String.valueOf(e.getValue())
                    )));
        }
        else {
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
        BigQueryIO.Write<TableRow> bqIo = BigQueryIO.writeTableRows()
                .to(options.getBqOutputTable())
                .withSchema(bqSchema)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withMethod(
                        options.getUseFileLoads()?
                                BigQueryIO.Write.Method.FILE_LOADS:BigQueryIO.Write.Method.STORAGE_WRITE_API
                )
                .withAutoSharding()
                .withTriggeringFrequency(Duration.standardSeconds(options.getTriggeringFrequency()))
                .withCustomGcsTempLocation(options.getGcsTempLocation());
        if(shades == 0) {
            bqIo = bqIo.withAutoSharding();
        } else {
            bqIo = bqIo.withNumFileShards(16);
        }

        pipeline
                .apply(
                        "ReadFromKafka",
                        KafkaIO.<Long, String>read()
                                .withBootstrapServers(options.getBootstrapServer())
                                .withTopics(List.of(options.getTopic()))
                                .withKeyDeserializer(LongDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                                // withMaxReadTime makes this a bounded job for testing. Remove for a streaming pipeline.
//                    .withMaxReadTime(Duration.standardSeconds(60))
                                .withConsumerConfigUpdates(mapFromProps)
                )
                // We only need the message value (the JSON string).
//        .apply("ExtractMessageValue", MapElements.via(new SimpleFunction<KafkaRecord<Long, String>, String>() {
//          @Override
//          public String apply(KafkaRecord<Long, String> input) {
//            // Return the value part of the key-value pair.
//            return input.getKV().getValue();
//          }
//        }))
//                .apply("ExtractMessageValue", MapElements.into(TypeDescriptors.strings()).via(records -> records.getKV().getValue()))
                .apply("ParseJsonToTableRow", ParDo.of(new JsonToTableRowFn()))
                .apply(
                        "WriteToBigQuery",
                        bqIo
                );

        pipeline.run();
    }
}
