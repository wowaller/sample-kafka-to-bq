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
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleKafkaBQPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(SampleKafkaBQPipeline.class);

  // A POJO (Plain Old Java Object) to represent the structure of your JSON log messages.
  // This makes parsing type-safe and easy to manage.
  @DefaultCoder(AvroCoder.class)
  public static class LogEntry {
    String timestamp;
    String severity;
    String service;
    String message;
    String trace_id;
    String request_details;
    Long latency_ms;
  }

  // A DoFn to parse JSON strings into TableRow objects for BigQuery.
  public static class JsonToTableRowFn extends DoFn<String, TableRow> {
    private transient Gson gson;

    @Setup
    public void setup() {
      gson = new Gson();
    }

    @ProcessElement
    public void processElement(@Element String json, OutputReceiver<TableRow> out) {
      try {
        LogEntry entry = gson.fromJson(json, LogEntry.class);
        if (entry != null) {
          TableRow row =
              new TableRow()
                  .set("timestamp", entry.timestamp)
                  .set("severity", entry.severity)
                  .set("service", entry.service)
                  .set("message", entry.message)
                  .set("trace_id", entry.trace_id)
                  .set("request_details", entry.request_details)
                  .set("latency_ms", entry.latency_ms);
          out.output(row);
        }
      } catch (JsonSyntaxException e) {
        // For production, push malformed JSON to a dead-letter queue for analysis.
        LOG.error("Failed to parse JSON record: {}", json, e);
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

//    // Load extra properties
//    Properties prop = new Properties();
//    try (InputStream input = new FileInputStream(options.getKafkaComsumerProps())) {
//      // load a properties file
//      prop.load(input);
//    } catch (IOException ex) {
//      ex.printStackTrace();
//    }
//
//    Map<String, Object> mapFromProps = prop.entrySet().stream()
//            .collect(Collectors.toMap(
//                    e -> String.valueOf(e.getKey()),
//                    e -> String.valueOf(e.getValue())
//            ));
    Map<String, Object> mapFromProps = new HashMap<>();
    mapFromProps.put("security.protocol", "SASL_SSL");
    mapFromProps.put("sasl.mechanism", "PLAIN");
    if (options.getKafkaUsername() != null) {
      mapFromProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
              "username=\"" + options.getKafkaUsername() + "\" " +
              "password=\"" + options.getKafkaPassword() + "\"");
    } else {
      mapFromProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
              "username=\"kafka2bq@binggang-lab.iam.gserviceaccount.com\" " +
              "password=\"ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiYmluZ2dhbmctbGFiIiwKICAicHJpdmF0ZV9rZXlfaWQiOiAiZDA4YjdmZWQyOGMzMDBhNDAxZTMyOTU3N2ExNzMxMDMyMGE4ODZkMiIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuTUlJRXZRSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2N3Z2dTakFnRUFBb0lCQVFEbXdkdncrMGQxNW13TVxuMCtVSDhwNm5CaU5CdmxjYzBId2R3UVQyRWpNY1VxM3VRL0Fsc0lMWXFYU1BOTHZDTGx4WjlDMjNtcjZRWTV0Q1xuUW54anJqQUNBeUR4QzVyR0RCWDNzRnRjbVIrVkZTVXFIOEtJSmUvbEIxSWhYdVZZWjV6RFNsLzAranVqVjdCelxueHVNYTg1Y1drWGhhN1BvdTNxR2c3cHFvdWpVUDNVUEZSSFJUYzlUa2NwMEpnc0s5S2FtSWJGMUNPMjhOR0NzcVxuTFZieCtUN3BSQmg3bmVXY0tMV0hsMnlLRFBHUlhMeVhOMXRMeGtYL0ExRVpRSktkTnVScEo2YTBHbW0zZHFDYlxuQTJWQng4YTJsUlNhZFpXdTVrZnEycXVnWFlOblFnRDVXdURSY3MydWxDVXNhZWd5eWFvcUxZT285a0ZqSk5sQlxudTdDZTBBSkJBZ01CQUFFQ2dnRUFBMURXdUpuZHZjdnJCOHlvS2syY3oyb3R5Y1BZSG9VcTVydTNnWk10bjVXd1xuWTRwS1NqcGFTUGpNdGdMcG1KbUFCaUsxZGt3azVVVi9Jc0ZsTzVmeVNlY2RCZHNCMzhzUmxPZ1lkS1pMeWV3K1xuWndKdGtpQUJ3dit3ZVE1ZHV6VllSSnRJeHNnQ2k4bkdrcHFtemE3TUlCelo3RUdOT2V6dm5qVEEyQldUNDF6N1xuMzVXM1VXMVFwSU55cFpzNWJzVkI2QzB6UXZrdmdwbkdwTlpjUTRxSDBUbHFnYklHR1hKYWdKUWVKL1psalBLOVxuZTRVUEdpeHBBeEN2RlMrb1lvR056V0dyZC93eTRxbGd4T1Z5d0xQYUhCam5zRjdWSTdIc25CVWpQdk9YRXFXTFxuUWwvSFBMejc2N3NTK20vTURRRG5GK2svVHVjMFA3d0ZPcGw0MkpIOXFRS0JnUUQ2WGMvZGtCZmplRmUrWHZiK1xuVVo3ejBLcUEwNXFWWk1XZ01DSHcvc1NLeXpSb3RhQzBud2VhMUVSOWFiaE5EazVFZDRxY2IwL2I4b1lEdXRxN1xuR2xFUXFLTXJ0R0xteElqUXF5RHR4dGppNUNTR2lCWnZSZU9hd1VoSkVaWm1pR0dyemloSDJ3NjdUTTJQOVNwSFxuQi8wZkVyQlZ0UlppbTYycDZvUU80VlpzYVFLQmdRRHI4eGVaT2p1dEpSVkpqaGYrMHJsTGFPcFRpMVZ6aVF6ZlxuVk9LbWdaNWdoZnFsZURYRXZzeDE3MHBEQnBUYzN5SW0wUG10eTlleU9DaDl3anhwU2lxZlZsTGk5ZTRlcjlqalxuT0YvaUlLbTd2L001VS91VmFCa3JERTNCcFU1cTdaZWErZXBRRHM3bzFUNXI4QUd2bTBtL1h1L3c1TC9yd1A0Y1xuSjN6VTNxYU1HUUtCZ0VLVU4vYmZucnFyQWdwcURweks3ODQrcTFqZTdMalZ0NGhGeElEbC96WTVtNUpNMnBPdFxueHNQeHR0VHpERVBSamhhL0EybGVZSXBKRUVKbHFrMGZUbmR6b1JDNlE2TENtTytFMHBqb3pEeHFGeHgvakJmRFxuS2llNVJwUEh6WXN1c1dxU3hEdDNrWTlWZVVUVEdZOFNITW5xbW1Id2FpZUc1c1BNelpsM2F4Z2hBb0dBVVRmUlxuVkxZOTJvRGNUZ0J5ZEEzWEtJVUp2QTVITU1qcFBHcjh4Ykh5Y1dsTStPNWpHOEZOb2Y3bmdpRGUxeFNicmQ3YlxuUXg3YXRZY3RNUFRPVkxQcDdnVUo3UVFZbmttTGo5TlU5Z0ttU25GdGFMdG02MnYwMWVPYlZGL3htVThGazV3WlxuTXN1V0g0RmZ4a25NV3NWS1lteHNqWEJBUEFlbnNNdEs1ZjlxTnVrQ2dZRUF6Nlh3dFNPVHJhZTRMT2NvQ2tSVlxuU0d4SDRyY2c3TVB4ZXpFZGwzRkZYOHp4WnVSbmhGcnRTR1c0SmFXZWZZd3hxdVVFbTBMQy9OT0JxVjVIeVU4MVxuRldyU2t2OGNQdnpabEpkclVKMHdWUzRWWHd3MUJWdmMvVEFaa1NmOTdkQWk3UysxODNSdGtsSG85RWJPNFhudFxuQ2Q1T3gwWWNGeXJlbmdDbXk2R2gweXM9XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAia2Fma2EyYnFAYmluZ2dhbmctbGFiLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjExNDQ1MTE1MzE0NDI4MjQ5NDc4MSIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkva2Fma2EyYnElNDBiaW5nZ2FuZy1sYWIuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJ1bml2ZXJzZV9kb21haW4iOiAiZ29vZ2xlYXBpcy5jb20iCn0K\"");
    }

    // Build the pipeline.
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            "ReadFromKafka",
            KafkaIO.<Long, String>read()
                    .withBootstrapServers(options.getBootstrapServer())
                    .withTopics(List.of(options.getTopic()))
                    .withKeyDeserializer(LongDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    // withMaxReadTime makes this a bounded job for testing. Remove for a streaming pipeline.
                    .withMaxReadTime(Duration.standardSeconds(60))
                    .withConsumerConfigUpdates(mapFromProps)
        )
        // We only need the message value (the JSON string).
        .apply("ExtractMessageValue", MapElements.via(new SimpleFunction<KafkaRecord<Long, String>, String>() {
          @Override
          public String apply(KafkaRecord<Long, String> input) {
            // Return the value part of the key-value pair.
            return input.getKV().getValue();
          }
        }))
        .apply("ParseJsonToTableRow", ParDo.of(new JsonToTableRowFn()))
        .apply(
            "WriteToBigQuery",
            BigQueryIO.writeTableRows()
                .to(options.getBqOutputTable())
                .withSchema(bqSchema)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                .withAutoSharding()
                .withTriggeringFrequency(Duration.standardMinutes(15))
                .withCustomGcsTempLocation(options.getGcsTempLocation())
        );

        pipeline.run().waitUntilFinish();
    }
}
