package com.google.bwo.sample;

import com.google.api.services.bigquery.model.TableConstraints;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.*;
import java.util.stream.Collectors;

public class TestUpsertKafkaBQPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(TestUpsertKafkaBQPipeline.class);

    private static final String BIGQUERY_SCHEMA = "BigQuery Schema";
    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String MODE = "mode";
    private static final String RECORD_TYPE = "RECORD";
    private static final String FIELDS_ENTRY = "fields";

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

    /** Parse BigQuery schema from a Json file. */
    private static TableSchema parseSchema(String jsonPath) {
        TableSchema tableSchema = new TableSchema();
        List<TableFieldSchema> fields = new ArrayList<>();

        JSONObject jsonSchema = parseJson(jsonPath);

        JSONArray bqSchemaJsonArray = jsonSchema.getJSONArray(BIGQUERY_SCHEMA);

        for (int i = 0; i < bqSchemaJsonArray.length(); i++) {
            JSONObject inputField = bqSchemaJsonArray.getJSONObject(i);
            fields.add(convertToTableFieldSchema(inputField));
        }
        tableSchema.setFields(fields);

        return tableSchema;
    }

    /**
     * Parses a JSON file and returns a JSONObject containing the necessary source, sink, and schema
     * information.
     *
     * @param pathToJson the JSON file location so we can download and parse it
     * @return the parsed JSONObject
     */
    private static JSONObject parseJson(String pathToJson) {
        try {
            // accessing GCS needs to be done after the pipeline create call, otherwise FileSystems
            // doesn't know about GCS.
            ReadableByteChannel readableByteChannel =
                    FileSystems.open(FileSystems.matchNewResource(pathToJson, false));
            String json =
                    new String(
                            StreamUtils.getBytesWithoutClosing(Channels.newInputStream(readableByteChannel)));
            return new JSONObject(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert a JSONObject from the Schema JSON to a TableFieldSchema. In case of RECORD, it handles
     * it recursively.
     *
     * @param inputField Input field to convert.
     * @return TableFieldSchema instance to populate the schema.
     */
    private static TableFieldSchema convertToTableFieldSchema(JSONObject inputField) {
        TableFieldSchema field =
                new TableFieldSchema()
                        .setName(inputField.getString(NAME))
                        .setType(inputField.getString(TYPE));

        if (inputField.has(MODE)) {
            field.setMode(inputField.getString(MODE));
        }

        if (inputField.getString(TYPE) != null && inputField.getString(TYPE).equals(RECORD_TYPE)) {
            List<TableFieldSchema> nestedFields = new ArrayList<>();
            JSONArray fieldsArr = inputField.getJSONArray(FIELDS_ENTRY);
            for (int i = 0; i < fieldsArr.length(); i++) {
                JSONObject nestedJSON = fieldsArr.getJSONObject(i);
                nestedFields.add(convertToTableFieldSchema(nestedJSON));
            }
            field.setFields(nestedFields);
        }

        return field;
    }

    // A DoFn to parse JSON strings into TableRow objects for BigQuery.
    public static class JsonToTableRowFn extends DoFn<KafkaRecord<Long, String>, TableRow> {
        private transient Gson gson;
        // Formatter for BigQuery DATETIME format (YYYY-MM-DD HH:MM:SS[.SSSSSS]).
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
        // For more information, see https://beam.apache.org/documentation/programming-guide/#configuring-pipeline-options
        PipelineOptionsFactory.register(ExamplePipelineOptions.class);
        ExamplePipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(ExamplePipelineOptions.class);

        // Define the BigQuery table schema to match your target table.
        TableSchema bqSchema =
                new TableSchema()
                    .setFields(List.of(
                        new TableFieldSchema().setName("timestamp").setType("DATETIME"),
                        new TableFieldSchema().setName("severity").setType("STRING"),
                        new TableFieldSchema().setName("service").setType("STRING"),
                        new TableFieldSchema().setName("message").setType("STRING"),
                        new TableFieldSchema().setName("trace_id").setType("STRING"),
                        new TableFieldSchema().setName("request_details").setType("STRING"),
                        new TableFieldSchema().setName("latency_ms").setType("INTEGER")))
                ;

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
                .withPrimaryKey(List.of("trace_id"))
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withTriggeringFrequency(Duration.standardSeconds(options.getTriggeringFrequency()))
                .withRowMutationInformationFn(row ->
                        RowMutationInformation.of(
                                RowMutationInformation.MutationType.UPSERT, Long.toHexString(System.currentTimeMillis())))
                .withCustomGcsTempLocation(options.getGcsTempLocation());



        // Enable auto sharding by default.
        if(shades == 0) {
            bqIo = bqIo.withAutoSharding();
        } else {
            bqIo = bqIo.withNumFileShards(shades);
        }

        pipeline
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
                .apply("ParseJsonToTableRow", ParDo.of(new JsonToTableRowFn()))
                .apply(
                        "WriteToBigQuery",
                        bqIo
                );

        pipeline.run();
    }
}
