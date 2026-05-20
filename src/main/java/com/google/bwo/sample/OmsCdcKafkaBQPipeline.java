package com.google.bwo.sample;

import com.google.api.services.bigquery.model.TableRow;
import com.google.bwo.sample.model.OmsCdcMessage;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class OmsCdcKafkaBQPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(OmsCdcKafkaBQPipeline.class);

    // A DoFn to parse Oceanic OMS JSON strings into TableRow objects for BigQuery.
    // We emit KV<String, TableRow> where the String is the table destination identifier: "database.table"
    public static class JsonToTableRowCdcFn extends DoFn<KafkaRecord<Long, String>, KV<String, TableRow>> {
        private transient Gson gson;

        @Setup
        public void setup() {
            gson = new Gson();
        }

        @ProcessElement
        public void processElement(@Element KafkaRecord<Long, String> record, OutputReceiver<KV<String, TableRow>> out) {
            try {
                String json = record.getKV().getValue();
                OmsCdcMessage entry = gson.fromJson(json, OmsCdcMessage.class);
                
                if (entry != null && !Boolean.TRUE.equals(entry.getIsDdl())) {
                    String type = entry.getType() != null ? entry.getType().toUpperCase() : "";
                    String bqChangeType = "";
                    
                    if (type.equals("INSERT") || type.equals("UPDATE")) {
                        bqChangeType = "UPSERT";
                    } else if (type.equals("DELETE")) {
                        bqChangeType = "DELETE";
                    }

                    List<Map<String, Object>> records = entry.getData();
                    if (records != null) {
                        for (Map<String, Object> dataRecord : records) {
                            TableRow row = new TableRow();
                            // If CDC applies, set the internal _CHANGE_TYPE header for BigQuery Storage Write API CDC
                            if (!bqChangeType.isEmpty()) {
                                row.set("_CHANGE_TYPE", bqChangeType);
                            }
                            
                            // Map all fields
                            for (Map.Entry<String, Object> field : dataRecord.entrySet()) {
                                row.set(field.getKey(), field.getValue());
                            }
                            
                            String destination = entry.getDatabase() + "." + entry.getTable();
                            out.output(KV.of(destination, row));
                        }
                    }
                }
            } catch (JsonSyntaxException e) {
                // For production, push malformed JSON to a dead-letter queue for analysis.
                LOG.error("Failed to parse JSON record: {}", record.getKV().getValue(), e);
            }
        }
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(ExamplePipelineOptions.class);
        ExamplePipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(ExamplePipelineOptions.class);

        Map<String, Object> mapFromProps = new HashMap<>();
        if (!options.getKafkaComsumerProps().isEmpty()) {
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

        Pipeline pipeline = Pipeline.create(options);

        // BigQuery CDC setup via Storage Write API
        // For CDC to work, BigQuery tables *MUST* be created with Primary Key constraints.
        // And you must use BigQueryIO.Write.Method.STORAGE_WRITE_API.
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
                .apply("ParseJsonToTableRow", ParDo.of(new JsonToTableRowCdcFn()))
                .apply("WriteToBigQueryCDC",
                        BigQueryIO.<KV<String, TableRow>>write()
                                .to(new DynamicDestinations<KV<String, TableRow>, String>() {
                                    @Override
                                    public String getDestination(ValueInSingleWindow<KV<String, TableRow>> element) {
                                        // Use the database.table combination
                                        return element.getValue().getKey();
                                    }

                                    @Override
                                    public TableDestination getTable(String destination) {
                                        // We map "database.table" to BigQuery dataset target
                                        // The user's BQ project is used implicitly, but dataset mapping is: dataset.table
                                        // destination format is `UAT_LITEMAIN.CCM_OMS_ORDER`
                                        return new TableDestination(destination, "CDC table " + destination);
                                    }

                                    @Override
                                    public com.google.api.services.bigquery.model.TableSchema getSchema(String destination) {
                                        // With STORAGE_WRITE_API, ignoring schema updates here if we want BQ to handle schema dynamically? 
                                        // Or rely on ignoreUnknownValues if the table already has the schema.
                                        // We return null and use create disposition CREATE_NEVER for safety, 
                                        // or one would need a schema lookup service per table.
                                        return null; 
                                    }
                                })
                                .withFormatFunction(KV::getValue)
                                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                                // .withUseCdcDeletes() or dynamically set with _CHANGE_TYPE header via STORAGE_WRITE_API
                                // Important: We use IgnoreUnknownValues to let BQ handle incoming extra columns gracefully,
                                // or drop them, avoiding pipeline crashes on schema evolution.
                                .ignoreUnknownValues()
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withTriggeringFrequency(Duration.standardSeconds(options.getTriggeringFrequency()))
                );

        pipeline.run();
    }
}
