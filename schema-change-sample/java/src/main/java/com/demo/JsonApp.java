package com.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonApp {
    private static final Logger LOG = LoggerFactory.getLogger(JsonApp.class);

    public interface DemoOptions extends PipelineOptions {
        @Description("Pub/Sub subscription to read from")
        @Required
        String getInputSubscription();
        void setInputSubscription(String value);

        @Description("BigQuery table reference: PROJECT:DATASET.TABLE")
        @Required
        String getOutputTable();
        void setOutputTable(String value);
    }

    public static class ParseAndMapJsonFn extends DoFn<String, TableRow> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @ProcessElement
        public void processElement(@Element String message, OutputReceiver<TableRow> receiver) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = mapper.readValue(message, Map.class);
                
                TableRow row = new TableRow();
                Map<String, Object> metadata = new HashMap<>();

                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    String key = entry.getKey().toLowerCase();
                    if (key.equals("id") || key.equals("sender_name")) {
                        // Store primitive keys in flat BQ columns
                        row.set(entry.getKey().toUpperCase(), entry.getValue());
                    } else {
                        // Group all dynamic/drifted/nested keys in the metadata map
                        metadata.put(entry.getKey(), entry.getValue());
                    }
                }
                
                // Map to BQ JSON column type
                row.set("METADATA", metadata);
                receiver.output(row);
            } catch (IOException e) {
                LOG.error("Failed to parse JSON message: " + message, e);
            }
        }
    }

    public static void main(String[] args) {
        DemoOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DemoOptions.class);
        Pipeline p = Pipeline.create(options);

        // Define static BQ schema with a JSON column type
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("ID").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("SENDER_NAME").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("METADATA").setType("JSON").setMode("NULLABLE"));
        TableSchema schema = new TableSchema().setFields(fields);

        p.apply("ReadFromPubSub", PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
         .apply("ParseAndMapJson", ParDo.of(new ParseAndMapJsonFn()))
         .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
             .to(options.getOutputTable())
             .withSchema(schema)
             .withMethod(Method.STORAGE_WRITE_API) // Using storage write API for fast writes
             .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
             .withWriteDisposition(WriteDisposition.WRITE_APPEND)
         );

        p.run();
    }
}
