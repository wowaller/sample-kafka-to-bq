package com.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoSchemaUpdateApp {
    private static final Logger LOG = LoggerFactory.getLogger(AutoSchemaUpdateApp.class);

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

    public static class ParseJsonToTableRowFn extends DoFn<String, TableRow> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @ProcessElement
        public void processElement(@Element String message, OutputReceiver<TableRow> receiver) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = mapper.readValue(message, Map.class);
                TableRow row = new TableRow();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    row.set(entry.getKey(), entry.getValue());
                }
                receiver.output(row);
            } catch (IOException e) {
                LOG.error("Failed to parse JSON message: " + message, e);
            }
        }
    }

    public static void main(String[] args) {
        DemoOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DemoOptions.class);
        Pipeline p = Pipeline.create(options);

        // Ingest and write using STORAGE_WRITE_API with withAutoSchemaUpdate(true)
        WriteResult writeResult = p
            .apply("ReadFromPubSub", PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
            .apply("ParseJsonToTableRow", ParDo.of(new ParseJsonToTableRowFn()))
            .apply("WriteToBQWithAutoSchemaUpdate", BigQueryIO.writeTableRows()
                .to(options.getOutputTable())
                .withMethod(Method.STORAGE_WRITE_API)
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withTriggeringFrequency(org.joda.time.Duration.standardSeconds(5))
                .withAutoSchemaUpdate(true) // Enable auto schema update!
            );

        // Log failed rows to see the propagation errors
        writeResult.getFailedStorageApiInserts()
            .apply("ExtractFailedRows", MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(BigQueryStorageApiInsertError::getRow))
            .apply("LogFailedRows", ParDo.of(new DoFn<TableRow, Void>() {
                @ProcessElement
                public void processElement(@Element TableRow row) {
                    LOG.warn("[FailedStorageAPIInsert] Ingestion failed for row: " + row.toString());
                }
            }));

        p.run();
    }
}
