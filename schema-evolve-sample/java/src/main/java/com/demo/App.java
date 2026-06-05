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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

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

    public static class JavaSchemaHealerDoFn extends DoFn<Iterable<TableRow>, TableRow> {
        private final String projectId;
        private final String datasetId;
        private final String tableName;
        private transient BigQuery bigquery;

        public JavaSchemaHealerDoFn(String projectId, String datasetId, String tableName) {
            this.projectId = projectId;
            this.datasetId = datasetId;
            this.tableName = tableName;
        }

        @Setup
        public void setup() {
            this.bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
        }

        @ProcessElement
        public void processElement(@Element Iterable<TableRow> failedRows, OutputReceiver<TableRow> receiver) {
            TableId tableId = TableId.of(datasetId, tableName);
            Table table = bigquery.getTable(tableId);
            Schema currentSchema = table.getDefinition().getSchema();

            // Collect all unique new fields across the failed rows
            List<TableFieldSchema> newFields = new ArrayList<>();
            for (TableRow row : failedRows) {
                for (String key : row.keySet()) {
                    if (currentSchema.getFields().get(key) == null) {
                        // Check if we already defined it in newFields
                        boolean alreadyDefined = false;
                        for (TableFieldSchema f : newFields) {
                            if (f.getName().equals(key)) {
                                alreadyDefined = true;
                                break;
                            }
                        }
                        if (!alreadyDefined) {
                            LOG.info("[JavaSchemaHealer] Detected new field: " + key);
                            newFields.add(new TableFieldSchema().setName(key).setType("STRING").setMode("NULLABLE"));
                        }
                    }
                }
            }

            // Perform schema update if needed
            if (!newFields.isEmpty()) {
                LOG.info("[JavaSchemaHealer] Updating schema to add " + newFields.size() + " fields.");
                List<Field> fieldsList = new ArrayList<>(currentSchema.getFields());
                for (TableFieldSchema newField : newFields) {
                    fieldsList.add(Field.of(newField.getName(), StandardSQLTypeName.valueOf(newField.getType())));
                }
                Table updatedTable = table.toBuilder()
                    .setDefinition(StandardTableDefinition.of(Schema.of(fieldsList)))
                    .build();
                bigquery.update(updatedTable);
                LOG.info("[JavaSchemaHealer] BigQuery table schema successfully updated.");
            }

            // Output all rows for re-ingestion
            for (TableRow row : failedRows) {
                receiver.output(row);
            }
        }
    }

    public static void main(String[] args) {
        DemoOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DemoOptions.class);
        Pipeline p = Pipeline.create(options);

        // Define initial schema
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("ID").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("SENDER_NAME").setType("STRING").setMode("NULLABLE"));
        TableSchema initialSchema = new TableSchema().setFields(fields);

        // Parse table details
        String outputTableSpec = options.getOutputTable();
        String[] parts = outputTableSpec.split(":");
        String project = parts[0];
        String[] datasetTable = parts[1].split("\\.");
        String dataset = datasetTable[0];
        String table = datasetTable[1];

        // 1. Main pipeline write (Storage Write API without ignoreUnknownValues)
        WriteResult writeResult = p
            .apply("ReadFromPubSub", PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
            .apply("ParseJsonToTableRow", ParDo.of(new ParseJsonToTableRowFn()))
            .apply("WriteStorageWriteApi", BigQueryIO.writeTableRows()
                .to(outputTableSpec)
                .withSchema(initialSchema)
                .withMethod(Method.STORAGE_WRITE_API)
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withTriggeringFrequency(Duration.standardSeconds(5))
                // Note: .ignoreUnknownValues() is NOT called!
            );

        // 2. Capture and Batch failures
        PCollection<TableRow> healedRows = writeResult.getFailedStorageApiInserts()
            .apply("ExtractFailedRows", MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(BigQueryStorageApiInsertError::getRow))
            // Key by table name to group
            .apply("KeyByTableSpec", WithKeys.of(outputTableSpec))
            // Group failed inserts into batches using a fixed 15-second max buffering window
            .apply("BatchFailures", GroupIntoBatches.<String, TableRow>ofSize(100)
                .withMaxBufferingDuration(Duration.standardSeconds(15)))
            .apply("ExtractValues", Values.create())
            // Perform schema healing and output TableRows for load job
            .apply("HealSchema", ParDo.of(new JavaSchemaHealerDoFn(project, dataset, table)));

        // 3. Re-ingest healed rows using BigQuery Load Jobs (FILE_LOADS)
        healedRows.apply("ReIngestViaLoadJobs", BigQueryIO.writeTableRows()
            .to(outputTableSpec)
            .withMethod(Method.FILE_LOADS)
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withTriggeringFrequency(Duration.standardSeconds(15))
        );

        p.run();
    }
}
