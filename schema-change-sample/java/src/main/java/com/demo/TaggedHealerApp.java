package com.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
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
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
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
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaggedHealerApp {
    private static final Logger LOG = LoggerFactory.getLogger(TaggedHealerApp.class);

    public static final TupleTag<TableRow> NORMAL_TAG = new TupleTag<TableRow>(){};
    public static final TupleTag<TableRow> DRIFTED_TAG = new TupleTag<TableRow>(){};
    public static final TupleTag<KV<String, String>> SCHEMA_UPDATE_TAG = new TupleTag<KV<String, String>>(){};

    public interface DemoOptions extends PipelineOptions {
        @Description("Pub/Sub subscription to read from")
        @Required
        String getInputSubscription();
        void setInputSubscription(String value);

        @Description("BigQuery table reference: PROJECT:DATASET.TABLE")
        @Required
        String getOutputTable();
        void setOutputTable(String value);

        @Description("Cloud Storage temp staging directory for drifted GCS files")
        @Required
        String getTempGcsDir();
        void setTempGcsDir(String value);

        @Description("Window size in seconds")
        @Required
        Integer getWindowSize();
        void setWindowSize(Integer value);
    }

    public static class TableRowToJsonFn extends SimpleFunction<TableRow, String> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public String apply(TableRow input) {
            try {
                return mapper.writeValueAsString(input);
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize TableRow to JSON string", e);
            }
        }
    }

    public static class PreWriteInspectorDoFn extends DoFn<String, TableRow> {
        private final String projectId;
        private final String datasetId;
        private final String tableName;
        private final String tableRef;
        private transient BigQuery bigquery;
        private Set<String> cache;

        public PreWriteInspectorDoFn(String projectId, String datasetId, String tableName) {
            this.projectId = projectId;
            this.datasetId = datasetId;
            this.tableName = tableName;
            this.tableRef = String.format("%s:%s.%s", projectId, datasetId, tableName);
            this.cache = new HashSet<>();
        }

        @Setup
        public void setup() {
            this.bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
            try {
                TableId tableId = TableId.of(datasetId, tableName);
                Table table = bigquery.getTable(tableId);
                Schema schema = table.getDefinition().getSchema();
                for (Field field : schema.getFields()) {
                    cache.add(field.getName().toLowerCase());
                }
                LOG.info("[Inspector] Pre-warmed schema cache: " + cache);
            } catch (Exception e) {
                LOG.warn("[Inspector] Could not pre-warm schema cache: " + e.getMessage());
            }
        }

        @ProcessElement
        public void processElement(@Element String elementStr, MultiOutputReceiver receiver) {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map;
            try {
                map = mapper.readValue(elementStr, Map.class);
            } catch (IOException e) {
                LOG.error("[Inspector] Failed to parse message JSON: " + elementStr, e);
                return;
            }

            TableRow row = new TableRow();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                row.set(entry.getKey(), entry.getValue());
            }

            Set<String> elementKeys = new HashSet<>();
            for (String key : map.keySet()) {
                elementKeys.add(key.toLowerCase());
            }

            Set<String> missingKeys = new HashSet<>(elementKeys);
            missingKeys.removeAll(cache);

            if (missingKeys.isEmpty()) {
                receiver.get(NORMAL_TAG).output(row);
                return;
            }

            // Double check BigQuery table schema
            Set<String> realMissingKeys = new HashSet<>(missingKeys);
            try {
                TableId tableId = TableId.of(datasetId, tableName);
                Table table = bigquery.getTable(tableId);
                Schema schema = table.getDefinition().getSchema();
                cache.clear();
                for (Field field : schema.getFields()) {
                    cache.add(field.getName().toLowerCase());
                }
                realMissingKeys.removeAll(cache);
            } catch (Exception e) {
                LOG.error("[Inspector] Failed to query BigQuery schema: " + e.getMessage());
            }

            if (realMissingKeys.isEmpty()) {
                receiver.get(NORMAL_TAG).output(row);
            } else {
                LOG.info("[Inspector] Schema drift detected! Missing keys: " + realMissingKeys + ". Routing to Safe Path.");
                receiver.get(DRIFTED_TAG).output(row);

                for (String key : realMissingKeys) {
                    receiver.get(SCHEMA_UPDATE_TAG).output(KV.of(tableRef, key));
                }
            }
        }
    }

    public static class SchemaUpdaterDoFn extends DoFn<KV<String, Iterable<String>>, String> {
        private final String projectId;
        private final String datasetId;
        private final String tableName;
        private transient BigQuery bigquery;

        public SchemaUpdaterDoFn(String projectId, String datasetId, String tableName) {
            this.projectId = projectId;
            this.datasetId = datasetId;
            this.tableName = tableName;
        }

        @Setup
        public void setup() {
            this.bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
        }

        @ProcessElement
        public void processElement(@Element KV<String, Iterable<String>> element, OutputReceiver<String> receiver) {
            String tableRef = element.getKey();
            Iterable<String> newColumns = element.getValue();

            TableId tableId = TableId.of(datasetId, tableName);
            Table table = bigquery.getTable(tableId);
            Schema currentSchema = table.getDefinition().getSchema();

            List<TableFieldSchema> fieldsToCreate = new ArrayList<>();
            for (String newCol : newColumns) {
                boolean exists = false;
                for (Field field : currentSchema.getFields()) {
                    if (field.getName().equalsIgnoreCase(newCol)) {
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    LOG.info("[SchemaUpdater] New column to create: " + newCol);
                    fieldsToCreate.add(new TableFieldSchema().setName(newCol).setType("STRING").setMode("NULLABLE"));
                }
            }

            if (!fieldsToCreate.isEmpty()) {
                List<Field> fieldsList = new ArrayList<>(currentSchema.getFields());
                for (TableFieldSchema schemaField : fieldsToCreate) {
                    fieldsList.add(Field.of(schemaField.getName(), StandardSQLTypeName.valueOf(schemaField.getType())));
                }
                Table updatedTable = table.toBuilder()
                    .setDefinition(StandardTableDefinition.of(Schema.of(fieldsList)))
                    .build();
                bigquery.update(updatedTable);
                LOG.info("[SchemaUpdater] BigQuery table schema updated successfully.");
            }

            receiver.output(tableRef);
        }
    }

    public static class TriggerGCSLoadJobDoFn extends DoFn<KV<String, Iterable<String>>, String> {
        private final String projectId;
        private final String datasetId;
        private final String tableName;
        private final PCollectionView<List<String>> signalView;
        private transient BigQuery bigquery;

        public TriggerGCSLoadJobDoFn(String projectId, String datasetId, String tableName, PCollectionView<List<String>> signalView) {
            this.projectId = projectId;
            this.datasetId = datasetId;
            this.tableName = tableName;
            this.signalView = signalView;
        }

        @Setup
        public void setup() {
            this.bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
        }

        @ProcessElement
        public void processElement(ProcessContext c, OutputReceiver<String> receiver) {
            // Read side input to block until schema update is ready for the window
            List<String> signals = c.sideInput(signalView);
            LOG.info("[LoadJobTrigger] Side input signals ready: " + signals);

            KV<String, Iterable<String>> element = c.element();
            String tableRef = element.getKey();
            Iterable<String> uris = element.getValue();

            List<String> urisList = new ArrayList<>();
            for (String uri : uris) {
                urisList.add(uri);
            }

            if (urisList.isEmpty()) {
                return;
            }

            LOG.info("[LoadJobTrigger] Triggering BigQuery GCS Load Job for: " + urisList);
            TableId tableId = TableId.of(datasetId, tableName);

            try {
                com.google.cloud.bigquery.LoadJobConfiguration jobConfig = com.google.cloud.bigquery.LoadJobConfiguration.newBuilder(tableId, urisList)
                    .setFormatOptions(com.google.cloud.bigquery.FormatOptions.json())
                    .setWriteDisposition(com.google.cloud.bigquery.JobInfo.WriteDisposition.WRITE_APPEND)
                    .build();

                com.google.cloud.bigquery.Job loadJob = bigquery.create(com.google.cloud.bigquery.JobInfo.of(jobConfig));
                loadJob.waitFor();
                LOG.info("[LoadJobTrigger] GCS Load Job completed successfully.");
                receiver.output(tableRef);
            } catch (Exception e) {
                LOG.error("[LoadJobTrigger] BigQuery Load Job failed: " + e.getMessage(), e);
            }
        }
    }

    public static void main(String[] args) {
        DemoOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DemoOptions.class);
        Pipeline p = Pipeline.create(options);

        // Parse table details
        String outputTableSpec = options.getOutputTable();
        String[] parts = outputTableSpec.split(":");
        String project = parts[0];
        String[] datasetTable = parts[1].split("\\.");
        String dataset = datasetTable[0];
        String table = datasetTable[1];

        // 1. Ingest and route
        PCollectionTuple splitStreams = p
            .apply("ReadFromPubSub", PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
            .apply("DecodeAndInspect", ParDo.of(new PreWriteInspectorDoFn(project, dataset, table))
                .withOutputTags(NORMAL_TAG, TupleTagList.of(DRIFTED_TAG).and(SCHEMA_UPDATE_TAG))
            );

        // 2. FAST PATH: Write normal rows to BigQuery (STORAGE_WRITE_API with auto schema update)
        splitStreams.get(NORMAL_TAG)
            .apply("WriteNormalToBQ", BigQueryIO.writeTableRows()
                .to(outputTableSpec)
                .withMethod(Method.STORAGE_WRITE_API)
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withTriggeringFrequency(Duration.standardSeconds(5))
                .withAutoSchemaUpdate(true) // Enable auto schema update!
                .ignoreUnknownValues()
            );

        // 3. SAFE PATH:
        PCollection<String> jsonStrings = splitStreams.get(DRIFTED_TAG)
            .apply("WindowDrifted", Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize()))))
            .apply("FormatAsJsonString", MapElements.via(new TableRowToJsonFn()));

        FileIO.Write<Void, String> fileWrite = FileIO.<String>write()
            .to(options.getTempGcsDir())
            .withPrefix("drifted")
            .withSuffix(".json")
            .via(TextIO.sink());

        PCollection<String> gcsFileUris = jsonStrings
            .apply("WriteDriftedToGCS", fileWrite)
            .getPerDestinationOutputFilenames()
            .apply("ExtractUris", Values.create());

        PCollection<String> schemaUpdateSignals = splitStreams.get(SCHEMA_UPDATE_TAG)
            .apply("WindowUpdates", Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize()))))
            .apply("GroupUpdatesByTableSpec", GroupByKey.create())
            .apply("ExecuteSchemaUpdateDDL", ParDo.of(new SchemaUpdaterDoFn(project, dataset, table)));

        // Create Side Input View of signals
        final PCollectionView<List<String>> signalView = schemaUpdateSignals
            .apply("CreateSignalView", View.asList());

        // 4. Sequential GCS Load: Group URIs and load after schema update finishes
        PCollection<KV<String, Iterable<String>>> windowedUris = gcsFileUris
            .apply("WindowURIs", Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize()))))
            .apply("KeyByTableSpec", WithKeys.of(outputTableSpec))
            .apply("GroupURIsByTableSpec", GroupByKey.create());

        windowedUris
            .apply("TriggerLoadJob", ParDo.of(new TriggerGCSLoadJobDoFn(project, dataset, table, signalView))
                .withSideInputs(signalView));

        p.run();
    }
}
