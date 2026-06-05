package com.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.values.*;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.JobInfo.SchemaUpdateOption;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsBatchLoadApp {
    private static final Logger LOG = LoggerFactory.getLogger(GcsBatchLoadApp.class);

    public interface DemoOptions extends PipelineOptions {
        @Description("Pub/Sub subscription to read from")
        @Required
        String getInputSubscription();
        void setInputSubscription(String value);

        @Description("BigQuery table reference: PROJECT:DATASET.TABLE")
        @Required
        String getOutputTable();
        void setOutputTable(String value);

        @Description("Cloud Storage temp staging directory for batch GCS files")
        @Required
        String getTempGcsDir();
        void setTempGcsDir(String value);

        @Description("Window size in seconds")
        @Default.Integer(300)
        Integer getWindowSize();
        void setWindowSize(Integer value);
    }

    public static class StringToTableRowFn extends DoFn<String, TableRow> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @ProcessElement
        public void processElement(@Element String elementStr, OutputReceiver<TableRow> receiver) {
            try {
                TableRow row = mapper.readValue(elementStr, TableRow.class);
                receiver.output(row);
            } catch (IOException e) {
                LOG.error("[StringToTableRowFn] Failed to parse message JSON: " + elementStr, e);
            }
        }
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

    public static class TriggerGCSLoadJobDoFn extends DoFn<KV<String, Iterable<String>>, String> {
        private final String projectId;
        private final String datasetId;
        private final String tableName;
        private transient BigQuery bigquery;

        public TriggerGCSLoadJobDoFn(String projectId, String datasetId, String tableName) {
            this.projectId = projectId;
            this.datasetId = datasetId;
            this.tableName = tableName;
        }

        @Setup
        public void setup() {
            this.bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
        }

        @ProcessElement
        public void processElement(ProcessContext c, OutputReceiver<String> receiver) {
            KV<String, Iterable<String>> element = c.element();
            String tableSpec = element.getKey();
            Iterable<String> gcsUris = element.getValue();

            List<String> urisList = new ArrayList<>();
            for (String uri : gcsUris) {
                urisList.add(uri);
            }

            if (urisList.isEmpty()) {
                return;
            }

            LOG.info("[LoadJobTrigger] Triggering BigQuery GCS Load Job for: " + urisList);
            TableId tableId = TableId.of(datasetId, tableName);

            try {
                LoadJobConfiguration jobConfig = LoadJobConfiguration.newBuilder(tableId, urisList)
                    .setFormatOptions(FormatOptions.json())
                    .setWriteDisposition(WriteDisposition.WRITE_APPEND)
                    .setAutodetect(true)
                    .setSchemaUpdateOptions(List.of(SchemaUpdateOption.ALLOW_FIELD_ADDITION))
                    .build();

                com.google.cloud.bigquery.Job loadJob = bigquery.create(JobInfo.of(jobConfig));
                loadJob.waitFor();
                LOG.info("[LoadJobTrigger] GCS Load Job completed successfully.");
                receiver.output(tableSpec);
            } catch (Exception e) {
                LOG.error("[LoadJobTrigger] BigQuery Load Job failed: " + e.getMessage(), e);
            }
        }
    }

    public static void main(String[] args) {
        DemoOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DemoOptions.class);
        Pipeline p = Pipeline.create(options);

        String outputTableSpec = options.getOutputTable();
        String[] parts = outputTableSpec.split(":");
        String projectId = parts[0];
        String[] datasetTable = parts[1].split("\\.");
        String datasetId = datasetTable[0];
        String tableName = datasetTable[1];

        PCollection<String> messages = p.apply("ReadFromPubSub",
            PubsubIO.readStrings().fromSubscription(options.getInputSubscription()));

        PCollection<TableRow> rows = messages.apply("ParseToTableRow",
            ParDo.of(new StringToTableRowFn()));

        PCollection<TableRow> windowedRows = rows.apply("WindowRows",
            Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize()))));

        String tempGcsDir = options.getTempGcsDir();

        PCollection<String> jsonRows = windowedRows.apply("ConvertToJsonString",
            MapElements.via(new TableRowToJsonFn()));

        FileIO.Write.Result writeResult = jsonRows.apply("WriteToGCS",
            FileIO.<String>write()
                .via(TextIO.sink())
                .to(tempGcsDir)
                .withNumShards(1)
                .withNaming(new FileIO.Write.FileNaming() {
                    @Override
                    public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
                        return String.format("batch-%s-%d-of-%d.json", window.maxTimestamp().toString().replace(":", "-"), shardIndex, numShards);
                    }
                }));

        PCollection<String> gcsUris = writeResult.getPerDestinationOutputFilenames()
            .apply("ExtractPaths", MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getValue()));

        PCollection<KV<String, Iterable<String>>> windowedUris = gcsUris
            .apply("WindowURIs", Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize()))))
            .apply("KeyByTableSpec", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(path -> KV.of(outputTableSpec, path)))
            .apply("GroupURIs", GroupByKey.create());

        windowedUris.apply("TriggerLoadJob",
            ParDo.of(new TriggerGCSLoadJobDoFn(projectId, datasetId, tableName)));

        p.run();
    }
}
