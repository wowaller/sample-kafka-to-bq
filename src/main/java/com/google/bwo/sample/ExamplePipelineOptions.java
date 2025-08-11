package com.google.bwo.sample;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

public interface ExamplePipelineOptions extends PipelineOptions {
    @Description("Kafka bootstrap server(s) (host:port)")
    @Required
    String getBootstrapServer();

    void setBootstrapServer(String value);

    @Description("Input Kafka topic")
    @Required
    String getTopic();

    void setTopic(String value);

    @Description("BigQuery output table spec, e.g., project:dataset.table")
    @Required
    String getBqOutputTable();

    void setBqOutputTable(String value);

    @Description("GCS location for temporary files")
    ValueProvider<String> getGcsTempLocation();
    void setGcsTempLocation(ValueProvider<String> value);

    @Description("Extra Kafka properties file.")
    @Default.String("")
    String getKafkaComsumerProps();
    void setKafkaComsumerProps(String value);

    @Description("Optional Kafka username.")
    @Default.String("")
    String getKafkaUsername();
    void setKafkaUsername(String value);

    @Description("Optional Kafka password.")
    @Default.String("")
    String getKafkaPassword();
    void setKafkaPassword(String value);

    @Description("Triggering frequency in seconds.")
    @Default.Long(15)
    Long getTriggeringFrequency();
    void setTriggeringFrequency(Long value);

    @Description("Use file_loads. Default true.")
    @Default.Boolean(true)
    Boolean getUseFileLoads();
    void setUseFileLoads(Boolean value);

    @Description("Set file shades. Default 0, autoshading.")
    @Default.Integer(0)
    Integer getFileShades();
    void setFileShades(Integer value);
}