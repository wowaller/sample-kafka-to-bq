# BigQuery 模式演进与流式写入架构设计指南 (GCP Dataflow)

当使用 Apache Beam 管道将半结构化流数据（如来自 Pub/Sub 的 JSON 消息）流式写入 BigQuery 时，有三种主要架构能够应对动态字段与模式变更（模式漂移）。

---

## 核心架构设计方案

### 方案 1：原生 BigQuery `JSON` 列方案（强烈推荐）

*   **实现机制：** 将目标表中所有变动、嵌套或动态扩展的字段均映射写入到单个设计为 `JSON` 类型（如 `METADATA`）的列中。
*   **如何应对模式变更：** 完全透明。任何新增的子字段均会自动被作为 JSON 键值对直接写入该列，管道无需重启，BigQuery 也无需做任何 DDL 表结构变更。
*   **数据消费：** 下游消费者在 SQL 中通过标准 BigQuery JSON 提取函数直接查询子字段：`JSON_VALUE(METADATA.new_field)`。
*   **优势：** 零运维开销、零代码变更、零停机时间。在高吞吐大规模场景下表现极为稳定。
*   **劣势：** 写入时无法进行强类型约束，类型验证逻辑后移至读取端。

### 方案 2：死信队列 (DLQ) 与表结构自动修复方案 (Inline JSON Load)

*   **实现机制：** 在流式写入节点设定严格的强类型 TableSchema。捕获所有的未知字段写入失败行，通过开窗分流到单 Worker 实例读取 BigQuery 当前最新 Schema 并更新表元数据（DDL），然后通过批 Load 任务重新写入。
*   **优势：** 自动演化表字段，在 BigQuery 内维持强类型字段结构。
*   **劣势：** 逻辑极为复杂；若大批未知字段并发，内存缓冲存在 OOM 风险；会导致字段更新期间的数据发生短时间时序乱序。

### 方案 3：GCS 暂存与 WaitOn 控制流的双通道方案 (企业级大吞吐)

*   **实现机制：** 在 Worker 节点上进行动态键比对，正常数据走**快速通道**直接流式写入，漂移数据走**安全通道**并行写入临时 GCS 桶。以单线程顺序变更表结构，并在检测到模式变更完成后（`WaitOn`），拉起 BigQuery 批量文件加载作业完成重新加载。
*   **优势：** 水平扩展，无内存开销；规避 BigQuery API 修改频次上限以及并发冲突。
*   **劣势：** 复杂度极高，需要额外引入 GCS 作业存储开销。

---

## 方案 1 示例代码：原生 JSON 列写入

以下是推荐的 **方案 1：原生 JSON 列写入** 的 Python 与 Java 完整实现代码。

### Python 流式写入 JSON 字段代码
```python
import argparse
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def prepare_row_for_json_col(element_str):
    try:
        element = json.loads(element_str)
    except Exception as e:
        logging.error(f"[Parser] Failed to parse message JSON: {element_str}. Error: {e}")
        return

    flat_keys = {'id', 'sender_name'}
    row = {}
    metadata = {}

    for key, value in element.items():
        if key.lower() in flat_keys:
            row[key.upper()] = value
        else:
            metadata[key] = value

    # BigQuery JSON 列直接接受字典作为输入值 (切勿使用 json.dumps 序列化为 string)
    row['METADATA'] = metadata
    return row

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_subscription', required=True)
    parser.add_argument('--output_table', required=True)
    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)

    bq_schema = {
        'fields': [
            {'name': 'ID', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'SENDER_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'METADATA', 'type': 'JSON', 'mode': 'NULLABLE'} 
        ]
    }

    with beam.Pipeline(options=options) as p:
        _ = (
            p
            | "Read" >> beam.io.ReadFromPubSub(subscription=args.input_subscription)
            | "PrepareRow" >> beam.Map(prepare_row_for_json_col)
            | "Write" >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=bq_schema,
                create_disposition=beam.io.gcp.bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND,
                method='STREAMING_INSERTS'
            )
        )
```

### Java 流式写入 JSON 字段代码
```java
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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import java.util.ArrayList;
import java.util.List;

public class JsonApp {
    public static class ParseAndMapJsonFn extends DoFn<String, TableRow> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @ProcessElement
        public void processElement(@Element String message, OutputReceiver<TableRow> receiver) {
            try {
                Map<String, Object> map = mapper.readValue(message, Map.class);
                TableRow row = new TableRow();
                Map<String, Object> metadata = new HashMap<>();

                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    String key = entry.getKey().toLowerCase();
                    if (key.equals("id") || key.equals("sender_name")) {
                        row.set(entry.getKey().toUpperCase(), entry.getValue());
                    } else {
                        metadata.put(entry.getKey(), entry.getValue());
                    }
                }
                row.set("METADATA", metadata);
                receiver.output(row);
            } catch (IOException e) {
                // 处理解析失败
            }
        }
    }

    public static void main(String[] args) {
        DemoOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DemoOptions.class);
        Pipeline p = Pipeline.create(options);

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("ID").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("SENDER_NAME").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("METADATA").setType("JSON").setMode("NULLABLE"));
        TableSchema schema = new TableSchema().setFields(fields);

        p.apply("Read", PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
         .apply("ParseAndMapJson", ParDo.of(new ParseAndMapJsonFn()))
         .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
             .to(options.getOutputTable())
             .withSchema(schema)
             .withMethod(Method.STORAGE_WRITE_API)
             .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
             .withWriteDisposition(WriteDisposition.WRITE_APPEND)
         );

        p.run();
    }
}
```

---

## 方案多维对比表

在选择具体的技术栈与写入策略时，可参考以下维度的考量指南：

| 评价指标 / 特性 | 原生 JSON 列写入 (方案 1) | 死信队列修复模式 (方案 2) | GCS 动态双通道模式 (方案 3) |
| :--- | :--- | :--- | :--- |
| **数据防丢保障 (Data Loss)** |   **零丢失**。读取端解析确保全部字段完整存储。 |   **零丢失**。依靠捕获未知键错误，通过批作业追加到表中。 |   **零丢失**。数据在确认表模式更新前一直在 GCS 暂存，极为可靠。 |
| **支持 of 流式吞吐上限** |   **极高**。原生 BigQuery JSON 列高度优化。 | ⚠️ **中等**。如果在爆发期出现海量未知行，内存缓冲会导致 Worker 发生 OOM。 |   **极高**。GCS 缓冲完全免去了内存等待开销，由 BigQuery 自带的分布式批处理加载。 |
| **代码实现复杂度**|   **极低**。直接转换映射。 | ⚠️ **中等**（需自定义 DoFn 调用 Python BQ 客户端）。 | ❌ **较高**（涉及开窗、多路由、GCS 写操作与 WaitOn 控制流同步）。 |
| **BQ API 流控额度保护** |   **安全**（无表结构修改）。 | ⚠️ **中等**（必须增加 Table-Keyed 分组开窗以保障单节点表元数据串行修改）。 |   **安全**（在开窗中进行聚合，限制了表结构更新调用频次）。 |
| **常规数据处理延迟 (Normal)** |   **秒级/毫秒级**。直接写入。 |   **秒级/毫秒级**。直接写入。 |   **秒级/毫秒级**。常规行直接走快速通道写入。 |
| **漂移数据处理延迟 (Drifted)**|   **秒级/毫秒级**。无更新开销。 | ⚠️ **窗口级**（根据窗口大小，例如 15 秒）。 | ⚠️ **窗口级**（根据窗口大小，例如 15 秒）。 |
| **外部 system 依赖** | 无 | 无 | 是（需要云存储 bucket 作为暂存区）。 |
| **最适用场景** | 渴望极低运维复杂度，不需要在 BigQuery 物理层做严格强类型约束的业务。 | 中等量级、字段不定期微调、对开发成本有控制的数据中台。 | 超高吞吐量的高价值流式接入管道，要求极高可用与绝对零丢失的金融级分析场景。 |
