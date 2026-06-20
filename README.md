# BigData Experiment 4 · 航空预订成功统计

《实验四 综合实验》—— **大数据采集分析与可视化系统**。基于中国航信 GDS 真实预订日志，统计**每个时间段、每个航空公司的预订成功数量**，并通过看板展示。

## 数据流 / 架构

```text
原始日志(256万条)
   → Kafka 采集 (gds-log-topic)
   → Spark Structured Streaming 处理 (解析 ITARES + 按"小时×航司"聚合)
   → 三种存储: MySQL + HBase + HDFS
   → Spring Boot 后端接口 (/api/stat/list)
   → ECharts 前端看板 (实时刷新)
```

## 技术栈

| 环节 | 技术 |
|------|------|
| 采集 | Java + kafka-clients 3.7，Kafka 3.9 |
| 处理 | Java + Spark 3.5.1 **Structured Streaming**（流式消费 Kafka，可额外加分） |
| 存储 | **MySQL 8**（供查询）+ **HBase 2.4**（列式，加分）+ **HDFS**（CSV，满足存储要求） |
| 后端 | Spring Boot 3.2 / JDK17 + Spring Data JPA |
| 前端 | 单文件 HTML + ECharts 5（汇总卡片 / 柱状 / 环形 / 堆叠 / 明细表 / 深色模式） |
| 集群 | 3 节点 Hadoop/HBase（node1 主，node2/3 从），YARN 分布式 |

## 小组分工

| 成员 | 负责模块 | 目录 |
| ---- | ------------------------ | ------------------------------------ |
| A | Kafka 数据采集 | `producer/` |
| B | Spark 数据处理 | `processor/` |
| C | 数据存储 + 后端接口 | `sql/`、`backend/` |
| D | 前端展示 + 报告整合 | `frontend/`、`docs/`、`screenshots/` |

## 目录结构

```text
bigdata-exp4/
├── producer/      # Kafka 生产者(Java)，发送日志到 gds-log-topic
├── processor/     # Spark 处理(Java)，聚合后写 MySQL/HBase/HDFS
├── backend/       # Spring Boot 后端，GET /api/stat/list
├── frontend/      # ECharts 单页看板(index.html)
├── sql/           # MySQL 建表(init.sql)
├── scripts/       # 一键启动 + 演示脚本
├── data/          # 实验数据(不入库，见 .gitignore)
├── docs/          # 实验报告
└── screenshots/   # 各模块运行截图
```

---

## 统一约定（接口契约）

| 项 | 值 |
|----|-----|
| Kafka topic | `gds-log-topic` |
| 集群入口 | node1 = `192.168.88.101`（node2/3 = .102/.103） |
| MySQL | `192.168.88.101:3306`，库 `bigdata_exp4`，表 `stat_result`，root/root |
| HBase 表 | `stat_result`（列族 `cf`，rowkey = `stat_hour#airline_code`） |
| HDFS 输出 | `hdfs:///user/hadoop/stat_result_out`（CSV） |
| 统计字段 | `stat_hour`(时间段) / `airline_code`(航司) / `success_count`(成功数) |
| 后端接口 | `GET /api/stat/list` → `[{statHour, airlineCode, successCount}, ...]` |

---

## 快速运行（在 node1 执行）

> 各模块详细说明见 `producer/RUN_ON_VM.md`、`processor/RUN_ON_VM.md`、`backend/DEPLOY_ON_VM.md`、`frontend/README.md`。

### 1. 一键启动全栈

```bash
bash scripts/start-all.sh
```
按 ZK → HDFS → YARN → HBase → Kafka → MySQL → 后端 顺序启动（幂等，已跑的跳过；内置等待与常见坑处理）。

启动后可访问：
- 接口：http://192.168.88.101:8080/api/stat/list
- HDFS UI：http://192.168.88.101:9870 ｜ YARN UI：:8088 ｜ HBase UI：:16010

### 2. 动态演示（从零看数据实时增长）

```bash
bash scripts/demo.sh /home/hadoop/sample-300k.txt 20000 3
```
清空(MySQL/HBase/topic/checkpoint) → 起 Spark 流式 → 发数据。浏览器打开 `frontend/index.html` 勾「自动刷新」，可见柱状图/数字随消费**渐进增长**。

### 3. 三存储批处理（吃加分：一次写 MySQL + HBase + HDFS）

```bash
spark-submit --class com.bigdata.processor.AirlineSuccessProcessor \
  /home/hadoop/processor-fx.jar --mode local --input file:///home/hadoop/sample-300k.txt
# 验证三份数据：
mysql -uroot -proot -e "SELECT COUNT(*) FROM bigdata_exp4.stat_result;"
echo "scan 'stat_result',{LIMIT=>5}" | /export/server/hbase/bin/hbase shell -n
hdfs dfs -cat /user/hadoop/stat_result_out/part-*.csv | head
```

### 4. 前端

直接双击 `frontend/index.html`（后端 CORS 已放行，`file://` 即可取数）。顶部 ⚙ 可改接口地址。

---

## 各模块要点（答辩准备）

- **producer**：KafkaProducer 逐行读日志发到 `gds-log-topic`，`acks=all` 可靠发送，打成 fat jar 用 `java -jar` 运行。
- **processor**：Spark Structured Streaming 消费 Kafka；`LogParser` 只取 `ITARES` 日志、提取行尾 `航司:success`，按"小时×航司" `groupBy().count()`；结果写 MySQL/HBase/HDFS。提供 `--mode local`(批处理) 与 `--mode kafka`(流式) 两种。
- **backend**：Spring Data JPA 查 `stat_result`，`GET /api/stat/list` 返回 JSON；`ddl-auto=validate` 校验表结构；CORS 放行供前端跨域。
- **frontend**：fetch 接口，ECharts 渲染卡片/柱状/环形/堆叠/表格，支持 Top N、搜索、导出 CSV、自动刷新、深色模式。

## 分布式体现

3 节点集群：node1 主（NameNode/ResourceManager/HMaster/Kafka/MySQL/后端），node2/3 从（DataNode/NodeManager/RegionServer）。Spark 可用 `--master yarn` 分发到三节点执行（YARN UI 可见）；HDFS 数据块、HBase Region 分布在 node2/3。

## 加分项落实

- ✅ **流计算消费 Kafka**：Spark Structured Streaming
- ✅ **HBase 列式存储**：结果写入 HBase 表 `stat_result`
- ✅ **结果存 HDFS**：CSV 输出到 HDFS

---

## Git 分支

```text
main                      最终可演示版本(已集成全链路)
fallback                  完整集成基线/兜底
feature/spark-process     B 的处理实现
feature/frontend          D 的 Vue 前端实现
feature/kafka-producer    A 的采集实现
```
> 历史协作规范（dev / feature 流程）见 git 提交记录。

## 数据集

原始数据 `实验四-kafka采集数据集.txt`（256 万行、24 小时）体积大，**不入库**（见 `.gitignore`）。演示用抽样子集 `sample-300k.txt`（32 万行、覆盖 24 小时）。
