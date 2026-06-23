# 角色 B：Spark 处理程序运行手册（node1）

> 作用：消费 Kafka `gds-log-topic` → 统计每小时每航司预订成功数 → 写入 MySQL `stat_result`。
> 用 **Structured Streaming**

## 前置条件

1. Kafka 已在 node1 运行，topic `gds-log-topic` 里有数据（角色 A 的 producer 发过）
2. MySQL 已就绪（角色 C 已部署，库 `bigdata_exp4` 表 `stat_result`）
3. `processor.jar`（fat jar，自带 kafka 连接器 + mysql 驱动）已传到 `/home/hadoop/`

## 打包（Windows）

```bat
cd C:\Users\24269\Desktop\bigdata-exp4\processor
mvn clean package
```
产物：`target\processor.jar`，scp 到 node1:/home/hadoop/

---

## 方式一：本机快速跑

```bash
/export/server/spark/bin/spark-submit \
  --class com.bigdata.processor.KafkaSparkProcessor \
  --master 'local[2]' \
  /home/hadoop/processor.jar
```

这是**流式作业，会一直运行**。控制台每个微批打印 `写入 N 条 (小时,航司) 统计`。
另开终端验证 MySQL：
```bash
mysql -uroot -proot -e "SELECT * FROM bigdata_exp4.stat_result ORDER BY success_count DESC LIMIT 10;"
```
看到真实统计数据 = B 模块跑通。停止：`Ctrl+C`。

---

## 方式二：YARN 分布式跑

先启动 HDFS + YARN，checkpoint 放 HDFS：
```bash
start-dfs.sh
start-yarn.sh
hdfs dfs -mkdir -p /user/hadoop

/export/server/spark/bin/spark-submit \
  --class com.bigdata.processor.KafkaSparkProcessor \
  --master yarn --deploy-mode client \
  /home/hadoop/processor.jar \
  node1:9092 \
  "jdbc:mysql://node1:3306/bigdata_exp4?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai" \
  hdfs:///user/hadoop/spark-ckpt-gds
```
任务会跑在 node1/2/3 三个节点上（YARN 调度），这就是"Spark 分布式处理"的演示。
可在 `http://node1:8088` 看 YARN 上的 application。

---

## 整条流水线联调顺序（A→B→C）

```bash
# node1 上
1. 启动 Kafka： cd /export/server/kafka
   bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
   bin/kafka-server-start.sh -daemon config/server.properties
2. 建 topic： bin/kafka-topics.sh --create --topic gds-log-topic --bootstrap-server node1:9092 --partitions 3 --replication-factor 1
3. A 跑 producer 灌数据（见 producer 模块）
4. B 跑 processor（上面方式一/二）→ 写入 MySQL
5. 刷新 http://node1:8080/api/stat/list → 看到真实统计
6. D 的前端展示
```

---

## 重新跑 / 重置

流式作业用 checkpoint 记录进度。想从头重算：
```bash
rm -rf /home/hadoop/spark-ckpt-gds            # local 模式
hdfs dfs -rm -r /user/hadoop/spark-ckpt-gds   # yarn 模式
```

- **为什么用流计算？** Structured Streaming 持续消费 Kafka，producer 边发、统计边更新、MySQL 实时刷新，符合"实时采集分析"场景，且文档说额外加分。
- **怎么解析的？** 按逗号切分，只取 `ITARES` 日志，用 `regexp_extract_all` 提取行尾所有 `航司:success`，按"小时+航司" `groupBy().count()` 聚合。
- **怎么写 MySQL 不破坏表？** JDBC 写用 `truncate=true` 的 overwrite——只清空数据不删表，保住 `id`/`created_at` 列，后端 JPA 校验仍通过。
- **分布式体现在哪？** `--master yarn` 时 Spark 作业在三节点上分布式执行。
