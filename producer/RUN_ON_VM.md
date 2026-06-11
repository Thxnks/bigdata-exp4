# 角色 A：Kafka Producer 运行手册（node1）

> 作用：读取日志数据文件，**每行作为一条消息**发送到 Kafka topic `gds-log-topic`。

## 打包（Windows）

```bat
cd C:\Users\24269\Desktop\bigdata-exp4\producer
mvn clean package
```
产物：`target\producer.jar`（fat jar，自带 kafka-clients），scp 到 node1:/home/hadoop/

## 运行（node1）

参数顺序：`数据文件 topic bootstrapServers 编码`
```bash
cd /home/hadoop
java -jar producer.jar 数据文件名 gds-log-topic node1:9092 UTF-8
```
看到 `Producer finished, total sent: N` 即发送完成。

- 小样本测试：`java -jar producer.jar sample-5000.txt gds-log-topic node1:9092 UTF-8`
- 正式全量：把 `实验四-kafka采集数据集.txt`(202MB) 传上来，文件名换成它
- 编码不对（乱码）就把最后一个参数换 `GBK`

## 验证发送成功

```bash
/export/server/kafka/bin/kafka-get-offsets.sh --bootstrap-server node1:9092 --topic gds-log-topic
```
三个分区数字相加 = 发送行数，即成功。也可用 console-consumer 看消息：
```bash
/export/server/kafka/bin/kafka-console-consumer.sh --bootstrap-server node1:9092 --topic gds-log-topic --from-beginning --max-messages 5
```

## 在联调里的位置

```
producer(本模块) → Kafka gds-log-topic → Spark 处理 → MySQL → 后端 → 前端
```
> 注意：producer 只负责**原样发送整行日志**，解析/统计是 processor（角色 B）的事。

## 答辩要点

- **怎么发的？** KafkaProducer，逐行读文件，每行一条 `ProducerRecord` 发到 `gds-log-topic`；`acks=all` 保证可靠，`send().get()` 同步确认。
- **topic 为什么是 gds-log-topic？** 全组 README 统一约定，下游 Spark 按这个名字订阅。
- **3 个分区？** 建 topic 时 `--partitions 3`，producer 默认按轮询/哈希分散到各分区，提升并行消费能力。
