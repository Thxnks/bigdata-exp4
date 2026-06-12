# Kafka Producer

成员 A 负责本模块。代码会读取实验日志文件，将每一行日志作为一条 Kafka 消息发送到 README 约定的 topic：

```text
gds-log-topic
```

## 运行前准备

先启动虚拟机里的 Kafka，并创建 topic：

```bat
kafka-topics.bat --create --topic gds-log-topic --bootstrap-server 虚拟机IP:9092 --partitions 1 --replication-factor 1
```

如果 Kafka 在 Linux 虚拟机里，进入 Kafka 目录后也可以用：

```bash
bin/kafka-topics.sh --create --topic gds-log-topic --bootstrap-server 虚拟机IP:9092 --partitions 1 --replication-factor 1
```

## 运行 Producer

在 IDEA 终端或 Windows PowerShell 中进入本目录：

```bat
cd C:\Users\24269\Desktop\bigdata-exp4\producer
```

如果 Kafka 能通过 `localhost:9092` 访问，直接运行：

```bat
mvn exec:java
```

如果 Kafka 在虚拟机里，把第三个参数改成虚拟机 IP：

```bat
mvn exec:java -Dexec.args="../实验四-kafka采集数据集.txt gds-log-topic 虚拟机IP:9092 UTF-8"
```

如果数据文件编码不是 UTF-8，可以把最后一个参数改成 `GBK`：

```bat
mvn exec:java -Dexec.args="../实验四-kafka采集数据集.txt gds-log-topic 虚拟机IP:9092 GBK"
```

参数顺序：

```text
数据文件路径 topic bootstrapServers 文件编码
```

## 消费者验证

另开一个终端查看消息：

```bat
kafka-console-consumer.bat --bootstrap-server 虚拟机IP:9092 --topic gds-log-topic --from-beginning
```

看到日志一行一行输出，就说明 A 模块完成。

## 截图

按照 README 要求，把截图放到：

```text
screenshots/producer/
```

需要截图：

```text
1. topic 创建成功
2. Producer 运行成功
3. Consumer 接收到消息
```
