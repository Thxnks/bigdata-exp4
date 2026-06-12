#!/bin/bash
# 大数据实验四：Kafka Topic创建脚本
# 严格遵循README约定：Topic名称 = gds-log-topic，副本数3，分区数1（适配3节点集群）

# 你的集群Kafka安装路径（和实验文档一致）
KAFKA_BIN_DIR="/usr/local/kafka_2.10-0.8.2.1/bin"
ZK_QUORUM="cluster1:2181,cluster2:2181,cluster3:2181"
TOPIC_NAME="gds-log-topic"

echo "开始创建Kafka Topic：$TOPIC_NAME"
# 创建Topic（3副本，1分区，匹配3节点集群）
$KAFKA_BIN_DIR/kafka-topics.sh \
    --create \
    --zookeeper $ZK_QUORUM \
    --topic $TOPIC_NAME \
    --replication-factor 3 \
    --partitions 1

echo "Topic创建完成，查看所有Topic："
# 验证Topic是否创建成功
$KAFKA_BIN_DIR/kafka-topics.sh \
    --list \
    --zookeeper $ZK_QUORUM