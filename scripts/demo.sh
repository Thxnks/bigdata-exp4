#!/bin/bash
# ============================================================
# 从零动态演示（在 node1 执行，前置: 已 bash start-all.sh）
# 清空 -> 起 Spark 流式(节流) -> 发数据，前端勾自动刷新可见数字渐进增长
# 用法: bash demo.sh [数据文件] [每批条数] [批次间隔秒]
#   例: bash demo.sh /home/hadoop/sample-300k.txt 20000 3
# ============================================================
source /etc/profile 2>/dev/null
KAFKA=/export/server/kafka
HBASE=/export/server/hbase
SPARK=/export/server/spark
JAR=/home/hadoop/processor-fx.jar
DATA=${1:-/home/hadoop/sample-300k.txt}
MAXPER=${2:-20000}
INTERVAL=${3:-3}
JDBC="jdbc:mysql://192.168.88.101:3306/bigdata_exp4?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"

echo "==== 1) 清空 MySQL / HBase / topic / checkpoint (从零开始) ===="
mysql -uroot -proot -e "TRUNCATE TABLE bigdata_exp4.stat_result;" 2>/dev/null && echo "  MySQL 已清空"
echo "truncate 'stat_result'" | $HBASE/bin/hbase shell -n >/dev/null 2>&1 && echo "  HBase 已清空"
$KAFKA/bin/kafka-topics.sh --delete --topic gds-log-topic --bootstrap-server node1:9092 >/dev/null 2>&1
sleep 4
$KAFKA/bin/kafka-topics.sh --create --topic gds-log-topic --bootstrap-server node1:9092 \
  --partitions 3 --replication-factor 1 >/dev/null 2>&1 && echo "  topic 已重建"
rm -rf /home/hadoop/spark-ckpt-gds && echo "  checkpoint 已清"

echo ""
echo "==== 2) 启动 Spark 流式作业(每批 ${MAXPER} 条, ${INTERVAL}s 一批) ===="
cd /home/hadoop
nohup $SPARK/bin/spark-submit --class com.bigdata.processor.KafkaSparkProcessor --master 'local[2]' \
  $JAR node1:9092 "$JDBC" "file:///home/hadoop/spark-ckpt-gds" $MAXPER $INTERVAL > spark.log 2>&1 &
echo "  Spark 启动中(挂起等数据)。现在请打开前端、勾上「自动刷新」"
echo "  等待流就绪..."; sleep 14

echo ""
echo "==== 3) 发送数据: $DATA ===="
java -jar /home/hadoop/producer.jar "$DATA" gds-log-topic node1:9092 UTF-8

echo ""
echo "==================== 发送完成 ===================="
echo "Spark 会一批批消费，前端数字/图表渐进增长。"
echo "看实时进度: watch -n2 \"mysql -uroot -proot -N -e 'SELECT COUNT(*) 行,SUM(success_count) 总成功 FROM bigdata_exp4.stat_result;'\""
echo "停止 Spark : pkill -f '[K]afkaSparkProcessor'"
echo ""
echo "# ---- 另一个演示: 批处理一次性把结果写入 MySQL+HBase+HDFS 三存储(吃加分) ----"
echo "# spark-submit --class com.bigdata.processor.AirlineSuccessProcessor $JAR --mode local --input file://$DATA"
echo "#   HBase 看: echo \"scan 'stat_result',{LIMIT=>5}\" | $HBASE/bin/hbase shell -n"
echo "#   HDFS  看: hdfs dfs -cat /user/hadoop/stat_result_out/part-*.csv | head"
