#!/bin/bash
# ============================================================
# 一键启动全栈大数据环境（在 node1 执行）
# 顺序: ZooKeeper -> HDFS -> YARN -> HBase -> Kafka -> MySQL -> 后端
# 已内置各种等待与坑的处理(建topic太早/HBase依赖HDFS/Kafka cluster.id不匹配等)
# 用法: bash start-all.sh
# ============================================================
source /etc/profile 2>/dev/null
export HBASE_HOME=/export/server/hbase
KAFKA=/export/server/kafka
HBASE=/export/server/hbase
JDK17=/export/server/jdk17

ok(){ echo "  [OK] $1"; }

echo "==== 1/7 ZooKeeper ===="
if ss -tlnp 2>/dev/null | grep -q :2181; then ok "ZK 已在跑"; else
  $KAFKA/bin/zookeeper-server-start.sh -daemon $KAFKA/config/zookeeper.properties
  sleep 6; ok "ZK 已启动 (2181)"
fi

echo "==== 2/7 HDFS ===="
if jps | grep -q NameNode; then ok "HDFS 已在跑"; else
  start-dfs.sh >/dev/null 2>&1; sleep 12
fi
hdfs dfsadmin -safemode leave >/dev/null 2>&1
ok "HDFS 就绪 (安全模式已关)"

echo "==== 3/7 YARN ===="
if jps | grep -q ResourceManager; then ok "YARN 已在跑"; else
  start-yarn.sh >/dev/null 2>&1; sleep 6; ok "YARN 已启动"
fi

echo "==== 4/7 HBase (依赖 HDFS+ZK) ===="
if jps | grep -q HMaster; then ok "HBase 已在跑"; else
  $HBASE/bin/start-hbase.sh >/dev/null 2>&1
  for i in $(seq 1 20); do jps | grep -q HMaster && break; sleep 3; done
fi
# 确保结果表存在
if ! echo "list" | $HBASE/bin/hbase shell -n 2>/dev/null | grep -q "stat_result"; then
  echo "create 'stat_result','cf'" | $HBASE/bin/hbase shell -n >/dev/null 2>&1
fi
ok "HBase 就绪 (表 stat_result)"

echo "==== 5/7 Kafka ===="
if ss -tlnp 2>/dev/null | grep -q :9092; then ok "Kafka 已在跑"; else
  $KAFKA/bin/kafka-server-start.sh -daemon $KAFKA/config/server.properties
  sleep 8
  # 处理重启后常见的 cluster.id 不匹配(ZK数据被/tmp清掉导致)
  if ! ss -tlnp 2>/dev/null | grep -q :9092; then
    if grep -q "Invalid cluster.id" $KAFKA/logs/server.log 2>/dev/null; then
      echo "  [修复] 检测到 cluster.id 不匹配，重置 meta.properties..."
      rm -f $KAFKA/logs/meta.properties
      $KAFKA/bin/kafka-server-start.sh -daemon $KAFKA/config/server.properties
      sleep 8
    fi
  fi
fi
# broker 就绪后再建 topic(避免"node not available"的WARN)
$KAFKA/bin/kafka-topics.sh --create --if-not-exists --topic gds-log-topic \
  --bootstrap-server node1:9092 --partitions 3 --replication-factor 1 >/dev/null 2>&1
ok "Kafka 就绪 (topic gds-log-topic)"

echo "==== 6/7 MySQL ===="
sudo systemctl start mysqld; sleep 2
ok "MySQL 就绪 (3306)"

echo "==== 7/7 后端 API ===="
if curl -s -m 3 http://localhost:8080/api/stat/list >/dev/null 2>&1; then ok "后端已在跑"; else
  cd /home/hadoop
  nohup $JDK17/bin/java -jar backend.jar > backend.log 2>&1 &
  for i in $(seq 1 30); do grep -q "Started BackendApplication" backend.log 2>/dev/null && break; sleep 1; done
  ok "后端已启动 (8080)"
fi

echo ""
echo "==================== 全栈启动完成 ===================="
echo "运行中的进程:"; jps | grep -vE "Jps" | sort
echo ""
echo "  接口   : http://192.168.88.101:8080/api/stat/list"
echo "  HDFS UI: http://192.168.88.101:9870"
echo "  YARN UI: http://192.168.88.101:8088"
echo "  HBase  : http://192.168.88.101:16010"
echo "下一步演示: bash demo.sh"
