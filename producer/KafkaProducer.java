import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;


public class KafkaProducer {
    // 统一配置项
    private static final String KAFKA_BROKERS = "cluster1:9092,cluster2:9092,cluster3:9092";
    private static final String TOPIC_NAME = "gds-log-topic-new";
    private static final String DATA_FILE_PATH = "/home/hadoop/bigdata-exp4/data/kafka采集数据实验.txt";
    private static final long SEND_INTERVAL_MS = 0;

    public static void main(String[] args) {
        Producer<String, String> producer = null;
        BufferedReader br = null;

        try {
            producer = createProducer();
            System.out.println("Kafka生产者启动成功，集群地址：" + KAFKA_BROKERS);
            System.out.println("目标Topic：" + TOPIC_NAME);
            System.out.println("开始批量发送256万条日志...");

            br = new BufferedReader(new FileReader(DATA_FILE_PATH));
            String line;
            int totalRead = 0;
            int totalSent = 0;

            while ((line = br.readLine()) != null) {
                totalRead++;
                String logData = line.trim();
                if (logData.isEmpty()) {
                    continue;
                }

                KeyedMessage<String, String> message = new KeyedMessage<>(TOPIC_NAME, logData);
                producer.send(message);
                totalSent++;

                if (totalSent % 10000 == 0) {
                    System.out.printf("已发送：%d 条，总读取：%d 条\n", totalSent, totalRead);
                }

                // 发送间隔0，全速发送
                Thread.sleep(SEND_INTERVAL_MS);
            }

            // 强制刷新所有未发送的消息
            producer.close();
            System.out.printf("全部发送完成！共读取日志 %d 条，成功发送 %d 条\n", totalRead, totalSent);

        } catch (Exception e) {
            System.err.println("error 生产者运行异常");
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (producer != null) {
                producer.close();
                System.out.println("Kafka生产者已关闭");
            }
        }
    }

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", KAFKA_BROKERS);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        props.put("request.required.acks", "1");

        props.put("batch.size", "16384");  // 每攒16KB消息批量发送
        props.put("linger.ms", "100");     // 最多等100ms就发一批

        return new Producer<>(new ProducerConfig(props));
    }
}