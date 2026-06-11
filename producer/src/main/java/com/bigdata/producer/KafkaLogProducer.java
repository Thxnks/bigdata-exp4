package com.bigdata.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaLogProducer {
    private static final String DEFAULT_TOPIC = "gds-log-topic";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String README_DATA_FILE = "data/kafka采集数据实验.txt";
    private static final String CURRENT_ROOT_DATA_FILE = "实验四-kafka采集数据集.txt";
    private static final int PRINT_INTERVAL = 1000;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        ProducerArguments arguments = ProducerArguments.parse(args);
        Path dataFile = resolveDataFile(arguments.dataFile);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, arguments.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        long count = 0;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = Files.newBufferedReader(dataFile, arguments.charset)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                ProducerRecord<String, String> record = new ProducerRecord<>(arguments.topic, line);
                RecordMetadata metadata = producer.send(record).get();
                count++;

                if (count == 1 || count % PRINT_INTERVAL == 0) {
                    System.out.printf(
                            "Sent %,d messages to %s-%d, offset=%d%n",
                            count,
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset()
                    );
                }
            }

            producer.flush();
        }

        System.out.printf("Producer finished, total sent: %,d%n", count);
    }

    private static Path resolveDataFile(String inputFile) throws IOException {
        Path dataFile = Paths.get(inputFile);
        if (Files.isRegularFile(dataFile)) {
            return dataFile;
        }

        Path rootDataFile = Paths.get(CURRENT_ROOT_DATA_FILE);
        if (README_DATA_FILE.equals(inputFile) && Files.isRegularFile(rootDataFile)) {
            return rootDataFile;
        }

        throw new IOException("Data file not found: " + dataFile.toAbsolutePath());
    }

    private static class ProducerArguments {
        private final String dataFile;
        private final String topic;
        private final String bootstrapServers;
        private final Charset charset;

        private ProducerArguments(String dataFile, String topic, String bootstrapServers, Charset charset) {
            this.dataFile = dataFile;
            this.topic = topic;
            this.bootstrapServers = bootstrapServers;
            this.charset = charset;
        }

        private static ProducerArguments parse(String[] args) {
            String dataFile = args.length > 0 ? args[0] : README_DATA_FILE;
            String topic = args.length > 1 ? args[1] : DEFAULT_TOPIC;
            String bootstrapServers = args.length > 2 ? args[2] : DEFAULT_BOOTSTRAP_SERVERS;
            Charset charset = args.length > 3 ? Charset.forName(args[3]) : StandardCharsets.UTF_8;
            return new ProducerArguments(dataFile, topic, bootstrapServers, charset);
        }
    }
}
