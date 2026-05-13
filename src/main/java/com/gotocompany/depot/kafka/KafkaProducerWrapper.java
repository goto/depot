package com.gotocompany.depot.kafka;

import com.gotocompany.depot.config.KafkaSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

@Slf4j
public class KafkaProducerWrapper implements Closeable {

    private static final int LARGE_MESSAGE_MAX_REQUEST_SIZE = 20971520;
    private static final String LARGE_MESSAGE_COMPRESSION_TYPE = "snappy";

    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;

    public KafkaProducerWrapper(KafkaSinkConfig config, Map<String, String> envVars) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getSinkKafkaBrokers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        if (config.isSinkKafkaProduceLargeMessageEnabled()) {
            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, LARGE_MESSAGE_MAX_REQUEST_SIZE);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, LARGE_MESSAGE_COMPRESSION_TYPE);
        }

        applySinkKafkaOverrides(props, envVars);

        this.producer = new KafkaProducer<>(props);
        this.topic = config.getSinkKafkaTopic();
        log.info("Kafka producer initialized for topic: {}", topic);
    }

    KafkaProducerWrapper(KafkaProducer<byte[], byte[]> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public Future<RecordMetadata> send(byte[] key, byte[] value) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, value);
        return producer.send(record);
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

    private void applySinkKafkaOverrides(Properties props, Map<String, String> envVars) {
        if (envVars == null) {
            return;
        }
        String prefix = "SINK_KAFKA_";
        for (Map.Entry<String, String> entry : envVars.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix) && !isReservedKey(key)) {
                String kafkaProperty = key.substring(prefix.length())
                        .toLowerCase()
                        .replace('_', '.');
                props.put(kafkaProperty, entry.getValue());
            }
        }
    }

    private boolean isReservedKey(String key) {
        return key.equals("SINK_KAFKA_BROKERS")
                || key.equals("SINK_KAFKA_TOPIC")
                || key.equals("SINK_KAFKA_PROTO_MESSAGE")
                || key.equals("SINK_KAFKA_PROTO_KEY")
                || key.equals("SINK_KAFKA_PROTO_MAPPING")
                || key.equals("SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE")
                || key.equals("SINK_KAFKA_STREAM")
                || key.startsWith("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_");
    }
}
