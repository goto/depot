package com.gotocompany.depot.kafka;

import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Wraps Kafka producer to produce serialized protobuf messages to the output Kafka topic.
 * Handles topic auto-creation, large message support, and passthrough Kafka producer configs.
 */
public class KafkaSinkProducer implements Closeable {

    private static final String SINK_KAFKA_PREFIX = "SINK_KAFKA_";
    private static final int LARGE_MESSAGE_MAX_REQUEST_SIZE = 20971520;
    private static final String LARGE_MESSAGE_COMPRESSION_TYPE = "snappy";

    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;
    private final Instrumentation instrumentation;

    public KafkaSinkProducer(KafkaSinkConfig config, Map<String, String> envVars, Instrumentation instrumentation) {
        this.topic = config.getSinkKafkaTopic();
        this.instrumentation = instrumentation;

        Properties producerProps = buildProducerProperties(config, envVars);
        this.producer = new KafkaProducer<>(producerProps);

        ensureTopicExists(config, producerProps);
        instrumentation.logInfo("Kafka sink producer initialized for topic: {}", topic);
    }

    // Constructor for testing with injected producer
    KafkaSinkProducer(KafkaProducer<byte[], byte[]> producer, String topic, Instrumentation instrumentation) {
        this.producer = producer;
        this.topic = topic;
        this.instrumentation = instrumentation;
    }

    /**
     * Produces a key-value pair to the configured output topic.
     * Returns the RecordMetadata future for error handling.
     */
    public Future<RecordMetadata> produce(byte[] key, byte[] value) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, value);
        return producer.send(record);
    }

    /**
     * Flushes any pending records.
     */
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

    private Properties buildProducerProperties(KafkaSinkConfig config, Map<String, String> envVars) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getSinkKafkaBrokers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // Large message support
        if (config.isSinkKafkaProduceLargeMessageEnabled()) {
            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, LARGE_MESSAGE_MAX_REQUEST_SIZE);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, LARGE_MESSAGE_COMPRESSION_TYPE);
            instrumentation.logInfo("Large message support enabled: max.request.size={}, compression.type={}",
                    LARGE_MESSAGE_MAX_REQUEST_SIZE, LARGE_MESSAGE_COMPRESSION_TYPE);
        }

        // Passthrough: SINK_KAFKA_* env vars → Kafka producer properties
        // e.g., SINK_KAFKA_BATCH_SIZE → batch.size
        if (envVars != null) {
            for (Map.Entry<String, String> entry : envVars.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(SINK_KAFKA_PREFIX) && !isReservedConfig(key)) {
                    String kafkaProp = key.substring(SINK_KAFKA_PREFIX.length())
                            .toLowerCase()
                            .replace('_', '.');
                    props.put(kafkaProp, entry.getValue());
                }
            }
        }

        return props;
    }

    private boolean isReservedConfig(String key) {
        return key.equals("SINK_KAFKA_BROKERS")
                || key.equals("SINK_KAFKA_TOPIC")
                || key.equals("SINK_KAFKA_PROTO_MESSAGE")
                || key.equals("SINK_KAFKA_PROTO_KEY")
                || key.equals("SINK_KAFKA_PROTO_MAPPING")
                || key.equals("SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE")
                || key.equals("SINK_KAFKA_STREAM")
                || key.startsWith("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL");
    }

    private void ensureTopicExists(KafkaSinkConfig config, Properties producerProps) {
        Properties adminProps = new Properties();
        adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getSinkKafkaBrokers());
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            if (!adminClient.listTopics().names().get().contains(topic)) {
                NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                instrumentation.logInfo("Auto-created output topic: {}", topic);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            instrumentation.logInfo("Interrupted while checking/creating topic: {}", topic);
        } catch (ExecutionException e) {
            instrumentation.logInfo("Could not auto-create topic '{}': {}. Topic may already exist or broker may handle auto-creation.",
                    topic, e.getMessage());
        }
    }
}
