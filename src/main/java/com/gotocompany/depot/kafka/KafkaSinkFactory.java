package com.gotocompany.depot.kafka;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.config.enums.SinkConnectorSchemaDataType;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.kafka.client.KafkaProducerClient;
import com.gotocompany.depot.kafka.client.KafkaProducerFactory;
import com.gotocompany.depot.kafka.client.KafkaProducerPropertiesFactory;
import com.gotocompany.depot.kafka.client.KafkaTopicCreator;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunctionCache;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunctionFactory;
import com.gotocompany.depot.kafka.parser.KafkaRecordParser;
import com.gotocompany.depot.kafka.schema.KafkaSinkStencilClientFactory;
import com.gotocompany.depot.kafka.schema.KafkaSinkStencilUpdateListener;
import com.gotocompany.depot.kafka.serializer.ProtoKafkaMessageSerializer;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.KafkaSinkMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.utils.MessageConfigUtils;
import com.gotocompany.stencil.client.StencilClient;
import com.timgroup.statsd.NoOpStatsDClient;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.producer.Producer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Builds and initializes the Kafka sink together with all of its collaborators.
 *
 * <p>{@link #init()} validates the configuration, sets up the sink Stencil client, the source message
 * parser, the proto mapping function and the producer properties. {@link #create()} then produces a new
 * {@link KafkaSink} backed by a fresh Kafka producer.
 */
public class KafkaSinkFactory {

    private final KafkaSinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;
    private final Map<String, String> configMap;
    private final KafkaTopicCreator topicCreator;
    private KafkaSinkMetrics kafkaSinkMetrics;
    private KafkaRecordParser recordParser;
    private Properties producerProperties;

    /**
     * Creates a factory from a raw configuration map.
     *
     * @param env            the environment configuration used to build the sink config and producer pass-through properties
     * @param statsDReporter the reporter used for metrics and the Stencil client
     */
    public KafkaSinkFactory(Map<String, String> env, StatsDReporter statsDReporter) {
        this(env, statsDReporter, new KafkaTopicCreator());
    }

    /**
     * Creates a factory from a raw configuration map with a custom topic creator.
     *
     * @param env            the environment configuration used to build the sink config and producer pass-through properties
     * @param statsDReporter the reporter used for metrics and the Stencil client
     * @param topicCreator   the topic creator used to ensure the output topic exists
     */
    KafkaSinkFactory(Map<String, String> env, StatsDReporter statsDReporter, KafkaTopicCreator topicCreator) {
        this.sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, env);
        this.statsDReporter = statsDReporter;
        this.configMap = env;
        this.topicCreator = topicCreator;
    }

    /**
     * Creates a factory from an already parsed sink configuration.
     *
     * @param sinkConfig     the parsed Kafka sink configuration
     * @param statsDReporter the reporter used for metrics and the Stencil client
     */
    public KafkaSinkFactory(KafkaSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this(sinkConfig, statsDReporter, new KafkaTopicCreator());
    }

    /**
     * Creates a factory from an already parsed sink configuration with a custom topic creator.
     *
     * @param sinkConfig     the parsed Kafka sink configuration
     * @param statsDReporter the reporter used for metrics and the Stencil client
     * @param topicCreator   the topic creator used to ensure the output topic exists
     */
    KafkaSinkFactory(KafkaSinkConfig sinkConfig, StatsDReporter statsDReporter, KafkaTopicCreator topicCreator) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
        this.configMap = Collections.emptyMap();
        this.topicCreator = topicCreator;
    }

    /**
     * Creates a factory from a parsed sink configuration using a no-op StatsD reporter.
     *
     * @param sinkConfig the parsed Kafka sink configuration
     */
    public KafkaSinkFactory(KafkaSinkConfig sinkConfig) {
        this(sinkConfig, new StatsDReporter(new NoOpStatsDClient()));
    }

    /**
     * Validates the configuration and builds the schema, mapping function, record parser and producer properties.
     *
     * <p>This must be called once before {@link #create()}. Any failure, including invalid configuration or
     * schema resolution errors, is wrapped and rethrown so that the sink fails fast at startup.
     *
     * @throws IllegalArgumentException if the sink cannot be initialized
     */
    public void init() {
        try {
            validateConfig();
            Instrumentation instrumentation = new Instrumentation(statsDReporter, KafkaSinkFactory.class);
            instrumentation.logInfo(String.format("\n\tkafka.brokers = %s\n\tkafka.topic = %s\n\tkafka.proto.message = %s"
                            + "\n\tkafka.proto.key = %s\n\tkafka.proto.mapping = %s\n\tkafka.produce.large.message.enable = %s\n\t",
                    sinkConfig.getSinkKafkaBrokers(),
                    sinkConfig.getSinkKafkaTopic(),
                    sinkConfig.getSinkKafkaProtoMessage(),
                    sinkConfig.getSinkKafkaProtoKey(),
                    sinkConfig.getSinkKafkaProtoMapping(),
                    sinkConfig.isSinkKafkaProduceLargeMessageEnable()));
            this.kafkaSinkMetrics = new KafkaSinkMetrics(sinkConfig);
            ProtoMappingFunctionCache mappingFunctionCache = new ProtoMappingFunctionCache();
            KafkaSinkStencilUpdateListener updateListener = new KafkaSinkStencilUpdateListener(
                    sinkConfig, new ProtoMappingFunctionFactory(), mappingFunctionCache, kafkaSinkMetrics,
                    new Instrumentation(statsDReporter, KafkaSinkStencilUpdateListener.class));
            StencilClient sinkStencilClient = KafkaSinkStencilClientFactory.create(sinkConfig, statsDReporter.getClient(), updateListener);
            updateListener.setSinkStencilClient(sinkStencilClient);
            MessageParser messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter, updateListener);
            updateListener.setMessageParser(messageParser);
            instrumentation.logInfo("Building kafka sink proto mapping function from the configured schemas");
            updateListener.updateSchema();
            this.recordParser = new KafkaRecordParser(messageParser, mappingFunctionCache,
                    new ProtoKafkaMessageSerializer(), MessageConfigUtils.getModeAndSchema(sinkConfig));
            this.producerProperties = KafkaProducerPropertiesFactory.create(sinkConfig, configMap);
            topicCreator.ensureTopicExists(sinkConfig, producerProperties,
                    new Instrumentation(statsDReporter, KafkaTopicCreator.class));
            instrumentation.captureValue(kafkaSinkMetrics.getKafkaLargeMessageModeMetric(),
                    sinkConfig.isSinkKafkaProduceLargeMessageEnable() ? 1 : 0);
            instrumentation.logInfo("Kafka sink initialized successfully");
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception occurred while creating Kafka sink", e);
        }
    }

    /**
     * Creates a new Kafka sink instance backed by a fresh Kafka producer.
     *
     * @return the initialized Kafka sink
     */
    public Sink create() {
        Instrumentation instrumentation = new Instrumentation(statsDReporter, KafkaSinkFactory.class);
        instrumentation.logInfo("Creating kafka producer for the topic {}", sinkConfig.getSinkKafkaTopic());
        Producer<byte[], byte[]> producer = KafkaProducerFactory.create(producerProperties);
        KafkaProducerClient kafkaProducerClient = new KafkaProducerClient(
                producer, sinkConfig.getSinkKafkaTopic(), new Instrumentation(statsDReporter, KafkaProducerClient.class));
        return new KafkaSink(
                kafkaProducerClient,
                recordParser,
                kafkaSinkMetrics,
                new Instrumentation(statsDReporter, KafkaSink.class));
    }

    /**
     * Validates the required Kafka sink configuration values.
     *
     * @throws ConfigurationException if the schema data type is unsupported or a required value is missing
     */
    private void validateConfig() {
        if (sinkConfig.getSinkConnectorSchemaDataType() != SinkConnectorSchemaDataType.PROTOBUF) {
            throw new ConfigurationException("kafka sink only supports PROTOBUF schema data type");
        }
        validateNotEmpty(sinkConfig.getSinkKafkaBrokers(), "SINK_KAFKA_BROKERS");
        validateNotEmpty(sinkConfig.getSinkKafkaTopic(), "SINK_KAFKA_TOPIC");
        validateNotEmpty(sinkConfig.getSinkKafkaProtoMessage(), "SINK_KAFKA_PROTO_MESSAGE");
        if (sinkConfig.getSinkKafkaProtoMapping().isEmpty()) {
            throw new ConfigurationException("config SINK_KAFKA_PROTO_MAPPING should contain at least one field mapping");
        }
        if (sinkConfig.isSinkKafkaSchemaRegistryStencilEnable()) {
            validateNotEmpty(sinkConfig.getSinkKafkaSchemaRegistryStencilUrls(), "SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS");
        }
    }

    /**
     * Validates that a configuration value is present and not blank.
     *
     * @param configValue the value to validate
     * @param configName  the configuration key name used in the error message
     * @throws ConfigurationException if the value is null or blank
     */
    private void validateNotEmpty(String configValue, String configName) {
        if (configValue == null || configValue.trim().isEmpty()) {
            throw new ConfigurationException(String.format("config %s should not be empty", configName));
        }
    }
}
