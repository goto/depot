package com.gotocompany.depot.config;

import com.gotocompany.depot.config.converter.EmptyStringToNull;
import com.gotocompany.depot.config.converter.KafkaProtoMappingConverter;
import com.gotocompany.depot.config.converter.SchemaRegistryRefreshConverter;
import com.gotocompany.depot.config.preprocessor.Trim;
import com.gotocompany.stencil.cache.SchemaRefreshStrategy;
import org.aeonbits.owner.Config;

import java.util.Map;

/**
 * Configuration for the Kafka sink, loading all of the {@code SINK_KAFKA_} environment variables.
 */
@Config.DisableFeature(Config.DisableableFeature.PARAMETER_FORMATTING)
@Config.PreprocessorClasses({Trim.class})
public interface KafkaSinkConfig extends SinkConfig {

    /**
     * Returns the comma separated list of Kafka brokers hosting the output topic.
     *
     * @return the configured Kafka brokers
     */
    @Key("SINK_KAFKA_BROKERS")
    String getSinkKafkaBrokers();

    /**
     * Returns the output Kafka topic that records are produced to.
     *
     * @return the configured output topic
     */
    @Key("SINK_KAFKA_TOPIC")
    String getSinkKafkaTopic();

    /**
     * Returns the fully qualified output value proto class name.
     *
     * @return the configured value proto class name
     */
    @Key("SINK_KAFKA_PROTO_MESSAGE")
    String getSinkKafkaProtoMessage();

    /**
     * Returns the fully qualified output key proto class name.
     *
     * @return the configured key proto class name, or {@code null} when no key proto is configured
     */
    @Key("SINK_KAFKA_PROTO_KEY")
    @DefaultValue("")
    @ConverterClass(EmptyStringToNull.class)
    String getSinkKafkaProtoKey();

    /**
     * Returns the parsed proto mapping of output field names to CEL expressions.
     *
     * @return the configured proto mapping, empty when not configured
     */
    @Key("SINK_KAFKA_PROTO_MAPPING")
    @DefaultValue("")
    @ConverterClass(KafkaProtoMappingConverter.class)
    Map<String, String> getSinkKafkaProtoMapping();

    /**
     * Returns whether large message mode is enabled for the producer.
     *
     * @return {@code true} if large message mode is enabled, {@code false} otherwise
     */
    @Key("SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE")
    @DefaultValue("false")
    boolean isSinkKafkaProduceLargeMessageEnable();

    /**
     * Returns whether the sink Stencil schema registry is enabled.
     *
     * @return {@code true} if the sink Stencil registry is enabled, {@code false} otherwise
     */
    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_ENABLE")
    @DefaultValue("false")
    boolean isSinkKafkaSchemaRegistryStencilEnable();

    /**
     * Returns the sink Stencil server urls hosting the output proto descriptors.
     *
     * @return the configured sink Stencil urls
     */
    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS")
    @DefaultValue("")
    String getSinkKafkaSchemaRegistryStencilUrls();

    /**
     * Returns whether the sink Stencil cache auto refresh is enabled.
     *
     * @return {@code true} if the sink Stencil cache auto refresh is enabled, {@code false} otherwise
     */
    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH")
    @DefaultValue("true")
    Boolean getSinkKafkaSchemaRegistryStencilCacheAutoRefresh();

    /**
     * Returns the sink Stencil schema refresh strategy.
     *
     * @return the configured sink Stencil refresh strategy
     */
    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY")
    @ConverterClass(SchemaRegistryRefreshConverter.class)
    @DefaultValue("VERSION_BASED_REFRESH")
    SchemaRefreshStrategy getSinkKafkaSchemaRegistryStencilRefreshStrategy();

    /**
     * Returns the bearer token used when fetching the sink descriptors.
     *
     * @return the configured bearer token, or {@code null} when not configured
     */
    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_AUTH_BEARER_TOKEN")
    @DefaultValue("")
    @ConverterClass(EmptyStringToNull.class)
    String getSinkKafkaSchemaRegistryStencilFetchAuthBearerToken();

    /**
     * Returns the sink Stencil cache time to live in milliseconds.
     *
     * @return the configured sink Stencil cache ttl in milliseconds
     */
    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS")
    @DefaultValue("900000")
    Long getSinkKafkaSchemaRegistryStencilCacheTtlMs();

    /**
     * Returns the sink Stencil fetch timeout in milliseconds.
     *
     * @return the configured sink Stencil fetch timeout in milliseconds
     */
    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getSinkKafkaSchemaRegistryStencilFetchTimeoutMs();

    /**
     * Returns the minimum backoff between sink Stencil fetch retries in milliseconds.
     *
     * @return the configured sink Stencil fetch backoff in milliseconds
     */
    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS")
    @DefaultValue("60000")
    Long getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs();

    /**
     * Returns the number of sink Stencil fetch retries.
     *
     * @return the configured sink Stencil fetch retries
     */
    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES")
    @DefaultValue("4")
    Integer getSinkKafkaSchemaRegistryStencilFetchRetries();
}
