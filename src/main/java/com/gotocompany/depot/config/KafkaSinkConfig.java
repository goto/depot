package com.gotocompany.depot.config;

import org.aeonbits.owner.Config;

@Config.DisableFeature(Config.DisableableFeature.PARAMETER_FORMATTING)
public interface KafkaSinkConfig extends SinkConfig {

    @Key("SINK_KAFKA_BROKERS")
    String getSinkKafkaBrokers();

    @Key("SINK_KAFKA_TOPIC")
    String getSinkKafkaTopic();

    @Key("SINK_KAFKA_PROTO_MESSAGE")
    String getSinkKafkaProtoMessage();

    @Key("SINK_KAFKA_PROTO_KEY")
    @DefaultValue("")
    String getSinkKafkaProtoKey();

    @Key("SINK_KAFKA_PROTO_MAPPING")
    String getSinkKafkaProtoMapping();

    @Key("SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE")
    @DefaultValue("false")
    boolean isSinkKafkaProduceLargeMessageEnable();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS")
    String getSinkKafkaSchemaRegistryStencilUrls();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH")
    @DefaultValue("false")
    boolean isSinkKafkaSchemaRegistryStencilCacheAutoRefresh();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY")
    @DefaultValue("VERSION_BASED_REFRESH")
    String getSinkKafkaSchemaRegistryStencilRefreshStrategy();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_AUTH_BEARER_TOKEN")
    @DefaultValue("")
    String getSinkKafkaSchemaRegistryStencilFetchAuthBearerToken();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS")
    @DefaultValue("86400000")
    long getSinkKafkaSchemaRegistryStencilCacheTtlMs();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS")
    @DefaultValue("10000")
    long getSinkKafkaSchemaRegistryStencilFetchTimeoutMs();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS")
    @DefaultValue("5000")
    long getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES")
    @DefaultValue("4")
    int getSinkKafkaSchemaRegistryStencilFetchRetries();

    @Key("SINK_KAFKA_LINGER_MS")
    @DefaultValue("0")
    String getSinkKafkaLingerMs();

    @Key("SINK_KAFKA_ACKS")
    @DefaultValue("all")
    String getSinkKafkaAcks();
}
