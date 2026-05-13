package com.gotocompany.depot.config;

import com.gotocompany.depot.config.converter.SchemaRegistryHeadersConverter;
import com.gotocompany.depot.config.converter.SchemaRegistryRefreshConverter;
import com.gotocompany.stencil.cache.SchemaRefreshStrategy;
import org.aeonbits.owner.Config;
import org.apache.hc.core5.http.Header;

import java.util.List;

@Config.DisableFeature(Config.DisableableFeature.PARAMETER_FORMATTING)
public interface KafkaSinkConfig extends Config {

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
    boolean isSinkKafkaProduceLargeMessageEnabled();

    @Key("SINK_KAFKA_STREAM")
    @DefaultValue("")
    String getSinkKafkaStream();

    // Sink Stencil Client configs (separate from the source Stencil Client)

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS")
    String getSinkKafkaSchemaRegistryStencilUrls();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH")
    @DefaultValue("true")
    Boolean getSinkKafkaSchemaRegistryStencilCacheAutoRefresh();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY")
    @ConverterClass(SchemaRegistryRefreshConverter.class)
    @DefaultValue("VERSION_BASED_REFRESH")
    SchemaRefreshStrategy getSinkKafkaSchemaRegistryStencilRefreshStrategy();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_AUTH_BEARER_TOKEN")
    @DefaultValue("")
    String getSinkKafkaSchemaRegistryStencilFetchAuthBearerToken();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS")
    @DefaultValue("900000")
    Long getSinkKafkaSchemaRegistryStencilCacheTtlMs();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getSinkKafkaSchemaRegistryStencilFetchTimeoutMs();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS")
    @DefaultValue("60000")
    Long getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES")
    @DefaultValue("4")
    Integer getSinkKafkaSchemaRegistryStencilFetchRetries();

    @Key("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS")
    @TokenizerClass(SchemaRegistryHeadersConverter.class)
    @ConverterClass(SchemaRegistryHeadersConverter.class)
    @DefaultValue("")
    List<Header> getSinkKafkaSchemaRegistryStencilFetchHeaders();

    // Source proto class (for parsing incoming messages)

    @Key("INPUT_SCHEMA_PROTO_CLASS")
    @DefaultValue("")
    String getInputSchemaProtoClass();

    // Source Stencil Client configs

    @Key("SCHEMA_REGISTRY_STENCIL_ENABLE")
    @DefaultValue("false")
    Boolean isSchemaRegistryStencilEnable();

    @Key("SCHEMA_REGISTRY_STENCIL_URLS")
    @DefaultValue("")
    String getSchemaRegistryStencilUrls();

    @Key("SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH")
    @DefaultValue("true")
    Boolean getSchemaRegistryStencilCacheAutoRefresh();

    @Key("SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY")
    @ConverterClass(SchemaRegistryRefreshConverter.class)
    @DefaultValue("VERSION_BASED_REFRESH")
    SchemaRefreshStrategy getSchemaRegistryStencilRefreshStrategy();

    @Key("SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS")
    @DefaultValue("900000")
    Long getSchemaRegistryStencilCacheTtlMs();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getSchemaRegistryStencilFetchTimeoutMs();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS")
    @DefaultValue("60000")
    Long getSchemaRegistryStencilFetchBackoffMinMs();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES")
    @DefaultValue("4")
    Integer getSchemaRegistryStencilFetchRetries();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS")
    @TokenizerClass(SchemaRegistryHeadersConverter.class)
    @ConverterClass(SchemaRegistryHeadersConverter.class)
    @DefaultValue("")
    List<Header> getSchemaRegistryStencilFetchHeaders();

    @Key("SINK_KAFKA_LINGER_MS")
    @DefaultValue("")
    String getSinkKafkaLingerMs();
}
