package com.gotocompany.depot.config;

import com.gotocompany.depot.config.converter.EmptyStringToNull;
import com.gotocompany.depot.config.converter.JsonToPropertiesConverter;
import com.gotocompany.depot.config.converter.RedisSinkDataTypeConverter;
import com.gotocompany.depot.config.converter.RedisSinkDeploymentTypeConverter;
import com.gotocompany.depot.config.converter.RedisSinkTtlTypeConverter;
import com.gotocompany.depot.config.preprocessor.Trim;
import com.gotocompany.depot.redis.enums.RedisSinkDataType;
import com.gotocompany.depot.redis.enums.RedisSinkDeploymentType;
import com.gotocompany.depot.redis.enums.RedisSinkTtlType;
import org.aeonbits.owner.Config;

import java.util.Properties;


/**
 * Owner configuration interface for the Redis sink.
 *
 * <p>{@code RedisSinkConfig} describes how Depot connects to Redis and how each message is mapped onto
 * a Redis data structure. It covers connection endpoints and credentials, socket and connection
 * timeouts, retry behaviour, the deployment topology (standalone or cluster), the target data type
 * (list, hash set, or key-value), key templating, time-to-live handling, and field-to-column
 * mappings. It extends {@link SinkConfig} so the shared sink and schema settings are also available.
 *
 * <p>The {@code @Config.DisableFeature(PARAMETER_FORMATTING)} annotation disables Owner's parameter
 * expansion, and {@code @Config.PreprocessorClasses({Trim.class})} applies {@link Trim} so that
 * surrounding whitespace is stripped from every property value before conversion.
 */
@Config.DisableFeature(Config.DisableableFeature.PARAMETER_FORMATTING)
@Config.PreprocessorClasses({Trim.class})
public interface RedisSinkConfig extends SinkConfig {
    /**
     * Returns the comma-separated Redis connection URL(s) the sink connects to.
     *
     * <p>For a standalone deployment a single {@code host:port} URL is expected; for a cluster
     * deployment one or more seed-node URLs are supplied. Bound to the {@code SINK_REDIS_URLS}
     * property; has no default. An invalid or missing value causes a
     * {@link com.gotocompany.depot.exception.ConfigurationException} when the client is created.
     *
     * @return the configured Redis URL(s)
     */
    @Key("SINK_REDIS_URLS")
    String getSinkRedisUrls();

    /**
     * Returns the maximum number of attempts the Redis cluster client makes for an operation.
     *
     * <p>Applies to cluster deployments, where the client retries an operation across nodes up to this
     * many times before failing. Bound to the {@code SINK_REDIS_CLUSTER_MAX_ATTEMPTS} property;
     * defaults to {@code 5}.
     *
     * @return the maximum number of Redis cluster attempts
     */
    @Key("SINK_REDIS_CLUSTER_MAX_ATTEMPTS")
    @DefaultValue("5")
    int getSinkRedisMaxAttempts();

    /**
     * Returns the username used to authenticate with Redis, or {@code null} when none is set.
     *
     * <p>Bound to the {@code SINK_REDIS_AUTH_USERNAME} property and converted with
     * {@link EmptyStringToNull}, so a blank value is normalized to {@code null} (no authentication
     * username). Has no default.
     *
     * @return the Redis authentication username, or {@code null} if unset or blank
     */
    @Key("SINK_REDIS_AUTH_USERNAME")
    @ConverterClass(EmptyStringToNull.class)
    String getSinkRedisAuthUsername();

    /**
     * Returns the password used to authenticate with Redis, or {@code null} when none is set.
     *
     * <p>Bound to the {@code SINK_REDIS_AUTH_PASSWORD} property and converted with
     * {@link EmptyStringToNull}, so a blank value is normalized to {@code null} (no authentication
     * password). Has no default.
     *
     * @return the Redis authentication password, or {@code null} if unset or blank
     */
    @Key("SINK_REDIS_AUTH_PASSWORD")
    @ConverterClass(EmptyStringToNull.class)
    String getSinkRedisAuthPassword();

    /**
     * Returns the connection timeout, in milliseconds, used when establishing a Redis connection.
     *
     * <p>Bound to the {@code SINK_REDIS_CONNECTION_TIMEOUT_MS} property; defaults to {@code 5000}
     * (5 seconds).
     *
     * @return the Redis connection timeout in milliseconds
     */
    @Key("SINK_REDIS_CONNECTION_TIMEOUT_MS")
    @DefaultValue("5000")
    int getSinkRedisConnectionTimeoutMs();

    /**
     * Returns the socket (read) timeout, in milliseconds, for Redis operations.
     *
     * <p>Bound to the {@code SINK_REDIS_SOCKET_TIMEOUT_MS} property; defaults to {@code 10000}
     * (10 seconds).
     *
     * @return the Redis socket timeout in milliseconds
     */
    @Key("SINK_REDIS_SOCKET_TIMEOUT_MS")
    @DefaultValue("10000")
    int getSinkRedisSocketTimeoutMs();

    /**
     * Returns the template used to build the Redis key for each record.
     *
     * <p>The template is rendered against message fields (see
     * {@link com.gotocompany.depot.common.Template}) to derive the key under which the record's value,
     * list entry, or hash is stored. Bound to the {@code SINK_REDIS_KEY_TEMPLATE} property; has no
     * default.
     *
     * @return the Redis key template expression
     */
    @Key("SINK_REDIS_KEY_TEMPLATE")
    String getSinkRedisKeyTemplate();

    /**
     * Returns the Redis data structure that records are written as.
     *
     * <p>Bound to the {@code SINK_REDIS_DATA_TYPE} property, converted with
     * {@link RedisSinkDataTypeConverter}, and defaulting to {@link RedisSinkDataType#HASHSET}. The
     * selected type determines which parser and writer Depot uses, namely
     * {@link RedisSinkDataType#LIST}, {@link RedisSinkDataType#HASHSET}, or
     * {@link RedisSinkDataType#KEYVALUE}.
     *
     * @return the configured Redis data type
     */
    @Key("SINK_REDIS_DATA_TYPE")
    @DefaultValue("HASHSET")
    @ConverterClass(RedisSinkDataTypeConverter.class)
    RedisSinkDataType getSinkRedisDataType();

    /**
     * Returns the time-to-live (TTL) strategy applied to written Redis entries.
     *
     * <p>Bound to the {@code SINK_REDIS_TTL_TYPE} property, converted with
     * {@link RedisSinkTtlTypeConverter}, and defaulting to {@link RedisSinkTtlType#DISABLE}. It selects
     * whether no expiry is set ({@link RedisSinkTtlType#DISABLE}), an absolute expiry instant is used
     * ({@link RedisSinkTtlType#EXACT_TIME}), or a relative duration is used
     * ({@link RedisSinkTtlType#DURATION}); the accompanying magnitude comes from
     * {@link #getSinkRedisTtlValue()}.
     *
     * @return the configured Redis TTL type
     */
    @Key("SINK_REDIS_TTL_TYPE")
    @DefaultValue("DISABLE")
    @ConverterClass(RedisSinkTtlTypeConverter.class)
    RedisSinkTtlType getSinkRedisTtlType();

    /**
     * Returns the numeric value paired with the configured {@linkplain #getSinkRedisTtlType() TTL
     * type}.
     *
     * <p>It is interpreted as a duration in seconds for {@link RedisSinkTtlType#DURATION}, as an
     * absolute Unix time (in seconds) for {@link RedisSinkTtlType#EXACT_TIME}, and is ignored when TTL
     * is {@link RedisSinkTtlType#DISABLE}. Bound to the {@code SINK_REDIS_TTL_VALUE} property; defaults
     * to {@code 0}. A negative value is rejected with a
     * {@link com.gotocompany.depot.exception.ConfigurationException}.
     *
     * @return the configured Redis TTL value
     */
    @Key("SINK_REDIS_TTL_VALUE")
    @DefaultValue("0")
    long getSinkRedisTtlValue();

    /**
     * Returns the Redis deployment topology to connect to.
     *
     * <p>Bound to the {@code SINK_REDIS_DEPLOYMENT_TYPE} property, converted with
     * {@link RedisSinkDeploymentTypeConverter}, and defaulting to {@code Standalone}. It selects
     * between {@link RedisSinkDeploymentType#STANDALONE} and {@link RedisSinkDeploymentType#CLUSTER},
     * which determines the client implementation Depot instantiates.
     *
     * @return the configured Redis deployment type
     */
    @Key("SINK_REDIS_DEPLOYMENT_TYPE")
    @DefaultValue("Standalone")
    @ConverterClass(RedisSinkDeploymentTypeConverter.class)
    RedisSinkDeploymentType getSinkRedisDeploymentType();

    /**
     * Returns the message field whose value is stored when using the key-value data type.
     *
     * <p>Applies when {@link #getSinkRedisDataType()} is {@link RedisSinkDataType#KEYVALUE}: the named
     * field supplies the string value written under the Redis key. Bound to the
     * {@code SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME} property; has no default.
     *
     * @return the field name supplying the key-value payload
     */
    @Key("SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME")
    String getSinkRedisKeyValueDataFieldName();

    /**
     * Returns the message field whose value is appended when using the list data type.
     *
     * <p>Applies when {@link #getSinkRedisDataType()} is {@link RedisSinkDataType#LIST}: the named
     * field supplies the element pushed onto the Redis list. Bound to the
     * {@code SINK_REDIS_LIST_DATA_FIELD_NAME} property; has no default.
     *
     * @return the field name supplying the list element
     */
    @Key("SINK_REDIS_LIST_DATA_FIELD_NAME")
    String getSinkRedisListDataFieldName();

    /**
     * Returns the mapping from message fields to Redis hash field (column) names.
     *
     * <p>Applies when {@link #getSinkRedisDataType()} is {@link RedisSinkDataType#HASHSET}: the mapping
     * controls which message fields become hash fields and under what names. Bound to the
     * {@code SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING} property, parsed from JSON by
     * {@link JsonToPropertiesConverter} (which also rejects duplicate target names), and defaulting to
     * an empty value.
     *
     * @return the field-to-column mapping as a {@link java.util.Properties} instance
     */
    @Key("SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING")
    @ConverterClass(JsonToPropertiesConverter.class)
    @DefaultValue("")
    Properties getSinkRedisHashsetFieldToColumnMapping();

    /**
     * Indicates whether Protobuf default values are written for fields absent from a message.
     *
     * <p>This Redis-specific property is read from the {@code SINK_REDIS_DEFAULT_FIELD_VALUE_ENABLE}
     * key and shadows {@link SinkConfig#getSinkDefaultFieldValueEnable()}. When {@code true}, fields
     * not present in the payload are written with their Protobuf default value; when {@code false},
     * such fields are omitted. Defaults to {@code true}.
     *
     * @return {@code true} if default field values should be written, {@code false} otherwise
     */
    @Key("SINK_REDIS_DEFAULT_FIELD_VALUE_ENABLE")
    @DefaultValue("true")
    boolean getSinkDefaultFieldValueEnable();

    /**
     * Returns the maximum number of times Depot retries establishing the initial Redis connection.
     *
     * <p>Bound to the {@code SINK_REDIS_CONNECTION_MAX_RETRIES} property; defaults to {@code 1}.
     *
     * @return the maximum number of Redis connection retries
     */
    @Key("SINK_REDIS_CONNECTION_MAX_RETRIES")
    @DefaultValue("1")
    int getSinkRedisConnectionMaxRetries();

    /**
     * Returns the backoff delay, in milliseconds, between Redis connection retry attempts.
     *
     * <p>Bound to the {@code SINK_REDIS_CONNECTION_RETRY_BACKOFF_MS} property; defaults to {@code 2000}
     * (2 seconds).
     *
     * @return the Redis connection retry backoff in milliseconds
     */
    @Key("SINK_REDIS_CONNECTION_RETRY_BACKOFF_MS")
    @DefaultValue("2000")
    long getSinkRedisConnectionRetryBackoffMs();
}
