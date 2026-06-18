package com.gotocompany.depot.config;

import com.gotocompany.depot.config.converter.ConfToListConverter;
import com.gotocompany.depot.config.converter.SchemaRegistryHeadersConverter;
import com.gotocompany.depot.config.converter.SchemaRegistryRefreshConverter;
import com.gotocompany.depot.config.converter.SinkConnectorSchemaDataTypeConverter;
import com.gotocompany.depot.config.converter.SinkConnectorSchemaMessageModeConverter;
import com.gotocompany.depot.config.enums.SinkConnectorSchemaDataType;
import com.gotocompany.depot.message.ProtoUnknownFieldValidationType;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.stencil.cache.SchemaRefreshStrategy;
import org.aeonbits.owner.Config;
import org.apache.hc.core5.http.Header;

import java.util.List;

/**
 * Base Owner configuration interface shared by every Depot sink.
 *
 * <p>{@code SinkConfig} defines the settings that are common to all sinks, independent of the
 * destination system. These fall into a few groups:
 * <ul>
 *   <li>Stencil schema-registry access: whether the registry is enabled, its URLs, fetch timeout,
 *       retries and backoff, request headers, refresh strategy, and cache behaviour.</li>
 *   <li>Connector schema selection: the data type (Protobuf or JSON), the message mode (log key or
 *       log message), the Protobuf message and key classes, and JSON parser behaviour.</li>
 *   <li>Unknown-field handling: whether unknown Protobuf fields are allowed, how they are validated,
 *       and whether that validation is instrumented.</li>
 *   <li>Record shaping common to sinks: the metric naming prefix, metadata columns, and default
 *       field-value emission.</li>
 * </ul>
 *
 * <p>Sink-specific interfaces such as {@link BigQuerySinkConfig}, {@link BigTableSinkConfig},
 * {@link RedisSinkConfig}, and {@link HttpClientConfig} extend this interface to add their own
 * properties. The {@code @Config.DisableFeature(PARAMETER_FORMATTING)} annotation disables Owner's
 * parameter expansion so that property values are used literally.
 */
@Config.DisableFeature(Config.DisableableFeature.PARAMETER_FORMATTING)
public interface SinkConfig extends Config {

    /**
     * Indicates whether schema resolution through the Stencil schema registry is enabled.
     *
     * <p>When {@code true}, Depot fetches Protobuf descriptors dynamically from Stencil instead of
     * relying solely on compiled classes. Bound to the {@code SCHEMA_REGISTRY_STENCIL_ENABLE}
     * property; defaults to {@code false}.
     *
     * @return {@code true} if the Stencil schema registry should be used, {@code false} otherwise
     */
    @Key("SCHEMA_REGISTRY_STENCIL_ENABLE")
    @DefaultValue("false")
    Boolean isSchemaRegistryStencilEnable();

    /**
     * Returns the timeout, in milliseconds, for fetching a schema from the Stencil registry.
     *
     * <p>Bound to the {@code SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS} property; defaults to
     * {@code 10000} (10 seconds).
     *
     * @return the Stencil fetch timeout in milliseconds
     */
    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getSchemaRegistryStencilFetchTimeoutMs();

    /**
     * Returns the number of times a failed Stencil schema fetch is retried.
     *
     * <p>Bound to the {@code SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES} property; defaults to {@code 4}.
     *
     * @return the maximum number of Stencil fetch retries
     */
    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES")
    @DefaultValue("4")
    Integer getSchemaRegistryStencilFetchRetries();

    /**
     * Returns the minimum backoff, in milliseconds, between Stencil schema fetch retries.
     *
     * <p>Bound to the {@code SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS} property; defaults to
     * {@code 60000} (60 seconds).
     *
     * @return the minimum Stencil fetch backoff in milliseconds
     */
    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS")
    @DefaultValue("60000")
    Long getSchemaRegistryStencilFetchBackoffMinMs();

    /**
     * Returns the strategy that governs how cached Stencil schemas are refreshed.
     *
     * <p>Bound to the {@code SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY} property, converted with
     * {@link SchemaRegistryRefreshConverter}, and defaulting to {@code VERSION_BASED_REFRESH}. The
     * resulting {@link SchemaRefreshStrategy} determines whether and how the Stencil client reloads
     * descriptors.
     *
     * @return the configured Stencil schema refresh strategy
     */
    @Key("SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY")
    @ConverterClass(SchemaRegistryRefreshConverter.class)
    @DefaultValue("VERSION_BASED_REFRESH")
    SchemaRefreshStrategy getSchemaRegistryStencilRefreshStrategy();

    /**
     * Returns the HTTP headers sent with each Stencil schema fetch request.
     *
     * <p>Bound to the {@code SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS} property and both tokenized and
     * converted by {@link SchemaRegistryHeadersConverter} into a {@code List<Header>}; it defaults to
     * an empty string, yielding no extra headers. This is useful for authenticating against a secured
     * Stencil endpoint.
     *
     * @return the list of HTTP headers attached to Stencil requests
     */
    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS")
    @TokenizerClass(SchemaRegistryHeadersConverter.class)
    @ConverterClass(SchemaRegistryHeadersConverter.class)
    @DefaultValue("")
    List<Header> getSchemaRegistryStencilFetchHeaders();

    /**
     * Indicates whether the Stencil schema cache is automatically refreshed in the background.
     *
     * <p>Bound to the {@code SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH} property; defaults to
     * {@code true}.
     *
     * @return {@code true} if the schema cache should auto-refresh, {@code false} otherwise
     */
    @Key("SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH")
    @DefaultValue("true")
    Boolean getSchemaRegistryStencilCacheAutoRefresh();

    /**
     * Returns the time-to-live, in milliseconds, for entries in the Stencil schema cache.
     *
     * <p>Bound to the {@code SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS} property; defaults to
     * {@code 900000} (15 minutes).
     *
     * @return the Stencil schema cache TTL in milliseconds
     */
    @Key("SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS")
    @DefaultValue("900000")
    Long getSchemaRegistryStencilCacheTtlMs();

    /**
     * Returns the Stencil registry URL(s) from which schemas are fetched.
     *
     * <p>Bound to the {@code SCHEMA_REGISTRY_STENCIL_URLS} property; has no default and is required
     * when {@link #isSchemaRegistryStencilEnable()} is {@code true}.
     *
     * @return the configured Stencil registry URL(s)
     */
    @Key("SCHEMA_REGISTRY_STENCIL_URLS")
    String getSchemaRegistryStencilUrls();

    /**
     * Returns the prefix prepended to the names of application-level metrics emitted by Depot.
     *
     * <p>Bound to the {@code SINK_METRICS_APPLICATION_PREFIX} property; defaults to
     * {@code application_}.
     *
     * @return the metric name prefix
     */
    @Key("SINK_METRICS_APPLICATION_PREFIX")
    @DefaultValue("application_")
    String getMetricsApplicationPrefix();

    /**
     * Returns the fully qualified Protobuf class used to parse the message body.
     *
     * <p>Applies when the schema data type is Protobuf; the class name is resolved to a descriptor
     * from the classpath or from Stencil. Bound to the
     * {@code SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS} property; defaults to an empty string.
     *
     * @return the Protobuf message class name, or an empty string when not configured
     */
    @Key("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS")
    @DefaultValue("")
    String getSinkConnectorSchemaProtoMessageClass();

    /**
     * Returns the fully qualified Protobuf class used to parse the message key.
     *
     * <p>Applies when the schema data type is Protobuf and the key is parsed, for example in log-key
     * message mode. Bound to the {@code SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS} property; defaults to an
     * empty string.
     *
     * @return the Protobuf key class name, or an empty string when not configured
     */
    @Key("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS")
    @DefaultValue("")
    String getSinkConnectorSchemaProtoKeyClass();

    /**
     * Indicates whether the JSON parser treats all scalar values as strings.
     *
     * <p>When {@code true}, the JSON message parser reads every leaf value as a string rather than
     * inferring numeric or boolean types. Bound to the
     * {@code SINK_CONNECTOR_SCHEMA_JSON_PARSER_STRING_MODE_ENABLED} property; defaults to {@code true}.
     *
     * @return {@code true} if JSON values are parsed in string mode, {@code false} otherwise
     */
    @Key("SINK_CONNECTOR_SCHEMA_JSON_PARSER_STRING_MODE_ENABLED")
    @DefaultValue("true")
    boolean getSinkConnectorSchemaJsonParserStringModeEnabled();

    /**
     * Returns the wire format used to interpret incoming messages.
     *
     * <p>Bound to the {@code SINK_CONNECTOR_SCHEMA_DATA_TYPE} property, converted with
     * {@link SinkConnectorSchemaDataTypeConverter}, and defaulting to
     * {@link SinkConnectorSchemaDataType#PROTOBUF}. It selects between Protobuf and JSON parsing, and
     * therefore which message parser Depot uses.
     *
     * @return the configured connector schema data type
     */
    @Key("SINK_CONNECTOR_SCHEMA_DATA_TYPE")
    @ConverterClass(SinkConnectorSchemaDataTypeConverter.class)
    @DefaultValue("PROTOBUF")
    SinkConnectorSchemaDataType getSinkConnectorSchemaDataType();

    /**
     * Returns which part of each record is parsed as the payload.
     *
     * <p>Bound to the {@code SINK_CONNECTOR_SCHEMA_MESSAGE_MODE} property, converted with
     * {@link SinkConnectorSchemaMessageModeConverter}, and defaulting to
     * {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE}. It selects whether the record's log message
     * ({@link SinkConnectorSchemaMessageMode#LOG_MESSAGE}) or log key
     * ({@link SinkConnectorSchemaMessageMode#LOG_KEY}) provides the data to deserialize.
     *
     * @return the configured connector schema message mode
     */
    @Key("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE")
    @ConverterClass(SinkConnectorSchemaMessageModeConverter.class)
    @DefaultValue("LOG_MESSAGE")
    SinkConnectorSchemaMessageMode getSinkConnectorSchemaMessageMode();

    /**
     * Indicates whether Protobuf messages containing unknown fields are accepted.
     *
     * <p>When {@code false}, encountering unknown fields raises an
     * {@link com.gotocompany.depot.exception.UnknownFieldsException}; when {@code true}, such fields
     * are tolerated. Bound to the {@code SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE}
     * property; defaults to {@code false}.
     *
     * @return {@code true} if unknown Protobuf fields are allowed, {@code false} otherwise
     */
    @Key("SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE")
    @DefaultValue("false")
    boolean getSinkConnectorSchemaProtoAllowUnknownFieldsEnable();

    /**
     * Indicates whether record metadata columns are appended to each output record.
     *
     * <p>When {@code true}, the metadata columns described by {@link #getMetadataColumnsTypes()} are
     * added alongside the message fields. Bound to the {@code SINK_ADD_METADATA_ENABLED} property;
     * defaults to {@code false}.
     *
     * @return {@code true} if metadata columns should be added, {@code false} otherwise
     */
    @DefaultValue("false")
    @Key("SINK_ADD_METADATA_ENABLED")
    boolean shouldAddMetadata();

    /**
     * Returns the metadata columns to add, each expressed as a name-to-type pair.
     *
     * <p>Bound to the {@code SINK_METADATA_COLUMNS_TYPES} property and parsed by
     * {@link ConfToListConverter} (using {@link ConfToListConverter#ELEMENT_SEPARATOR} to split
     * entries) into a list of {@link TupleString} values, where each tuple's first element is the
     * column name and its second element is the type. It defaults to an empty string, yielding no
     * metadata columns, and is honoured only when {@link #shouldAddMetadata()} is {@code true}.
     *
     * @return the list of metadata column name/type pairs
     */
    @DefaultValue("")
    @Key("SINK_METADATA_COLUMNS_TYPES")
    @ConverterClass(ConfToListConverter.class)
    @Separator(ConfToListConverter.ELEMENT_SEPARATOR)
    List<TupleString> getMetadataColumnsTypes();

    /**
     * Indicates whether Protobuf default values are emitted for fields absent from a message.
     *
     * <p>When {@code true}, fields not present in the payload are written with their Protobuf default
     * value; when {@code false}, such fields are omitted. Bound to the
     * {@code SINK_DEFAULT_FIELD_VALUE_ENABLE} property; defaults to {@code true}. Individual sinks may
     * override this with their own key.
     *
     * @return {@code true} if default field values should be written, {@code false} otherwise
     */
    @Key("SINK_DEFAULT_FIELD_VALUE_ENABLE")
    @DefaultValue("true")
    boolean getSinkDefaultFieldValueEnable();

    /**
     * Returns the strategy used to detect unknown fields within Protobuf payloads.
     *
     * <p>Bound to the {@code SINK_CONNECTOR_SCHEMA_PROTO_UNKNOWN_FIELDS_VALIDATION} property and
     * defaulting to {@link ProtoUnknownFieldValidationType#MESSAGE}. The value controls whether a
     * single message, only the first element of a message list, or every element of a message list is
     * inspected for unknown fields.
     *
     * @return the configured unknown-field validation type
     */
    @Key("SINK_CONNECTOR_SCHEMA_PROTO_UNKNOWN_FIELDS_VALIDATION")
    @DefaultValue("MESSAGE")
    ProtoUnknownFieldValidationType getSinkConnectorSchemaProtoUnknownFieldsValidation();

    /**
     * Indicates whether instrumentation metrics are emitted for unknown-field validation.
     *
     * <p>Bound to the
     * {@code SINK_CONNECTOR_SCHEMA_PROTO_UNKNOWN_FIELDS_VALIDATION_INSTRUMENTATION_ENABLE} property;
     * defaults to {@code false}.
     *
     * @return {@code true} if unknown-field validation should be instrumented, {@code false} otherwise
     */
    @Key("SINK_CONNECTOR_SCHEMA_PROTO_UNKNOWN_FIELDS_VALIDATION_INSTRUMENTATION_ENABLE")
    @DefaultValue("false")
    boolean getSinkConnectorSchemaProtoUnknownFieldsValidationInstrumentationEnable();
}
