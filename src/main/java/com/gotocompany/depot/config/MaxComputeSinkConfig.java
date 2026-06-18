package com.gotocompany.depot.config;

import com.aliyun.odps.tunnel.io.CompressOption;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.converter.ConfToListConverter;
import com.gotocompany.depot.config.converter.KeyValuePairsToMapConverter;
import com.gotocompany.depot.config.converter.LocalDateTimeConverter;
import com.gotocompany.depot.config.converter.MaxComputeOdpsGlobalSettingsConverter;
import com.gotocompany.depot.config.converter.ZoneIdConverter;
import com.gotocompany.depot.maxcompute.enumeration.MaxComputeTimestampDataType;
import org.aeonbits.owner.Config;

import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * Owner configuration interface for the Alibaba Cloud MaxCompute (ODPS) sink.
 *
 * <p>{@code MaxComputeSinkConfig} declares the full set of properties Depot uses to write to
 * MaxCompute. They cover connection and credentials (ODPS and tunnel endpoints, access keys, project,
 * and schema), table layout (name, partitioning, lifecycle, and metadata columns), the streaming
 * tunnel insert path (compression, session, and slot settings), DDL retry behaviour,
 * table-validation limits, timestamp handling (time zone, valid range, past/future windows,
 * nanosecond and negative-second handling, and truncation), and the Protobuf-to-MaxCompute type
 * mapping for integer, float, double, and decimal values. Unlike the other sink configs, it extends
 * {@link Config} directly rather than {@code SinkConfig}.
 */
public interface MaxComputeSinkConfig extends Config {

    /**
     * Returns the MaxCompute (ODPS) service endpoint URL.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_ODPS_URL} property; has no default.
     *
     * @return the ODPS service endpoint URL
     */
    @Key("SINK_MAXCOMPUTE_ODPS_URL")
    String getMaxComputeOdpsUrl();

    /**
     * Returns the MaxCompute tunnel endpoint URL used for data upload.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_TUNNEL_URL} property; has no default.
     *
     * @return the MaxCompute tunnel endpoint URL
     */
    @Key("SINK_MAXCOMPUTE_TUNNEL_URL")
    String getMaxComputeTunnelUrl();

    /**
     * Returns the access ID (access key ID) used to authenticate with MaxCompute.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_ACCESS_ID} property; has no default.
     *
     * @return the MaxCompute access ID
     */
    @Key("SINK_MAXCOMPUTE_ACCESS_ID")
    String getMaxComputeAccessId();

    /**
     * Returns the access key (secret) used to authenticate with MaxCompute.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_ACCESS_KEY} property; has no default.
     *
     * @return the MaxCompute access key
     */
    @Key("SINK_MAXCOMPUTE_ACCESS_KEY")
    String getMaxComputeAccessKey();

    /**
     * Returns the MaxCompute project that owns the target table.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_PROJECT_ID} property; has no default.
     *
     * @return the MaxCompute project ID
     */
    @Key("SINK_MAXCOMPUTE_PROJECT_ID")
    String getMaxComputeProjectId();

    /**
     * Returns the namespace under which record metadata columns are grouped.
     *
     * <p>When non-empty, metadata columns are nested within a struct column of this name rather than
     * added at the top level. Bound to the {@code SINK_MAXCOMPUTE_METADATA_NAMESPACE} property;
     * defaults to an empty string.
     *
     * @return the metadata namespace, or an empty string for top-level metadata columns
     */
    @Key("SINK_MAXCOMPUTE_METADATA_NAMESPACE")
    @DefaultValue("")
    String getMaxcomputeMetadataNamespace();

    /**
     * Indicates whether record metadata columns are added to each MaxCompute row.
     *
     * <p>When {@code true}, the metadata columns described by {@link #getMetadataColumnsTypes()} are
     * appended to every row. Bound to the {@code SINK_MAXCOMPUTE_ADD_METADATA_ENABLED} property;
     * defaults to {@code true}.
     *
     * @return {@code true} if metadata columns should be added, {@code false} otherwise
     */
    @Key("SINK_MAXCOMPUTE_ADD_METADATA_ENABLED")
    @DefaultValue("true")
    boolean shouldAddMetadata();

    /**
     * Returns the metadata columns to add, each expressed as a name-to-type pair.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_METADATA_COLUMNS_TYPES} property and parsed by
     * {@link ConfToListConverter} (using {@link ConfToListConverter#ELEMENT_SEPARATOR} to split
     * entries) into a list of {@link TupleString} values, where each tuple's first element is the
     * column name and its second element is the type. It defaults to an empty string and is honoured
     * only when {@link #shouldAddMetadata()} is {@code true}.
     *
     * @return the list of metadata column name/type pairs
     */
    @DefaultValue("")
    @Key("SINK_MAXCOMPUTE_METADATA_COLUMNS_TYPES")
    @ConverterClass(ConfToListConverter.class)
    @Separator(ConfToListConverter.ELEMENT_SEPARATOR)
    List<TupleString> getMetadataColumnsTypes();

    /**
     * Returns the MaxCompute schema (namespace within the project) that contains the table.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_SCHEMA} property; defaults to {@code default}.
     *
     * @return the MaxCompute schema name
     */
    @Key("SINK_MAXCOMPUTE_SCHEMA")
    @DefaultValue("default")
    String getMaxComputeSchema();

    /**
     * Returns the MaxCompute column type that Protobuf timestamp fields are mapped to.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_PROTO_TIMESTAMP_TO_MAXCOMPUTE_TYPE} property and
     * defaulting to {@code TIMESTAMP}. The resulting {@link MaxComputeTimestampDataType} selects the
     * destination temporal type (for example timestamp or timestamp without time zone).
     *
     * @return the configured timestamp mapping type
     */
    @Key("SINK_MAXCOMPUTE_PROTO_TIMESTAMP_TO_MAXCOMPUTE_TYPE")
    @DefaultValue("TIMESTAMP")
    MaxComputeTimestampDataType getMaxComputeProtoTimestampToMaxcomputeType();

    /**
     * Indicates whether the MaxCompute table is partitioned.
     *
     * <p>When {@code true}, the table is created or maintained with a partition derived from
     * {@link #getTablePartitionKey()} and {@link #getTablePartitionColumnName()}. Bound to the
     * {@code SINK_MAXCOMPUTE_TABLE_PARTITIONING_ENABLE} property; defaults to {@code false}.
     *
     * @return {@code true} if table partitioning is enabled, {@code false} otherwise
     */
    @Key("SINK_MAXCOMPUTE_TABLE_PARTITIONING_ENABLE")
    @DefaultValue("false")
    Boolean isTablePartitioningEnabled();

    /**
     * Returns the source field used to derive the table partition.
     *
     * <p>Applies when {@link #isTablePartitioningEnabled()} is {@code true}. Bound to the
     * {@code SINK_MAXCOMPUTE_TABLE_PARTITION_KEY} property; has no default.
     *
     * @return the partition source field name
     */
    @Key("SINK_MAXCOMPUTE_TABLE_PARTITION_KEY")
    String getTablePartitionKey();

    /**
     * Returns the name of the partition column written to the MaxCompute table.
     *
     * <p>Applies when {@link #isTablePartitioningEnabled()} is {@code true}; it may differ from
     * {@link #getTablePartitionKey()} when a dedicated partition column is generated. Bound to the
     * {@code SINK_MAXCOMPUTE_TABLE_PARTITION_COLUMN_NAME} property; has no default.
     *
     * @return the partition column name
     */
    @Key("SINK_MAXCOMPUTE_TABLE_PARTITION_COLUMN_NAME")
    String getTablePartitionColumnName();

    /**
     * Returns the time unit used when partitioning by a timestamp.
     *
     * <p>Determines the granularity (for example day) at which timestamp-based partitions are
     * bucketed. Bound to the {@code SINK_MAXCOMPUTE_TABLE_PARTITION_BY_TIMESTAMP_TIME_UNIT} property;
     * defaults to {@code DAY}.
     *
     * @return the timestamp partitioning time unit
     */
    @Key("SINK_MAXCOMPUTE_TABLE_PARTITION_BY_TIMESTAMP_TIME_UNIT")
    @DefaultValue("DAY")
    String getTablePartitionByTimestampTimeUnit();

    /**
     * Returns the name of the MaxCompute table that records are written to.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_TABLE_NAME} property; has no default.
     *
     * @return the MaxCompute table name
     */
    @Key("SINK_MAXCOMPUTE_TABLE_NAME")
    String getMaxComputeTableName();

    /**
     * Returns the table lifecycle, in days, after which MaxCompute data expires.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_TABLE_LIFECYCLE_DAYS} property; has no default, in which
     * case no lifecycle is applied.
     *
     * @return the table lifecycle in days, or {@code null} when unset
     */
    @Key("SINK_MAXCOMPUTE_TABLE_LIFECYCLE_DAYS")
    Long getMaxComputeTableLifecycleDays();

    /**
     * Returns the timeout, in milliseconds, after which a buffered record pack is flushed.
     *
     * <p>A value of {@code -1} disables time-based flushing. Bound to the
     * {@code SINK_MAXCOMPUTE_RECORD_PACK_FLUSH_TIMEOUT_MS} property; defaults to {@code -1}.
     *
     * @return the record-pack flush timeout in milliseconds, or {@code -1} when disabled
     */
    @Key("SINK_MAXCOMPUTE_RECORD_PACK_FLUSH_TIMEOUT_MS")
    @DefaultValue("-1")
    Long getMaxComputeRecordPackFlushTimeoutMs();

    /**
     * Indicates whether payload compression is enabled for streaming tunnel inserts.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_ENABLED} property; defaults
     * to {@code false}.
     *
     * @return {@code true} if streaming-insert compression is enabled, {@code false} otherwise
     */
    @Key("SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_ENABLED")
    @DefaultValue("false")
    boolean isStreamingInsertCompressEnabled();

    /**
     * Returns the compression algorithm used for streaming tunnel inserts.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_ALGORITHM} property and
     * defaulting to {@code ODPS_LZ4_FRAME}. The value is a {@code CompressOption.CompressAlgorithm} and
     * applies when {@link #isStreamingInsertCompressEnabled()} is {@code true}.
     *
     * @return the configured tunnel compression algorithm
     */
    @Key("SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_ALGORITHM")
    @DefaultValue("ODPS_LZ4_FRAME")
    CompressOption.CompressAlgorithm getMaxComputeCompressionAlgorithm();

    /**
     * Returns the compression level used for streaming tunnel inserts.
     *
     * <p>Higher values trade CPU for a smaller payload, as interpreted by the selected
     * {@linkplain #getMaxComputeCompressionAlgorithm() algorithm}. Bound to the
     * {@code SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_LEVEL} property; defaults to {@code 1}.
     *
     * @return the tunnel compression level
     */
    @Key("SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_LEVEL")
    @DefaultValue("1")
    int getMaxComputeCompressionLevel();

    /**
     * Returns the compression strategy used for streaming tunnel inserts.
     *
     * <p>Selects an algorithm-specific compression strategy. Bound to the
     * {@code SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_STRATEGY} property; defaults to {@code 0}.
     *
     * @return the tunnel compression strategy
     */
    @Key("SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_STRATEGY")
    @DefaultValue("0")
    int getMaxComputeCompressionStrategy();

    /**
     * Returns the maximum number of concurrent streaming tunnel sessions maintained.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_STREAMING_INSERT_MAXIMUM_SESSION_COUNT} property; defaults
     * to {@code 2}.
     *
     * @return the maximum number of streaming insert sessions
     */
    @Key("SINK_MAXCOMPUTE_STREAMING_INSERT_MAXIMUM_SESSION_COUNT")
    @DefaultValue("2")
    int getStreamingInsertMaximumSessionCount();

    /**
     * Returns the number of tunnel slots allocated per streaming insert session.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_STREAMING_INSERT_TUNNEL_SLOT_COUNT_PER_SESSION} property;
     * defaults to {@code 1}.
     *
     * @return the tunnel slot count per session
     */
    @Key("SINK_MAXCOMPUTE_STREAMING_INSERT_TUNNEL_SLOT_COUNT_PER_SESSION")
    @DefaultValue("1")
    long getStreamingInsertTunnelSlotCountPerSession();

    /**
     * Returns the time zone used to interpret and write timestamp values.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_ZONE_ID} property, converted with {@link ZoneIdConverter},
     * and defaulting to {@code Asia/Bangkok}. An unparseable value raises a
     * {@link com.gotocompany.depot.exception.ConfigurationException}.
     *
     * @return the configured {@link java.time.ZoneId}
     */
    @Key("SINK_MAXCOMPUTE_ZONE_ID")
    @ConverterClass(ZoneIdConverter.class)
    @DefaultValue("Asia/Bangkok")
    ZoneId getZoneId();

    /**
     * Returns the maximum number of times a failed DDL operation is retried.
     *
     * <p>DDL operations include table creation and schema evolution. Bound to the
     * {@code SINK_MAXCOMPUTE_MAX_DDL_RETRY_COUNT} property; defaults to {@code 10}.
     *
     * @return the maximum number of DDL retries
     */
    @Key("SINK_MAXCOMPUTE_MAX_DDL_RETRY_COUNT")
    @DefaultValue("10")
    int getMaxDdlRetryCount();

    /**
     * Returns the backoff delay, in milliseconds, between DDL retry attempts.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_DDL_RETRY_BACKOFF_MILLIS} property; defaults to
     * {@code 1000} (1 second).
     *
     * @return the DDL retry backoff in milliseconds
     */
    @Key("SINK_MAXCOMPUTE_DDL_RETRY_BACKOFF_MILLIS")
    @DefaultValue("1000")
    long getDdlRetryBackoffMillis();

    /**
     * Returns the global ODPS session settings applied to MaxCompute operations.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_ODPS_GLOBAL_SETTINGS} property and parsed by
     * {@link MaxComputeOdpsGlobalSettingsConverter} from comma-separated {@code key=value} pairs into a
     * {@code Map<String, String>}. It defaults to
     * {@code odps.schema.evolution.enable=true,odps.namespace.schema=true}, which enables schema
     * evolution and namespace schemas.
     *
     * @return the ODPS global settings keyed by setting name
     */
    @Key("SINK_MAXCOMPUTE_ODPS_GLOBAL_SETTINGS")
    @ConverterClass(MaxComputeOdpsGlobalSettingsConverter.class)
    @DefaultValue("odps.schema.evolution.enable=true,odps.namespace.schema=true")
    Map<String, String> getOdpsGlobalSettings();

    /**
     * Returns the regular expression that valid MaxCompute table and column names must match.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_TABLE_VALIDATOR_NAME_REGEX} property; defaults to
     * {@code ^[A-Za-z][A-Za-z0-9_]{0,127}$}, which permits an initial letter followed by up to 127
     * letters, digits, or underscores.
     *
     * @return the table/column name validation regular expression
     */
    @Key("SINK_MAXCOMPUTE_TABLE_VALIDATOR_NAME_REGEX")
    @DefaultValue("^[A-Za-z][A-Za-z0-9_]{0,127}$")
    String getTableValidatorNameRegex();

    /**
     * Returns the maximum number of columns permitted in a MaxCompute table.
     *
     * <p>Used by table validation to reject schemas that would exceed MaxCompute limits. Bound to the
     * {@code SINK_MAXCOMPUTE_TABLE_VALIDATOR_MAX_COLUMNS_PER_TABLE} property; defaults to {@code 1200}.
     *
     * @return the maximum number of columns per table
     */
    @Key("SINK_MAXCOMPUTE_TABLE_VALIDATOR_MAX_COLUMNS_PER_TABLE")
    @DefaultValue("1200")
    int getTableValidatorMaxColumnsPerTable();

    /**
     * Returns the maximum number of partition keys permitted in a MaxCompute table.
     *
     * <p>Used by table validation to enforce MaxCompute partition limits. Bound to the
     * {@code SINK_MAXCOMPUTE_TABLE_VALIDATOR_MAX_PARTITION_KEYS_PER_TABLE} property; defaults to
     * {@code 6}.
     *
     * @return the maximum number of partition keys per table
     */
    @Key("SINK_MAXCOMPUTE_TABLE_VALIDATOR_MAX_PARTITION_KEYS_PER_TABLE")
    @DefaultValue("6")
    int getTableValidatorMaxPartitionKeysPerTable();

    /**
     * Returns the earliest timestamp accepted for ingestion.
     *
     * <p>Records with an event time before this bound are treated as invalid. Bound to the
     * {@code SINK_MAXCOMPUTE_VALID_MIN_TIMESTAMP} property, converted with
     * {@link LocalDateTimeConverter}, and defaulting to {@code 1970-01-01T00:00:00}.
     *
     * @return the minimum valid {@link java.time.LocalDateTime}
     */
    @Key("SINK_MAXCOMPUTE_VALID_MIN_TIMESTAMP")
    @ConverterClass(LocalDateTimeConverter.class)
    @DefaultValue("1970-01-01T00:00:00")
    LocalDateTime getValidMinTimestamp();

    /**
     * Returns the latest timestamp accepted for ingestion.
     *
     * <p>Records with an event time after this bound are treated as invalid. Bound to the
     * {@code SINK_MAXCOMPUTE_VALID_MAX_TIMESTAMP} property, converted with
     * {@link LocalDateTimeConverter}, and defaulting to {@code 9999-12-31T23:59:59}.
     *
     * @return the maximum valid {@link java.time.LocalDateTime}
     */
    @Key("SINK_MAXCOMPUTE_VALID_MAX_TIMESTAMP")
    @ConverterClass(LocalDateTimeConverter.class)
    @DefaultValue("9999-12-31T23:59:59")
    LocalDateTime getValidMaxTimestamp();

    /**
     * Returns the maximum number of years in the past an event time may be from now.
     *
     * <p>Records older than this window are rejected with an
     * {@link com.gotocompany.depot.exception.InvalidMessageException}. Bound to the
     * {@code SINK_MAXCOMPUTE_MAX_PAST_EVENT_TIME_DIFFERENCE_YEAR} property; defaults to {@code 5}.
     *
     * @return the maximum allowed past event-time difference in years
     */
    @Key("SINK_MAXCOMPUTE_MAX_PAST_EVENT_TIME_DIFFERENCE_YEAR")
    @DefaultValue("5")
    int getMaxPastYearEventTimeDifference();

    /**
     * Returns the maximum number of years in the future an event time may be from now.
     *
     * <p>Records further ahead than this window are rejected with an
     * {@link com.gotocompany.depot.exception.InvalidMessageException}. Bound to the
     * {@code SINK_MAXCOMPUTE_MAX_FUTURE_EVENT_TIME_DIFFERENCE_YEAR} property; defaults to {@code 1}.
     *
     * @return the maximum allowed future event-time difference in years
     */
    @Key("SINK_MAXCOMPUTE_MAX_FUTURE_EVENT_TIME_DIFFERENCE_YEAR")
    @DefaultValue("1")
    int getMaxFutureYearEventTimeDifference();

    /**
     * Indicates whether Protobuf integer types are mapped to the MaxCompute {@code BIGINT} type.
     *
     * <p>When {@code true}, integer-valued Protobuf fields are written as {@code BIGINT} instead of
     * narrower integer types. Bound to the
     * {@code SINK_MAXCOMPUTE_PROTO_INTEGER_TYPES_TO_BIGINT_ENABLED} property; defaults to
     * {@code false}.
     *
     * @return {@code true} if integer types map to {@code BIGINT}, {@code false} otherwise
     */
    @Key("SINK_MAXCOMPUTE_PROTO_INTEGER_TYPES_TO_BIGINT_ENABLED")
    @DefaultValue("false")
    boolean isProtoIntegerTypesToBigintEnabled();

    /**
     * Indicates whether Protobuf {@code float} fields are mapped to the MaxCompute {@code DECIMAL}
     * type.
     *
     * <p>When {@code true}, float values are stored as decimals with the precision and scale given by
     * {@link #getProtoFloatToDecimalPrecision()} and {@link #getProtoFloatToDecimalScale()}. Bound to
     * the {@code SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DECIMAL_ENABLED} property; defaults to
     * {@code false}.
     *
     * @return {@code true} if {@code float} maps to {@code DECIMAL}, {@code false} otherwise
     */
    @Key("SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DECIMAL_ENABLED")
    @DefaultValue("false")
    boolean isProtoFloatTypeToDecimalEnabled();

    /**
     * Indicates whether Protobuf {@code float} fields are mapped to the MaxCompute {@code DOUBLE}
     * type.
     *
     * <p>When {@code true}, float values are widened to {@code DOUBLE}. Bound to the
     * {@code SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DOUBLE_ENABLED} property; defaults to {@code false}.
     *
     * @return {@code true} if {@code float} maps to {@code DOUBLE}, {@code false} otherwise
     */
    @Key("SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DOUBLE_ENABLED")
    @DefaultValue("false")
    boolean isProtoFloatTypeToDoubleEnabled();

    /**
     * Returns the decimal precision used when mapping Protobuf {@code float} fields to {@code DECIMAL}.
     *
     * <p>Applies when {@link #isProtoFloatTypeToDecimalEnabled()} is {@code true}. Bound to the
     * {@code SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DECIMAL_PRECISION} property; defaults to {@code 38}.
     *
     * @return the decimal precision for float-to-decimal mapping
     */
    @Key("SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DECIMAL_PRECISION")
    @DefaultValue("38")
    int getProtoFloatToDecimalPrecision();

    /**
     * Returns the decimal scale used when mapping Protobuf {@code float} fields to {@code DECIMAL}.
     *
     * <p>Applies when {@link #isProtoFloatTypeToDecimalEnabled()} is {@code true}. Bound to the
     * {@code SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DECIMAL_SCALE} property; defaults to {@code 18}.
     *
     * @return the decimal scale for float-to-decimal mapping
     */
    @Key("SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DECIMAL_SCALE")
    @DefaultValue("18")
    int getProtoFloatToDecimalScale();

    /**
     * Indicates whether Protobuf {@code double} fields are mapped to the MaxCompute {@code DECIMAL}
     * type.
     *
     * <p>When {@code true}, double values are stored as decimals with the precision and scale given by
     * {@link #getProtoDoubleToDecimalPrecision()} and {@link #getProtoDoubleToDecimalScale()}. Bound
     * to the {@code SINK_MAXCOMPUTE_PROTO_DOUBLE_TYPE_TO_DECIMAL_ENABLED} property; defaults to
     * {@code false}.
     *
     * @return {@code true} if {@code double} maps to {@code DECIMAL}, {@code false} otherwise
     */
    @Key("SINK_MAXCOMPUTE_PROTO_DOUBLE_TYPE_TO_DECIMAL_ENABLED")
    @DefaultValue("false")
    boolean isProtoDoubleToDecimalEnabled();

    /**
     * Returns the decimal precision used when mapping Protobuf {@code double} fields to
     * {@code DECIMAL}.
     *
     * <p>Applies when {@link #isProtoDoubleToDecimalEnabled()} is {@code true}. Bound to the
     * {@code SINK_MAXCOMPUTE_PROTO_DOUBLE_TYPE_TO_DECIMAL_PRECISION} property; defaults to {@code 38}.
     *
     * @return the decimal precision for double-to-decimal mapping
     */
    @Key("SINK_MAXCOMPUTE_PROTO_DOUBLE_TYPE_TO_DECIMAL_PRECISION")
    @DefaultValue("38")
    int getProtoDoubleToDecimalPrecision();

    /**
     * Returns the decimal scale used when mapping Protobuf {@code double} fields to {@code DECIMAL}.
     *
     * <p>Applies when {@link #isProtoDoubleToDecimalEnabled()} is {@code true}. Bound to the
     * {@code SINK_MAXCOMPUTE_PROTO_DOUBLE_TYPE_TO_DECIMAL_SCALE} property; defaults to {@code 18}.
     *
     * @return the decimal scale for double-to-decimal mapping
     */
    @Key("SINK_MAXCOMPUTE_PROTO_DOUBLE_TYPE_TO_DECIMAL_SCALE")
    @DefaultValue("18")
    int getProtoDoubleToDecimalScale();

    /**
     * Returns the rounding mode applied when coercing values into decimal columns.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_DECIMAL_ROUNDING_MODE} property and defaulting to
     * {@code UNNECESSARY}, which means a value requiring rounding triggers an error rather than being
     * rounded. The value is a {@link java.math.RoundingMode}.
     *
     * @return the configured decimal rounding mode
     */
    @Key("SINK_MAXCOMPUTE_DECIMAL_ROUNDING_MODE")
    @DefaultValue("UNNECESSARY")
    RoundingMode getDecimalRoundingMode();

    /**
     * Returns additional table properties applied when creating the MaxCompute table.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_TABLE_PROPERTIES} property and parsed by
     * {@link KeyValuePairsToMapConverter} from comma-separated {@code key=value} pairs into a
     * {@code Map<String, String>}; it defaults to an empty string, yielding no extra properties.
     *
     * @return the table properties keyed by property name
     */
    @Key("SINK_MAXCOMPUTE_TABLE_PROPERTIES")
    @ConverterClass(KeyValuePairsToMapConverter.class)
    @DefaultValue("")
    Map<String, String> getTableProperties();

    /**
     * Indicates whether nanosecond precision in timestamps is handled explicitly.
     *
     * <p>When {@code true}, sub-microsecond components of timestamps are processed according to the
     * configured truncation mode rather than dropped. Bound to the
     * {@code SINK_MAXCOMPUTE_NANO_HANDLING_ENABLED} property; defaults to {@code true}.
     *
     * @return {@code true} if nanosecond handling is enabled, {@code false} otherwise
     */
    @Key("SINK_MAXCOMPUTE_NANO_HANDLING_ENABLED")
    @DefaultValue("true")
    boolean isNanoHandlingEnabled();

    /**
     * Returns the maximum depth to which nested Protobuf messages are expanded into columns.
     *
     * <p>Nested messages deeper than this limit are not expanded further, bounding schema size and
     * recursion. Bound to the {@code SINK_MAXCOMPUTE_PROTO_MAX_NESTED_MESSAGE_DEPTH} property; defaults
     * to {@code 15}.
     *
     * @return the maximum nested message depth
     */
    @Key("SINK_MAXCOMPUTE_PROTO_MAX_NESTED_MESSAGE_DEPTH")
    @DefaultValue("15")
    int getMaxNestedMessageDepth();

    /**
     * Indicates whether timestamps with a negative seconds component are ignored.
     *
     * <p>When {@code true}, timestamps that resolve to a negative epoch-seconds value are skipped
     * rather than rejected. Bound to the
     * {@code SINK_MAXCOMPUTE_IGNORE_NEGATIVE_SECOND_TIMESTAMP_ENABLED} property; defaults to
     * {@code true}.
     *
     * @return {@code true} if negative-second timestamps are ignored, {@code false} otherwise
     */
    @Key("SINK_MAXCOMPUTE_IGNORE_NEGATIVE_SECOND_TIMESTAMP_ENABLED")
    @DefaultValue("true")
    boolean isIgnoreNegativeSecondTimestampEnabled();

    /**
     * Indicates whether records whose schema does not match the table are tolerated.
     *
     * <p>When {@code false}, a schema mismatch is treated as an error; when {@code true}, mismatched
     * records are permitted. Bound to the {@code SINK_MAXCOMPUTE_ALLOW_SCHEMA_MISMATCH_ENABLED}
     * property; defaults to {@code false}.
     *
     * @return {@code true} if schema mismatches are allowed, {@code false} otherwise
     */
    @Key("SINK_MAXCOMPUTE_ALLOW_SCHEMA_MISMATCH_ENABLED")
    @DefaultValue("false")
    boolean isAllowSchemaMismatchEnabled();

    /**
     * Returns the unit to which timestamps are truncated before being written.
     *
     * <p>Bound to the {@code SINK_MAXCOMPUTE_TIMESTAMP_TRUNCATE_MODE} property and defaulting to
     * {@code MICROS}, meaning timestamps are truncated to microsecond precision. The value is a
     * {@link java.time.temporal.ChronoUnit}.
     *
     * @return the configured timestamp truncation unit
     */
    @Key("SINK_MAXCOMPUTE_TIMESTAMP_TRUNCATE_MODE")
    @DefaultValue("MICROS")
    ChronoUnit getTimestampTruncateMode();
}
