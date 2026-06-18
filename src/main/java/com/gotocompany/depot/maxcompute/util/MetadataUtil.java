package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.enumeration.MaxComputeTimestampDataType;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Helper that maps Depot metadata column type names to MaxCompute types and coerces raw metadata
 * values into the matching MaxCompute representations.
 *
 * <p>Depot lets operators declare metadata columns by a logical type name (such as {@code integer},
 * {@code long}, {@code float}, {@code double}, {@code string}, {@code boolean}, or
 * {@code timestamp}). This class resolves each name to a concrete {@link TypeInfo} and provides a
 * matching value mapper that converts an incoming value (for example a boxed {@link Number} or an
 * epoch-millis {@link Long}) into the type MaxCompute expects. Behaviour is influenced by
 * configuration: integer types may be widened to {@code BIGINT}, and timestamps are materialised
 * either as {@code TIMESTAMP} or {@code TIMESTAMP_NTZ} using the configured {@link ZoneId}.</p>
 */
public class MetadataUtil {

    /**
     * Logical type name for timestamp metadata columns.
     */
    private static final String TIMESTAMP = "timestamp";
    /**
     * Logical type name for 32-bit integer metadata columns (optionally widened to a 64-bit integer).
     */
    private static final String INTEGER = "integer";
    /**
     * Logical type name for 64-bit integer metadata columns.
     */
    private static final String LONG = "long";
    /**
     * Logical type name for single-precision floating-point metadata columns.
     */
    private static final String FLOAT = "float";
    /**
     * Logical type name for double-precision floating-point metadata columns.
     */
    private static final String DOUBLE = "double";
    /**
     * Logical type name for string metadata columns.
     */
    private static final String STRING = "string";
    /**
     * Logical type name for boolean metadata columns.
     */
    private static final String BOOLEAN = "boolean";

    /**
     * Immutable map from logical type name to the resolved MaxCompute {@link TypeInfo}.
     */
    private final Map<String, TypeInfo> metadataTypeMap;
    /**
     * Immutable map from logical type name to the function that coerces a raw value into the matching
     * MaxCompute representation. Timestamp values are handled separately and are intentionally absent
     * from this map.
     */
    private final Map<String, Function<Object, Object>> metadataMapperMap;
    /**
     * Configured MaxCompute timestamp data type used to materialise timestamp metadata values.
     */
    private final MaxComputeTimestampDataType maxComputeTimestampDataType;
    /**
     * Time zone applied when converting epoch-milliseconds timestamps into local date-times.
     */
    private final ZoneId zoneId;

    /**
     * Builds a metadata helper whose type and value mappings reflect the supplied configuration.
     *
     * <p>It precomputes two immutable maps keyed by logical type name: one from type name to
     * {@link TypeInfo}, and one from type name to a value-coercion function. The integer mapping
     * depends on the proto-integer-to-bigint flag (mapping to {@code BIGINT} when enabled, otherwise
     * {@code INT}), and the timestamp mapping uses the configured MaxCompute timestamp data type. The
     * configured {@link ZoneId} is retained for timestamp conversion.</p>
     *
     * @param maxComputeSinkConfig the sink configuration providing the timestamp data type, time zone,
     *        and integer-widening flag
     */
    public MetadataUtil(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.maxComputeTimestampDataType = maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType();
        this.zoneId = maxComputeSinkConfig.getZoneId();
        metadataTypeMap = ImmutableMap.<String, TypeInfo>builder()
                .put(INTEGER, maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled() ? TypeInfoFactory.BIGINT : TypeInfoFactory.INT)
                .put(LONG, TypeInfoFactory.BIGINT)
                .put(FLOAT, TypeInfoFactory.FLOAT)
                .put(DOUBLE, TypeInfoFactory.DOUBLE)
                .put(STRING, TypeInfoFactory.STRING)
                .put(BOOLEAN, TypeInfoFactory.BOOLEAN)
                .put(TIMESTAMP, maxComputeTimestampDataType.getTypeInfo())
                .build();
        metadataMapperMap = ImmutableMap.<String, Function<Object, Object>>builder()
                .put(INTEGER, obj -> {
                    if (maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled()) {
                        return ((Number) obj).longValue();
                    }
                    return ((Number) obj).intValue();
                })
                .put(LONG, obj -> ((Number) obj).longValue())
                .put(FLOAT, obj -> ((Number) obj).floatValue())
                .put(DOUBLE, obj -> ((Number) obj).doubleValue())
                .put(STRING, Function.identity())
                .put(BOOLEAN, Function.identity())
                .build();
    }

    /**
     * Resolves the MaxCompute {@link TypeInfo} for a single logical metadata type name.
     *
     * @param type the logical type name (case-insensitive), such as {@code integer} or
     *        {@code timestamp}
     * @return the corresponding {@link TypeInfo}, or {@code null} if the name is not recognised
     */
    public TypeInfo getMetadataTypeInfo(String type) {
        return metadataTypeMap.get(type.toLowerCase());
    }

    /**
     * Coerces a raw metadata value into the representation required by the column's MaxCompute type.
     *
     * <p>When the logical type is {@code timestamp} and the value is a {@link Long} (interpreted as
     * epoch milliseconds), it is converted to a {@link LocalDateTime} or {@link Timestamp} depending
     * on the configured timestamp data type. For all other types the registered value mapper is
     * applied, narrowing or widening numeric values as configured and passing strings and booleans
     * through unchanged.</p>
     *
     * @param type the logical type name (case-insensitive) of the metadata column
     * @param value the raw metadata value to coerce
     * @return the value converted to the MaxCompute representation for the given type
     * @throws NullPointerException if no value mapper is registered for {@code type}
     * @throws ClassCastException if {@code value} is not compatible with the expected type for
     *         {@code type}
     */
    public Object getValidMetadataValue(String type, Object value) {
        if (TIMESTAMP.equalsIgnoreCase(type) && value instanceof Long) {
            return getTimestampValue((long) value);
        }
        return metadataMapperMap.get(type.toLowerCase()).apply(value);
    }

    /**
     * Builds a MaxCompute {@link StructTypeInfo} describing a group of metadata columns.
     *
     * <p>The struct's field names are the first elements of the supplied tuples and the field types
     * are resolved from the second elements via the internal type map. This is used to materialise all
     * metadata as a single namespaced struct column.</p>
     *
     * @param metadataColumnsTypes the ordered metadata column definitions, each a tuple of column name
     *        and logical type name
     * @return a struct type whose fields mirror the supplied metadata column definitions
     */
    public StructTypeInfo getMetadataTypeInfo(List<TupleString> metadataColumnsTypes) {
        return TypeInfoFactory.getStructTypeInfo(metadataColumnsTypes
                        .stream()
                        .map(TupleString::getFirst)
                        .collect(Collectors.toList()),
                metadataColumnsTypes
                        .stream()
                        .map(tuple -> metadataTypeMap.get(tuple.getSecond().toLowerCase()))
                        .collect(Collectors.toList()));
    }

    /**
     * Converts an epoch-milliseconds value into the configured MaxCompute timestamp representation.
     *
     * <p>The millisecond instant is interpreted in the configured {@link ZoneId} to obtain a
     * {@link LocalDateTime}. When the configured type is {@code TIMESTAMP_NTZ} the local date-time is
     * returned as-is; otherwise it is wrapped in a {@link Timestamp}.</p>
     *
     * @param value the timestamp value in epoch milliseconds
     * @return a {@link LocalDateTime} for {@code TIMESTAMP_NTZ}, or a {@link Timestamp} otherwise
     */
    private Object getTimestampValue(long value) {
        LocalDateTime localDateTime = Instant.ofEpochMilli(value)
                .atZone(zoneId)
                .toLocalDateTime();
        if (MaxComputeTimestampDataType.TIMESTAMP_NTZ == maxComputeTimestampDataType) {
            return localDateTime;
        }
        return Timestamp.valueOf(localDateTime);
    }
}
