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
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MetadataUtil {

    private static final String TIMESTAMP = "timestamp";
    private static final String INTEGER = "integer";
    private static final String LONG = "long";
    private static final String FLOAT = "float";
    private static final String DOUBLE = "double";
    private static final String STRING = "string";
    private static final String BOOLEAN = "boolean";

    private final Map<String, TypeInfo> metadataTypeMap;
    private final Map<String, Function<Object, Object>> metadataMapperMap;
    private final MaxComputeTimestampDataType maxComputeTimestampDataType;
    private final ZoneId zoneId;

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

    public TypeInfo getMetadataTypeInfo(String type) {
        return metadataTypeMap.get(type.toLowerCase());
    }

    public Object getValidMetadataValue(String type, Object value) {
        if (Objects.isNull(value) || Objects.isNull(type)) {
            return null;
        }
        if (TIMESTAMP.equalsIgnoreCase(type) && value instanceof Long) {
            return getTimestampValue((long) value);
        }
        return Optional.ofNullable(metadataMapperMap.get(type.toLowerCase()))
                .map(mapper -> mapper.apply(value))
                .orElse(null);
    }

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
