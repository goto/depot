package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.enumeration.MaxComputeTimestampDataType;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.*;
import static java.util.Objects.isNull;

public class MaxComputeProtobufConverterCache {

    private static final String GOOGLE_PROTOBUF_TIMESTAMP = "google.protobuf.Timestamp";
    private static final String GOOGLE_PROTOBUF_DURATION = "google.protobuf.Duration";
    private static final String GOOGLE_PROTOBUF_STRUCT = "google.protobuf.Struct";
    private static final Set<Descriptors.FieldDescriptor.Type> SUPPORTED_PRIMITIVE_PROTO_TYPES = Sets.newHashSet(
            BYTES, STRING, ENUM, DOUBLE, FLOAT, BOOL,
            INT64, INT32,
            UINT64, UINT32,
            FIXED64, FIXED32,
            SFIXED64, SFIXED32,
            SINT64, SINT32);

    private final Map<String, ProtobufMaxComputeConverter> protobufMaxComputeConverterMap;
    private final Map<String, TypeInfo> typeInfoCache;

    public MaxComputeProtobufConverterCache(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.protobufMaxComputeConverterMap = new ConcurrentHashMap<>();
        this.typeInfoCache = new ConcurrentHashMap<>();
        PrimitiveProtobufMaxComputeConverter primitiveProtobufMaxComputeConverter = new PrimitiveProtobufMaxComputeConverter(maxComputeSinkConfig);
        SUPPORTED_PRIMITIVE_PROTO_TYPES.forEach(type -> protobufMaxComputeConverterMap.put(type.toString(), primitiveProtobufMaxComputeConverter));
        if (maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType() == MaxComputeTimestampDataType.TIMESTAMP_NTZ) {
            protobufMaxComputeConverterMap.put(GOOGLE_PROTOBUF_TIMESTAMP, new TimestampNTZProtobufMaxComputeConverter(maxComputeSinkConfig));
        } else {
            protobufMaxComputeConverterMap.put(GOOGLE_PROTOBUF_TIMESTAMP, new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig));
        }
        protobufMaxComputeConverterMap.put(GOOGLE_PROTOBUF_TIMESTAMP, MaxComputeTimestampDataType.TIMESTAMP_NTZ == maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()
                ? new TimestampNTZProtobufMaxComputeConverter(maxComputeSinkConfig) : new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig));
        protobufMaxComputeConverterMap.put(GOOGLE_PROTOBUF_DURATION, new DurationProtobufMaxComputeConverter());
        protobufMaxComputeConverterMap.put(GOOGLE_PROTOBUF_STRUCT, new StructProtobufMaxComputeConverter());
        protobufMaxComputeConverterMap.put(MESSAGE.toString(), new MessageProtobufMaxComputeConverter(this, maxComputeSinkConfig));
    }

    public TypeInfo getOrCreateTypeInfo(ProtoPayload protoPayload) {
        TypeInfo typeInfo = typeInfoCache.get(getTypeInfoCacheKey(protoPayload));
        if (isNull(typeInfo)) {
            ProtobufMaxComputeConverter protobufMaxComputeConverter = getConverter(protoPayload.getFieldDescriptor());
            typeInfo = protobufMaxComputeConverter.convertTypeInfo(protoPayload);
            typeInfoCache.put(getTypeInfoCacheKey(protoPayload), typeInfo);
        }
        return typeInfo;
    }

    public TypeInfo getOrCreateTypeInfo(ProtoPayload protoPayload, Supplier<TypeInfo> supplier) {
        TypeInfo typeInfo = typeInfoCache.get(getTypeInfoCacheKey(protoPayload));
        if (isNull(typeInfo)) {
            typeInfo = supplier.get();
            typeInfoCache.put(getTypeInfoCacheKey(protoPayload), typeInfo);
        }
        return typeInfo;
    }

    public ProtobufMaxComputeConverter getConverter(Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.getType() == MESSAGE) {
            switch (fieldDescriptor.getMessageType().getFullName()) {
                case GOOGLE_PROTOBUF_TIMESTAMP:
                    return protobufMaxComputeConverterMap.get(GOOGLE_PROTOBUF_TIMESTAMP);
                case GOOGLE_PROTOBUF_DURATION:
                    return protobufMaxComputeConverterMap.get(GOOGLE_PROTOBUF_DURATION);
                case GOOGLE_PROTOBUF_STRUCT:
                    return protobufMaxComputeConverterMap.get(GOOGLE_PROTOBUF_STRUCT);
                default:
                    return protobufMaxComputeConverterMap.get(MESSAGE.toString());
            }
        }
        ProtobufMaxComputeConverter protobufMaxComputeConverter = protobufMaxComputeConverterMap.get(fieldDescriptor.getType().toString());
        if (protobufMaxComputeConverter == null) {
            throw new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getType());
        }
        return protobufMaxComputeConverter;
    }

    private String getTypeInfoCacheKey(ProtoPayload protoPayload) {
        return String.format("%d_%s", protoPayload.getLevel(), protoPayload.getFieldDescriptor().getFullName());
    }

    public void clearCache() {
        typeInfoCache.clear();
    }

}
