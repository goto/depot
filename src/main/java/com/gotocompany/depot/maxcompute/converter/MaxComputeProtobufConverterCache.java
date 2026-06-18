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

/**
 * Registry and cache that maps Protobuf field types to their {@link ProtobufMaxComputeConverter} and memoizes
 * the derived MaxCompute {@link TypeInfo} objects.
 *
 * <p>On construction it wires one converter per supported field category:</p>
 * <ul>
 *     <li>a single {@link PrimitiveProtobufMaxComputeConverter} shared by every supported primitive type;</li>
 *     <li>a timestamp converter selected by configuration ({@link TimestampNTZProtobufMaxComputeConverter} or
 *     {@link TimestampProtobufMaxComputeConverter}) for {@code google.protobuf.Timestamp};</li>
 *     <li>a {@link DurationProtobufMaxComputeConverter} for {@code google.protobuf.Duration};</li>
 *     <li>a {@link StructProtobufMaxComputeConverter} for {@code google.protobuf.Struct};</li>
 *     <li>a {@link MessageProtobufMaxComputeConverter} for every other nested message.</li>
 * </ul>
 *
 * <p>Derived type information is cached under a key that combines the nesting level with the field's fully
 * qualified name, allowing a recursive schema to be resolved once and reused. Both the converter map and the
 * type cache are backed by {@link ConcurrentHashMap}, so lookups are safe for concurrent use; the type cache
 * can be reset through {@link #clearCache()} when the schema changes.</p>
 *
 * @see ProtobufMaxComputeConverter
 * @see ProtobufConverterOrchestrator
 */
public class MaxComputeProtobufConverterCache {

    /**
     * Fully qualified name of the well-known {@code google.protobuf.Timestamp} message type.
     */
    private static final String GOOGLE_PROTOBUF_TIMESTAMP = "google.protobuf.Timestamp";
    /**
     * Fully qualified name of the well-known {@code google.protobuf.Duration} message type.
     */
    private static final String GOOGLE_PROTOBUF_DURATION = "google.protobuf.Duration";
    /**
     * Fully qualified name of the well-known {@code google.protobuf.Struct} message type.
     */
    private static final String GOOGLE_PROTOBUF_STRUCT = "google.protobuf.Struct";
    /**
     * Set of Protobuf scalar field types routed to the shared {@link PrimitiveProtobufMaxComputeConverter}.
     */
    private static final Set<Descriptors.FieldDescriptor.Type> SUPPORTED_PRIMITIVE_PROTO_TYPES = Sets.newHashSet(
            BYTES, STRING, ENUM, DOUBLE, FLOAT, BOOL,
            INT64, INT32,
            UINT64, UINT32,
            FIXED64, FIXED32,
            SFIXED64, SFIXED32,
            SINT64, SINT32);

    /**
     * Maps a field-type key (a primitive type name, a well-known message name, or {@code MESSAGE}) to its converter.
     */
    private final Map<String, ProtobufMaxComputeConverter> protobufMaxComputeConverterMap;
    /**
     * Memoized MaxCompute type information keyed by nesting level and fully qualified field name.
     */
    private final Map<String, TypeInfo> typeInfoCache;

    /**
     * Builds the converter registry from configuration.
     *
     * <p>Registers the shared primitive converter for every supported primitive type, selects the timestamp
     * converter according to the configured timestamp data type (TIMESTAMP or TIMESTAMP_NTZ), and registers the
     * duration, struct, and generic message converters.</p>
     *
     * @param maxComputeSinkConfig the sink configuration that drives converter selection
     */
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

    /**
     * Returns the cached MaxCompute type for a field, computing and caching it on first request.
     *
     * <p>On a cache miss the converter for the field descriptor is resolved through
     * {@link #getConverter(Descriptors.FieldDescriptor)} and asked to derive the type, which is then stored under
     * the composite level-and-name cache key.</p>
     *
     * @param protoPayload the payload wrapper carrying the field descriptor and nesting level
     * @return the cached or newly computed MaxCompute {@link TypeInfo}
     */
    public TypeInfo getOrCreateTypeInfo(ProtoPayload protoPayload) {
        TypeInfo typeInfo = typeInfoCache.get(getTypeInfoCacheKey(protoPayload));
        if (isNull(typeInfo)) {
            ProtobufMaxComputeConverter protobufMaxComputeConverter = getConverter(protoPayload.getFieldDescriptor());
            typeInfo = protobufMaxComputeConverter.convertTypeInfo(protoPayload);
            typeInfoCache.put(getTypeInfoCacheKey(protoPayload), typeInfo);
        }
        return typeInfo;
    }

    /**
     * Returns the cached MaxCompute type for a field, computing it from the supplier on a cache miss.
     *
     * <p>This overload lets the caller (notably {@link MessageProtobufMaxComputeConverter}) provide the type
     * computation explicitly, which is needed to break the recursion when building nested struct types.</p>
     *
     * @param protoPayload the payload wrapper carrying the field descriptor and nesting level
     * @param supplier     the supplier invoked to compute the type when it is not already cached
     * @return the cached or newly computed MaxCompute {@link TypeInfo}
     */
    public TypeInfo getOrCreateTypeInfo(ProtoPayload protoPayload, Supplier<TypeInfo> supplier) {
        TypeInfo typeInfo = typeInfoCache.get(getTypeInfoCacheKey(protoPayload));
        if (isNull(typeInfo)) {
            typeInfo = supplier.get();
            typeInfoCache.put(getTypeInfoCacheKey(protoPayload), typeInfo);
        }
        return typeInfo;
    }

    /**
     * Resolves the converter responsible for the given Protobuf field.
     *
     * <p>For message fields the well-known timestamp, duration, and struct types are matched by their fully
     * qualified name and routed to their dedicated converters; any other message is handled by the generic
     * message converter. For non-message fields the converter is looked up by the field's primitive type.</p>
     *
     * @param fieldDescriptor the descriptor of the field whose converter is required
     * @return the {@link ProtobufMaxComputeConverter} able to handle the field
     * @throws IllegalArgumentException if the field's primitive type has no registered converter
     */
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

    /**
     * Builds the cache key that identifies a field's type within the schema tree.
     *
     * <p>The key combines the nesting level with the field's fully qualified name, so the same field appearing at
     * different depths is cached separately.</p>
     *
     * @param protoPayload the payload wrapper carrying the field descriptor and nesting level
     * @return the composite cache key
     */
    private String getTypeInfoCacheKey(ProtoPayload protoPayload) {
        return String.format("%d_%s", protoPayload.getLevel(), protoPayload.getFieldDescriptor().getFullName());
    }

    /**
     * Clears the memoized type information so that types are recomputed on the next request.
     *
     * <p>The registered converter instances are left intact.</p>
     */
    public void clearCache() {
        typeInfoCache.clear();
    }

}
