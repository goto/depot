package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

/**
 * Orchestrates the conversion of Protobuf fields to MaxCompute record fields.
 * It uses a cache to store the converters for each field descriptor.
 *
 * <p>It is a thin facade over a {@link MaxComputeProtobufConverterCache}: the cache selects the right
 * {@link ProtobufMaxComputeConverter} for each field descriptor and memoizes the resulting {@link TypeInfo}
 * objects. Callers in the schema and record layers use this class to map field descriptors to MaxCompute
 * types and to map field values to MaxCompute values, without needing to know which concrete converter
 * applies.</p>
 *
 * @see MaxComputeProtobufConverterCache
 * @see ProtobufMaxComputeConverter
 */
public class ProtobufConverterOrchestrator {

    /**
     * Cache that resolves and memoizes the per-field converters and their derived MaxCompute types.
     */
    private final MaxComputeProtobufConverterCache maxComputeProtobufConverterCache;

    /**
     * Creates an orchestrator backed by a fresh converter cache built from the given configuration.
     *
     * @param maxComputeSinkConfig the sink configuration that determines how primitive, timestamp, and other
     *                             field types are mapped
     */
    public ProtobufConverterOrchestrator(MaxComputeSinkConfig maxComputeSinkConfig) {
        maxComputeProtobufConverterCache = new MaxComputeProtobufConverterCache(maxComputeSinkConfig);
    }

    /**
     * Converts a Protobuf field to a MaxCompute TypeInfo.
     *
     * @param protoPayload the proto payload wrapper for Protobuf field descriptor
     * @return the MaxCompute TypeInfo
     */
    public TypeInfo toMaxComputeTypeInfo(ProtoPayload protoPayload) {
        return maxComputeProtobufConverterCache.getOrCreateTypeInfo(protoPayload);
    }

    /**
     * Converts a Protobuf field to a MaxCompute record field.
     *
     * @param protoPayload the proto payload wrapper for Protobuf field descriptor
     * @return the MaxCompute record field
     */
    public Object toMaxComputeValue(ProtoPayload protoPayload) {
        ProtobufMaxComputeConverter protobufMaxComputeConverter = maxComputeProtobufConverterCache.getConverter(protoPayload.getFieldDescriptor());
        return protobufMaxComputeConverter.convertPayload(protoPayload);
    }

    /**
     * Clears the cache. This method should be called when the schema changes.
     */
    public void clearCache() {
        maxComputeProtobufConverterCache.clearCache();
    }

}
