package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

/**
 * Orchestrates the conversion of Protobuf fields to MaxCompute record fields.
 * It uses a cache to store the converters for each field descriptor.
 */
public class ProtobufConverterOrchestrator {

    private final MaxComputeProtobufConverterCache maxComputeProtobufConverterCache;

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
