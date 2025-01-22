package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.initializer.PrimitiveProtobufMappingStrategy;
import com.gotocompany.depot.maxcompute.converter.initializer.PrimitiveProtobufMappingStrategyFactory;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

/**
 * Handle the conversion of primitive protobuf types to MaxCompute compatible format.
 */
public class PrimitiveProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private final PrimitiveProtobufMappingStrategy primitiveProtobufMappingStrategy;

    public PrimitiveProtobufMaxComputeConverter(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.primitiveProtobufMappingStrategy = PrimitiveProtobufMappingStrategyFactory.createPrimitiveProtobufMappingStrategy(maxComputeSinkConfig);
    }

    @Override
    public TypeInfo convertSingularTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        return primitiveProtobufMappingStrategy.getProtoTypeMap().get(fieldDescriptor.getType());
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        return primitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(protoPayload.getFieldDescriptor().getType()).apply(protoPayload.getParsedObject());
    }

}
