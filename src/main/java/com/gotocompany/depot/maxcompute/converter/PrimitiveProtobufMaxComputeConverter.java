package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.strategy.ProtoPrimitiveDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.strategy.ProtoPrimitiveDataTypeMappingStrategyFactory;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

/**
 * Handle the conversion of primitive protobuf types to MaxCompute compatible format.
 */
public class PrimitiveProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private final ProtoPrimitiveDataTypeMapper protoPrimitiveDataTypeMapper;

    public PrimitiveProtobufMaxComputeConverter(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.protoPrimitiveDataTypeMapper = ProtoPrimitiveDataTypeMappingStrategyFactory.createPrimitiveProtobufMappingStrategy(maxComputeSinkConfig);
    }

    @Override
    public TypeInfo convertSingularTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        return protoPrimitiveDataTypeMapper.getProtoTypeMap().get(fieldDescriptor.getType());
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        return protoPrimitiveDataTypeMapper.getProtoPayloadMapperMap().get(protoPayload.getFieldDescriptor().getType()).apply(protoPayload.getParsedObject());
    }

}
