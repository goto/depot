package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.mapper.ProtoPrimitiveDataTypeMapperFactory;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.util.Map;
import java.util.function.Function;

/**
 * Handle the conversion of primitive protobuf types to MaxCompute compatible format.
 */
public class PrimitiveProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private final Map<Descriptors.FieldDescriptor.Type, TypeInfo> typeInfoMap;
    private final Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> payloadMapperMap;

    public PrimitiveProtobufMaxComputeConverter(MaxComputeSinkConfig maxComputeSinkConfig) {
        ProtoPrimitiveDataTypeMapperFactory protoPrimitiveDataTypeMapperFactory = new ProtoPrimitiveDataTypeMapperFactory(maxComputeSinkConfig);
        this.typeInfoMap = protoPrimitiveDataTypeMapperFactory.getProtoTypeMap();
        this.payloadMapperMap = protoPrimitiveDataTypeMapperFactory.getProtoPayloadMapperMap();
    }

    @Override
    public TypeInfo convertSingularTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        return this.typeInfoMap.get(fieldDescriptor.getType());
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        return this.payloadMapperMap.get(protoPayload.getFieldDescriptor().getType()).apply(protoPayload.getParsedObject());
    }

}
