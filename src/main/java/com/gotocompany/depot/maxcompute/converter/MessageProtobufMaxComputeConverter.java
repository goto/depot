package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts google.protobuf.Message to MaxCompute Struct.
 */
@Setter
public class MessageProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private final MaxComputeProtobufConverterCache maxComputeProtobufConverterCache;
    private final int maxNestedMessageDepth;

    public MessageProtobufMaxComputeConverter(MaxComputeProtobufConverterCache maxComputeProtobufConverterCache,
                                              MaxComputeSinkConfig maxComputeSinkConfig) {
        this.maxComputeProtobufConverterCache = maxComputeProtobufConverterCache;
        if (maxComputeSinkConfig.getMaxNestedMessageDepth() < 1) {
            throw new IllegalArgumentException("Max nested cannot be less than 1");
        }
        this.maxNestedMessageDepth = maxComputeSinkConfig.getMaxNestedMessageDepth() - 1;
    }

    @Override
    public TypeInfo convertTypeInfo(ProtoPayload protoPayload) {
        return maxComputeProtobufConverterCache.getOrCreateTypeInfo(protoPayload, () -> ProtobufMaxComputeConverter.super.convertTypeInfo(protoPayload));
    }

    @Override
    public StructTypeInfo convertSingularTypeInfo(ProtoPayload protoPayload) {
        List<String> fieldNames = protoPayload.getFieldDescriptor().getMessageType().getFields().stream()
                .filter(fd -> protoPayload.getLevel() != maxNestedMessageDepth || fd.getType() != Descriptors.FieldDescriptor.Type.MESSAGE)
                .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toList());
        List<TypeInfo> typeInfos = protoPayload.getFieldDescriptor()
                .getMessageType()
                .getFields()
                .stream()
                .filter(fd -> protoPayload.getLevel() != maxNestedMessageDepth || fd.getType() != Descriptors.FieldDescriptor.Type.MESSAGE)
                .map(fd -> {
                    ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fd);
                    return converter.convertTypeInfo(new ProtoPayload(fd, null, protoPayload.getLevel() + 1));
                })
                .collect(Collectors.toList());
        return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        Message dynamicMessage = (Message) protoPayload.getParsedObject();
        List<Object> values = new ArrayList<>();
        Map<Descriptors.FieldDescriptor, Object> payloadFields = dynamicMessage.getAllFields();
        protoPayload.getFieldDescriptor()
                .getMessageType()
                .getFields()
                .stream()
                .filter(fd -> protoPayload.getLevel() != maxNestedMessageDepth || fd.getType() != Descriptors.FieldDescriptor.Type.MESSAGE)
                .forEach(innerFieldDescriptor -> {
                    if (!payloadFields.containsKey(innerFieldDescriptor)) {
                        values.add(null);
                        return;
                    }
                    Object mappedInnerValue = maxComputeProtobufConverterCache.getConverter(innerFieldDescriptor)
                            .convertPayload(new ProtoPayload(innerFieldDescriptor, payloadFields.get(innerFieldDescriptor), protoPayload.getLevel() + 1));
                    values.add(mappedInnerValue);
                });
        TypeInfo typeInfo = convertTypeInfo(protoPayload);
        StructTypeInfo structTypeInfo = (StructTypeInfo) (typeInfo instanceof ArrayTypeInfo ? ((ArrayTypeInfo) typeInfo).getElementTypeInfo() : typeInfo);
        return new SimpleStruct(structTypeInfo, values);
    }

}
