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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts google.protobuf.Message to MaxCompute Struct.
 */
@Setter
public class MessageProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private final MaxComputeProtobufConverterCache maxComputeProtobufConverterCache;
    private final boolean defaultValueEnabled;

    public MessageProtobufMaxComputeConverter(MaxComputeProtobufConverterCache maxComputeProtobufConverterCache,
                                              MaxComputeSinkConfig maxComputeSinkConfig) {
        this.maxComputeProtobufConverterCache = maxComputeProtobufConverterCache;
        this.defaultValueEnabled = maxComputeSinkConfig.isProtoDefaultValueEnabled();
    }

    @Override
    public TypeInfo convertTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        return maxComputeProtobufConverterCache.getOrCreateTypeInfo(fieldDescriptor,
                () -> ProtobufMaxComputeConverter.super.convertTypeInfo(fieldDescriptor));
    }

    @Override
    public StructTypeInfo convertSingularTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        List<String> fieldNames = fieldDescriptor.getMessageType().getFields().stream()
                .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toList());
        List<TypeInfo> typeInfos = fieldDescriptor.getMessageType().getFields().stream()
                .map(fd -> {
                    ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fd);
                    return converter.convertTypeInfo(fd);
                })
                .collect(Collectors.toList());
        return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        Message dynamicMessage = (Message) protoPayload.getParsedObject();
        List<Object> values = new ArrayList<>();
        Map<Descriptors.FieldDescriptor, Object> payloadFields = dynamicMessage.getAllFields();
        protoPayload.getFieldDescriptor().getMessageType().getFields().forEach(innerFieldDescriptor -> {
            Object mappedInnerValue;
            if (!payloadFields.containsKey(innerFieldDescriptor)) {
                if (innerFieldDescriptor.isRepeated()) {
                    values.add(Collections.emptyList());
                    return;
                }
                mappedInnerValue = defaultValueEnabled ? maxComputeProtobufConverterCache.getConverter(innerFieldDescriptor)
                        .convertPayload(new ProtoPayload(innerFieldDescriptor, getDefaultValue(innerFieldDescriptor), false)) : null;
            } else {
                mappedInnerValue = maxComputeProtobufConverterCache.getConverter(innerFieldDescriptor)
                        .convertPayload(new ProtoPayload(innerFieldDescriptor, payloadFields.get(innerFieldDescriptor), false));
            }
            values.add(mappedInnerValue);
        });
        TypeInfo typeInfo = convertTypeInfo(protoPayload.getFieldDescriptor());
        StructTypeInfo structTypeInfo = (StructTypeInfo) (typeInfo instanceof ArrayTypeInfo ? ((ArrayTypeInfo) typeInfo).getElementTypeInfo() : typeInfo);
        return new SimpleStruct(structTypeInfo, values);
    }

    private Object getDefaultValue(Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return fieldDescriptor.toProto().getDefaultInstanceForType();
        }
        return fieldDescriptor.getDefaultValue();
    }
}
