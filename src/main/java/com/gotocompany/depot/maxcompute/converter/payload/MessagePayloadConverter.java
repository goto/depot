package com.gotocompany.depot.maxcompute.converter.payload;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.maxcompute.converter.type.TypeInfoConverter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class MessagePayloadConverter implements PayloadConverter {

    private final List<PayloadConverter> payloadConverters;
    private final List<TypeInfoConverter> typeInfoConverters;

    @Override
    public Object convert(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        List<String> fieldNames = new ArrayList<>();
        List<TypeInfo> typeInfos = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        DynamicMessage dynamicMessage = (DynamicMessage) object;

        dynamicMessage.getAllFields()
                .forEach((innerFieldDescriptor, value) -> {
                    fieldNames.add(innerFieldDescriptor.getName());
                    typeInfos.add(getTypeInfoConverter(innerFieldDescriptor).convert(innerFieldDescriptor));
                    values.add(getPayloadConverter(innerFieldDescriptor).convert(innerFieldDescriptor, value));
                    values.add(value);
                });
        return new SimpleStruct(TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos), values);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE;
    }

    private TypeInfoConverter getTypeInfoConverter(Descriptors.FieldDescriptor fieldDescriptor) {
        return typeInfoConverters.stream()
                .filter(converter -> converter.canConvert(fieldDescriptor))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getJavaType()));
    }

    private PayloadConverter getPayloadConverter(Descriptors.FieldDescriptor fieldDescriptor) {
        return payloadConverters.stream()
                .filter(converter -> converter.canConvert(fieldDescriptor))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getJavaType()));
    }

}