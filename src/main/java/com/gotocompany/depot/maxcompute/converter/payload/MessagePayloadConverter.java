package com.gotocompany.depot.maxcompute.converter.payload;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.maxcompute.converter.type.MessageTypeInfoConverter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class MessagePayloadConverter implements PayloadConverter {

    private final MessageTypeInfoConverter messageTypeInfoConverter;
    private final List<PayloadConverter> payloadConverters;

    @Override
    public Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        DynamicMessage dynamicMessage = (DynamicMessage) object;
        List<Object> values = new ArrayList<>();
        dynamicMessage.getAllFields().forEach((innerFieldDescriptor, value) -> {
            Object mappedInnerValue = payloadConverters.stream()
                    .filter(converter -> converter.canConvert(innerFieldDescriptor))
                    .findFirst()
                    .map(converter -> converter.convert(innerFieldDescriptor, value))
                    .orElse(null);
            values.add(mappedInnerValue);
        });
        return new SimpleStruct((StructTypeInfo) messageTypeInfoConverter.convert(fieldDescriptor), values);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return messageTypeInfoConverter.canConvert(fieldDescriptor);
    }

}
