package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.maxcompute.converter.type.MessageTypeInfoConverter;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class MessagePayloadConverter implements PayloadConverter {

    private final MessageTypeInfoConverter messageTypeInfoConverter;
    private final List<PayloadConverter> payloadConverters;

    @Override
    public Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        return payloadConverters.stream()
                .filter(converter -> converter.canConvert(fieldDescriptor))
                .findFirst()
                .map(converter -> converter.convertSingular(fieldDescriptor, object))
                .orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getJavaType()));
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return messageTypeInfoConverter.canConvert(fieldDescriptor);
    }

}
