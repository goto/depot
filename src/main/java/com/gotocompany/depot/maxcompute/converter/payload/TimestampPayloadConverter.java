package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.maxcompute.converter.type.TimestampTypeInfoConverter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TimestampPayloadConverter implements PayloadConverter {

    public static final int SECOND_TO_MILLIS_MULTIPLIER = 1000;
    private final TimestampTypeInfoConverter timestampTypeInfoConverter;

    @Override
    public Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        Timestamp timestamp = (Timestamp) object;
        java.sql.Timestamp convertedTimestamp = new java.sql.Timestamp(timestamp.getSeconds() * SECOND_TO_MILLIS_MULTIPLIER);
        convertedTimestamp.setNanos(timestamp.getNanos());
        return convertedTimestamp;
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return timestampTypeInfoConverter.canConvert(fieldDescriptor);
    }

}
