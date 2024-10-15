package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.sql.Timestamp;

public class TimestampPayloadConverter implements PayloadConverter {

    private static final String SECONDS = "seconds";
    private static final String NANOS = "nanos";
    private static final String PROTO_TIMESTAMP_QUALIFIED_NAME = "google.protobuf.Timestamp";

    @Override
    public Object convert(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        DynamicMessage dynamicMessage = (DynamicMessage) object;
        long seconds = (long) dynamicMessage.getField(dynamicMessage.getDescriptorForType().findFieldByName(SECONDS));
        int nanos = (int) dynamicMessage.getField(dynamicMessage.getDescriptorForType().findFieldByName(NANOS));
        return new Timestamp(seconds * 1000 + nanos / 1000000);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE) &&
                fieldDescriptor.getMessageType().getFullName().equals(PROTO_TIMESTAMP_QUALIFIED_NAME);
    }

}
