package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

public class StructPayloadConverter implements PayloadConverter {

    private final JsonFormat.Printer printer = JsonFormat.printer()
            .preservingProtoFieldNames()
            .omittingInsignificantWhitespace();

    @Override
    public Object convert(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        try {
            return printer.print((DynamicMessage) object);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                && fieldDescriptor.getMessageType().getFullName().equals("google.protobuf.Struct");
    }

}
