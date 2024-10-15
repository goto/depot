package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PrimitivePayloadConverter implements PayloadConverter {

    @Override
    public Object convert(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        return object;
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType() != Descriptors.FieldDescriptor.Type.MESSAGE;
    }

}
