package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;

public interface PayloadConverter {
    Object convert(Descriptors.FieldDescriptor fieldDescriptor, Object object);
    boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor);
}
