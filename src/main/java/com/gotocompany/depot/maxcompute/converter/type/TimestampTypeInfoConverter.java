package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

public class TimestampTypeInfoConverter implements TypeInfoConverter {

    @Override
    public TypeInfo convert(Descriptors.FieldDescriptor fieldDescriptor) {
        return wrap(fieldDescriptor, TypeInfoFactory.TIMESTAMP_NTZ);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return Descriptors.FieldDescriptor.Type.MESSAGE.equals(fieldDescriptor.getType()) &&
                fieldDescriptor.getMessageType().getFullName().equals("google.protobuf.Timestamp");
    }

}
