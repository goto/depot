package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

public class StructTypeInfoConverter implements TypeInfoConverter {

    @Override
    public TypeInfo convert(Descriptors.FieldDescriptor fieldDescriptor) {
        return wrap(fieldDescriptor, TypeInfoFactory.STRING);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                && fieldDescriptor.getMessageType().getFullName().equals("google.protobuf.Struct");
    }

}
