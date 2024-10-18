package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

public interface TypeInfoConverter {
    TypeInfo convert(Descriptors.FieldDescriptor fieldDescriptor);
    boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor);
    default int getPriority() {
        return 0;
    };
    default TypeInfo wrap(Descriptors.FieldDescriptor fieldDescriptor, TypeInfo typeInfo) {
        return fieldDescriptor.isRepeated() ? TypeInfoFactory.getArrayTypeInfo(typeInfo) : typeInfo;
    }
}
