package com.gotocompany.depot.maxcompute.model;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import lombok.Getter;

@Getter
public class ProtoPayload {
    private final Descriptors.FieldDescriptor fieldDescriptor;
    private final Object parsedObject;
    private final int level;
    private final TypeInfo maxComputeTypeInfo;

    public ProtoPayload(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
        this.parsedObject = null;
        this.level = 0;
        this.maxComputeTypeInfo = null;
    }

    public ProtoPayload(Descriptors.FieldDescriptor fieldDescriptor, int level) {
        this.fieldDescriptor = fieldDescriptor;
        this.parsedObject = null;
        this.level = level;
        this.maxComputeTypeInfo = null;
    }

    public ProtoPayload(Descriptors.FieldDescriptor fieldDescriptor, Object parsedObject, int level, TypeInfo maxComputeTypeInfo) {
        this.fieldDescriptor = fieldDescriptor;
        this.parsedObject = parsedObject;
        this.level = level;
        this.maxComputeTypeInfo = maxComputeTypeInfo;
    }

    public boolean isRootLevel() {
        return level == 0;
    }
}
