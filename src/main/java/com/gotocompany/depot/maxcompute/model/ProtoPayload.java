package com.gotocompany.depot.maxcompute.model;

import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class ProtoPayload {
    private final Descriptors.FieldDescriptor fieldDescriptor;
    private final Object parsedObject;
    private final boolean isRootLevel;
    private final int level;

    public ProtoPayload(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
        this.parsedObject = null;
        this.isRootLevel = true;
        this.level = 0;
    }
}
