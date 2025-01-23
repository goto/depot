package com.gotocompany.depot.maxcompute.converter.strategy;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;

import java.util.Map;
import java.util.function.Function;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FLOAT;


public class FloatDataTypeMappingStrategy implements ProtoPrimitiveDataTypeMappingStrategy {

    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(FLOAT, TypeInfoFactory.FLOAT)
                .build();
    }

    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(FLOAT, object -> handleFloat((float) object))
                .build();
    }

    private static float handleFloat(float value) {
        if (!Float.isFinite(value)) {
            throw new InvalidMessageException("Invalid float value: " + value);
        }
        return value;
    }

}
