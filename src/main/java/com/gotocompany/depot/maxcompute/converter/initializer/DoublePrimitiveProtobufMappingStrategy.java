package com.gotocompany.depot.maxcompute.converter.initializer;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;

import java.util.Map;
import java.util.function.Function;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.DOUBLE;

public class DoublePrimitiveProtobufMappingStrategy implements PrimitiveProtobufMappingStrategy {

    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(Descriptors.FieldDescriptor.Type.DOUBLE, TypeInfoFactory.DOUBLE)
                .build();
    }

    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(DOUBLE, object -> handleDouble((double) object))
                .build();
    }

    private static double handleDouble(double value) {
        if (!Double.isFinite(value)) {
            throw new InvalidMessageException("Invalid float value: " + value);
        }
        return value;
    }

}
