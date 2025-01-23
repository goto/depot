package com.gotocompany.depot.maxcompute.converter.strategy;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;

import java.util.Map;
import java.util.function.Function;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.INT32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.INT64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SFIXED32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SFIXED64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT64;

public class IntegerToBigintDataTypeMappingStrategy implements ProtoPrimitiveDataTypeMappingStrategy {

    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(INT64, TypeInfoFactory.BIGINT)
                .put(INT32, TypeInfoFactory.BIGINT)
                .put(UINT64, TypeInfoFactory.BIGINT)
                .put(UINT32, TypeInfoFactory.BIGINT)
                .put(FIXED64, TypeInfoFactory.BIGINT)
                .put(FIXED32, TypeInfoFactory.BIGINT)
                .put(SFIXED64, TypeInfoFactory.BIGINT)
                .put(SFIXED32, TypeInfoFactory.BIGINT)
                .put(SINT64, TypeInfoFactory.BIGINT)
                .put(SINT32, TypeInfoFactory.BIGINT)
                .build();
    }

    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(INT64, Function.identity())
                .put(INT32, this::upcastInteger)
                .put(UINT64, Function.identity())
                .put(UINT32, this::upcastInteger)
                .put(FIXED64, Function.identity())
                .put(FIXED32, this::upcastInteger)
                .put(SFIXED64, Function.identity())
                .put(SFIXED32, this::upcastInteger)
                .put(SINT64, Function.identity())
                .put(SINT32, this::upcastInteger)
                .build();
    }

    private Object upcastInteger(Object object) {
        return ((Integer) object).longValue();
    }
}
