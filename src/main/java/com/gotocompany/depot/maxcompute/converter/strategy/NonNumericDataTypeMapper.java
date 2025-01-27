package com.gotocompany.depot.maxcompute.converter.strategy;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;

import java.util.Map;
import java.util.function.Function;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.BOOL;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.BYTES;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.ENUM;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.STRING;

public class NonNumericDataTypeMapper implements ProtoPrimitiveDataTypeMapper {

    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(BYTES, TypeInfoFactory.BINARY)
                .put(STRING, TypeInfoFactory.STRING)
                .put(ENUM, TypeInfoFactory.STRING)
                .put(BOOL, TypeInfoFactory.BOOLEAN)
                .build();
    }

    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(BYTES, object -> handleBytes((ByteString) object))
                .put(STRING, Function.identity())
                .put(ENUM, Object::toString)
                .put(BOOL, Function.identity())
                .build();
    }

    private static Binary handleBytes(ByteString object) {
        return new Binary(object.toByteArray());
    }

}
