package com.gotocompany.depot.maxcompute.converter.mapper;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;

import java.util.Map;
import java.util.function.Function;

public interface ProtoPrimitiveDataTypeMapper {

    Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap();

    Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap();

}
