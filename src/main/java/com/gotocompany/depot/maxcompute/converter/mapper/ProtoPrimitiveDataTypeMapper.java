package com.gotocompany.depot.maxcompute.converter.mapper;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public interface ProtoPrimitiveDataTypeMapper {
    Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap();

    Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap();

    default ProtoPrimitiveDataTypeMapper mergeStrategy(ProtoPrimitiveDataTypeMapper strategy) {
        return new ProtoPrimitiveDataTypeMapper() {
            @Override
            public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
                Map<Descriptors.FieldDescriptor.Type, TypeInfo> finalMap = new HashMap<>(ProtoPrimitiveDataTypeMapper.this.getProtoTypeMap());
                finalMap.putAll(strategy.getProtoTypeMap());
                return finalMap;
            }

            @Override
            public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
                Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> finalMap = new HashMap<>(ProtoPrimitiveDataTypeMapper.this.getProtoPayloadMapperMap());
                finalMap.putAll(strategy.getProtoPayloadMapperMap());
                return finalMap;
            }
        };
    }

}
