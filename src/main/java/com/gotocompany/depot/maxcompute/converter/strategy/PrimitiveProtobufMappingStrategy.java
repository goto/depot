package com.gotocompany.depot.maxcompute.converter.strategy;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public interface PrimitiveProtobufMappingStrategy {
    Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap();

    Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap();

    default PrimitiveProtobufMappingStrategy mergeStrategy(PrimitiveProtobufMappingStrategy strategy) {
        return new PrimitiveProtobufMappingStrategy() {
            @Override
            public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
                Map<Descriptors.FieldDescriptor.Type, TypeInfo> finalMap = new HashMap<>(PrimitiveProtobufMappingStrategy.this.getProtoTypeMap());
                finalMap.putAll(strategy.getProtoTypeMap());
                return finalMap;
            }

            @Override
            public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
                Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> finalMap = new HashMap<>(PrimitiveProtobufMappingStrategy.this.getProtoPayloadMapperMap());
                finalMap.putAll(strategy.getProtoPayloadMapperMap());
                return finalMap;
            }
        };
    }

}
