package com.gotocompany.depot.maxcompute.converter.mapper;

import com.aliyun.odps.type.TypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.mapper.casted.DoubleToDecimalDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.casted.FloatToDecimalDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.casted.IntegerToBigintDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.noncasted.DoubleDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.noncasted.FloatDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.noncasted.IntegerDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.noncasted.NonNumericDataTypeMapper;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

@RequiredArgsConstructor
public class ProtoPrimitiveDataTypeMapperFactory {

    private final ProtoPrimitiveDataTypeMapper nonNumericDataTypeMapper;
    private final ProtoPrimitiveDataTypeMapper integerProtoPrimitiveDataTypeMapper;
    private final ProtoPrimitiveDataTypeMapper floatProtoPrimitiveDataTypeMapper;
    private final ProtoPrimitiveDataTypeMapper doubleProtoPrimitiveDataTypeMapper;

    public ProtoPrimitiveDataTypeMapperFactory(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.nonNumericDataTypeMapper = new NonNumericDataTypeMapper();
        this.integerProtoPrimitiveDataTypeMapper = maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled() ? new IntegerToBigintDataTypeMapper() : new IntegerDataTypeMapper();
        this.floatProtoPrimitiveDataTypeMapper = maxComputeSinkConfig.isProtoFloatTypeToDecimalEnabled() ? new FloatToDecimalDataTypeMapper(maxComputeSinkConfig) : new FloatDataTypeMapper();
        this.doubleProtoPrimitiveDataTypeMapper = maxComputeSinkConfig.isProtoDoubleToDecimalEnabled() ? new DoubleToDecimalDataTypeMapper(maxComputeSinkConfig) : new DoubleDataTypeMapper();
    }

    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return mergeMaps(ImmutableList.of(
                nonNumericDataTypeMapper.getProtoTypeMap(),
                integerProtoPrimitiveDataTypeMapper.getProtoTypeMap(),
                floatProtoPrimitiveDataTypeMapper.getProtoTypeMap(),
                doubleProtoPrimitiveDataTypeMapper.getProtoTypeMap()
        ));
    }

    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return mergeMaps(ImmutableList.of(
                nonNumericDataTypeMapper.getProtoPayloadMapperMap(),
                integerProtoPrimitiveDataTypeMapper.getProtoPayloadMapperMap(),
                floatProtoPrimitiveDataTypeMapper.getProtoPayloadMapperMap(),
                doubleProtoPrimitiveDataTypeMapper.getProtoPayloadMapperMap()
        ));
    }

    private static <K, V> Map<K, V> mergeMaps(List<Map<K, V>> maps) {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (Map<K, V> map : maps) {
            builder.putAll(map);
        }
        return builder.build();
    }

}
