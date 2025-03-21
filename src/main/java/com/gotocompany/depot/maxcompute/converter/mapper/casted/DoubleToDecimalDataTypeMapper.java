package com.gotocompany.depot.maxcompute.converter.mapper.casted;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.converter.mapper.ProtoPrimitiveDataTypeMapper;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Map;
import java.util.function.Function;

public class DoubleToDecimalDataTypeMapper implements ProtoPrimitiveDataTypeMapper {

    private final int precision;
    private final int scale;
    private final RoundingMode roundingMode;

    public DoubleToDecimalDataTypeMapper(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.precision = maxComputeSinkConfig.getProtoDoubleToDecimalPrecision();
        this.scale = maxComputeSinkConfig.getProtoDoubleToDecimalScale();
        this.roundingMode = maxComputeSinkConfig.getDecimalRoundingMode();
    }

    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(Descriptors.FieldDescriptor.Type.DOUBLE, TypeInfoFactory.getDecimalTypeInfo(precision, scale))
                .build();
    }

    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(Descriptors.FieldDescriptor.Type.DOUBLE, value -> isValid((double) value))
                .build();
    }

    private BigDecimal isValid(double value) {
        if (!Double.isFinite(value)) {
            throw new InvalidMessageException("Invalid double value: " + value);
        }
        return new BigDecimal(String.valueOf(value), new MathContext(precision)).setScale(scale, roundingMode);
    }
}
