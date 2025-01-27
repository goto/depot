package com.gotocompany.depot.maxcompute.converter.strategy;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Map;
import java.util.function.Function;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FLOAT;

public class FloatToDecimalDataTypeMapper implements ProtoPrimitiveDataTypeMapper {

    private final int scale;
    private final int precision;
    private final RoundingMode roundingMode;

    public FloatToDecimalDataTypeMapper(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.scale = maxComputeSinkConfig.getProtoFloatToDecimalScale();
        this.precision = maxComputeSinkConfig.getProtoFloatToDecimalPrecision();
        this.roundingMode = maxComputeSinkConfig.getDecimalRoundingMode();
    }

    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(FLOAT, TypeInfoFactory.getDecimalTypeInfo(precision, scale))
                .build();
    }

    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(FLOAT, object -> isValid((float) object))
                .build();
    }

    private BigDecimal isValid(float value) {
        if (!Float.isFinite(value)) {
            throw new InvalidMessageException("Invalid float value: " + value);
        }
        return new BigDecimal(Float.toString(value), new MathContext(precision))
                .setScale(scale, roundingMode);
    }

}
