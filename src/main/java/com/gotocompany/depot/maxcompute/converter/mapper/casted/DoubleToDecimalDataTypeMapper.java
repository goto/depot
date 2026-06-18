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

/**
 * {@link ProtoPrimitiveDataTypeMapper} that maps the Protobuf {@code DOUBLE} type to a MaxCompute {@code DECIMAL}.
 *
 * <p>Selected when double-to-decimal conversion is enabled. The target {@code DECIMAL} precision and scale, and
 * the rounding mode applied during conversion, are taken from configuration. Each value is validated for
 * finiteness and converted to a {@link BigDecimal} bounded by the configured precision and set to the configured
 * scale.</p>
 *
 * @see ProtoPrimitiveDataTypeMapper
 */
public class DoubleToDecimalDataTypeMapper implements ProtoPrimitiveDataTypeMapper {

    /**
     * Total number of significant digits in the target {@code DECIMAL} type.
     */
    private final int precision;
    /**
     * Number of digits to the right of the decimal point in the target {@code DECIMAL} type.
     */
    private final int scale;
    /**
     * Rounding mode applied when a value does not fit the configured scale.
     */
    private final RoundingMode roundingMode;

    /**
     * Creates the mapper using the configured decimal precision, scale, and rounding mode.
     *
     * @param maxComputeSinkConfig the sink configuration providing the double-to-decimal precision, scale, and
     *                             rounding mode
     */
    public DoubleToDecimalDataTypeMapper(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.precision = maxComputeSinkConfig.getProtoDoubleToDecimalPrecision();
        this.scale = maxComputeSinkConfig.getProtoDoubleToDecimalScale();
        this.roundingMode = maxComputeSinkConfig.getDecimalRoundingMode();
    }

    /**
     * Returns the MaxCompute type mapping for the double type.
     *
     * @return a map from {@code DOUBLE} to a MaxCompute {@code DECIMAL} type with the configured precision and scale
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(Descriptors.FieldDescriptor.Type.DOUBLE, TypeInfoFactory.getDecimalTypeInfo(precision, scale))
                .build();
    }

    /**
     * Returns the value-conversion mapping for the double type.
     *
     * @return a map from {@code DOUBLE} to a converter that validates the double and converts it to a {@link BigDecimal}
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(Descriptors.FieldDescriptor.Type.DOUBLE, value -> isValid((double) value))
                .build();
    }

    /**
     * Validates that a double value is finite and converts it to a scaled {@link BigDecimal}.
     *
     * @param value the double value to validate and convert
     * @return the value as a {@link BigDecimal} bounded by the configured precision and set to the configured scale
     * @throws InvalidMessageException if the value is {@code NaN} or infinite
     */
    private BigDecimal isValid(double value) {
        if (!Double.isFinite(value)) {
            throw new InvalidMessageException("Invalid double value: " + value);
        }
        return new BigDecimal(String.valueOf(value), new MathContext(precision)).setScale(scale, roundingMode);
    }
}
