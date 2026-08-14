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

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FLOAT;

/**
 * {@link ProtoPrimitiveDataTypeMapper} that maps the Protobuf {@code FLOAT} type to a MaxCompute {@code DECIMAL}.
 *
 * <p>Selected when float-to-decimal conversion is enabled. The target {@code DECIMAL} precision and scale, and
 * the rounding mode applied during conversion, are taken from configuration. Each value is validated for
 * finiteness and converted to a {@link BigDecimal} bounded by the configured precision and set to the configured
 * scale.</p>
 *
 * @see ProtoPrimitiveDataTypeMapper
 */
public class FloatToDecimalDataTypeMapper implements ProtoPrimitiveDataTypeMapper {

    /**
     * Number of digits to the right of the decimal point in the target {@code DECIMAL} type.
     */
    private final int scale;
    /**
     * Total number of significant digits in the target {@code DECIMAL} type.
     */
    private final int precision;
    /**
     * Rounding mode applied when a value does not fit the configured scale.
     */
    private final RoundingMode roundingMode;

    /**
     * Creates the mapper using the configured decimal precision, scale, and rounding mode.
     *
     * @param maxComputeSinkConfig the sink configuration providing the float-to-decimal precision, scale, and
     *                             rounding mode
     */
    public FloatToDecimalDataTypeMapper(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.scale = maxComputeSinkConfig.getProtoFloatToDecimalScale();
        this.precision = maxComputeSinkConfig.getProtoFloatToDecimalPrecision();
        this.roundingMode = maxComputeSinkConfig.getDecimalRoundingMode();
    }

    /**
     * Returns the MaxCompute type mapping for the float type.
     *
     * @return a map from {@code FLOAT} to a MaxCompute {@code DECIMAL} type with the configured precision and scale
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(FLOAT, TypeInfoFactory.getDecimalTypeInfo(precision, scale))
                .build();
    }

    /**
     * Returns the value-conversion mapping for the float type.
     *
     * @return a map from {@code FLOAT} to a converter that validates the float and converts it to a {@link BigDecimal}
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(FLOAT, object -> isValid((float) object))
                .build();
    }

    /**
     * Validates that a float value is finite and converts it to a scaled {@link BigDecimal}.
     *
     * @param value the float value to validate and convert
     * @return the value as a {@link BigDecimal} bounded by the configured precision and set to the configured scale
     * @throws InvalidMessageException if the value is {@code NaN} or infinite
     */
    private BigDecimal isValid(float value) {
        if (!Float.isFinite(value)) {
            throw new InvalidMessageException("Invalid float value: " + value);
        }
        return new BigDecimal(Float.toString(value), new MathContext(precision))
                .setScale(scale, roundingMode);
    }

}
