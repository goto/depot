package com.gotocompany.depot.maxcompute.converter.mapper.casted;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Unit tests for {@link FloatToDecimalDataTypeMapper}, the casting primitive mapping strategy that converts
 * the Protobuf {@code FLOAT} type to a MaxCompute {@code DECIMAL} with a configurable precision and scale.
 *
 * <p>This mapper is selected when the sink is configured to store floats as fixed-point decimals. Because the
 * decimal precision, scale, and rounding mode are taken from {@link MaxComputeSinkConfig}, each test runs
 * against a {@link FloatToDecimalDataTypeMapper} built in {@link #setup()} from a Mockito-mocked configuration
 * stubbed with {@link #PRECISION}, {@link #SCALE}, and {@code RoundingMode.UNNECESSARY}.</p>
 *
 * <p>The suite verifies the resolved {@code DECIMAL} type (including its precision and scale), the conversion
 * of a finite float to an equivalent {@link java.math.BigDecimal}, and the rejection of the non-finite
 * IEEE-754 values ({@code NaN}, positive infinity, and negative infinity) with an
 * {@link InvalidMessageException}.</p>
 */
public class FloatToDecimalDataTypeMapperTest {

    /**
     * Decimal precision (the total number of significant digits) stubbed on the mocked configuration.
     */
    private static final int PRECISION = 38;

    /**
     * Decimal scale (the number of fractional digits) stubbed on the mocked configuration.
     */
    private static final int SCALE = 18;

    /**
     * The mapper under test, rebuilt in {@link #setup()} from the mocked configuration.
     */
    private FloatToDecimalDataTypeMapper decimalCastedFloatPrimitiveProtobufMappingStrategy;

    /**
     * Builds the mapper under test from a Mockito-mocked {@link MaxComputeSinkConfig}.
     *
     * <p>Stubs the configuration to report a decimal precision of {@link #PRECISION}, a scale of
     * {@link #SCALE}, and a rounding mode of {@code RoundingMode.UNNECESSARY}, then constructs the
     * {@link FloatToDecimalDataTypeMapper} exercised by every test.</p>
     */
    @Before
    public void setup() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getProtoFloatToDecimalPrecision()).thenReturn(PRECISION);
        Mockito.when(maxComputeSinkConfig.getProtoFloatToDecimalScale()).thenReturn(SCALE);
        Mockito.when(maxComputeSinkConfig.getDecimalRoundingMode()).thenReturn(RoundingMode.UNNECESSARY);
        decimalCastedFloatPrimitiveProtobufMappingStrategy = new FloatToDecimalDataTypeMapper(maxComputeSinkConfig);
    }

    /**
     * Verifies that the type map resolves the Protobuf {@code FLOAT} type to a MaxCompute {@code DECIMAL} with
     * the configured precision and scale.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.FLOAT} in
     * {@link FloatToDecimalDataTypeMapper#getProtoTypeMap()} and asserts the resolved type reports an
     * {@code OdpsType} of {@code DECIMAL} with precision {@link #PRECISION} and scale {@link #SCALE}.</p>
     */
    @Test
    public void shouldMapProtoFloatToOdpsDecimalType() {
        TypeInfo result = decimalCastedFloatPrimitiveProtobufMappingStrategy.getProtoTypeMap()
                .get(Descriptors.FieldDescriptor.Type.FLOAT);

        Assertions.assertEquals(OdpsType.DECIMAL, result.getOdpsType());
        Assertions.assertEquals(PRECISION, ((DecimalTypeInfo) result).getPrecision());
        Assertions.assertEquals(SCALE, ((DecimalTypeInfo) result).getScale());
    }

    /**
     * Verifies that a finite {@code float} is converted to an equivalent {@link java.math.BigDecimal}.
     *
     * <p>Applies the {@code FLOAT} mapper from {@link FloatToDecimalDataTypeMapper#getProtoPayloadMapperMap()}
     * to a finite value and asserts that the resulting {@code BigDecimal}, when narrowed back with
     * {@code floatValue()}, equals the original input.</p>
     */
    @Test
    public void shouldMapProtoFloatValue() {
        float input = 123.456f;

        BigDecimal result = (BigDecimal) decimalCastedFloatPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap()
                .get(Descriptors.FieldDescriptor.Type.FLOAT)
                .apply(input);

        Assertions.assertEquals(input, result.floatValue());
    }

    /**
     * Verifies that converting {@link Float#POSITIVE_INFINITY} is rejected.
     *
     * <p>Applies the {@code FLOAT} mapper from {@link FloatToDecimalDataTypeMapper#getProtoPayloadMapperMap()}
     * to positive infinity and expects an {@link InvalidMessageException}, because MaxCompute cannot represent
     * non-finite values as a decimal.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenFloatIsPositiveInfinity() {
        float value = Float.POSITIVE_INFINITY;

        decimalCastedFloatPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FLOAT).apply(value);
    }

    /**
     * Verifies that converting {@link Float#NEGATIVE_INFINITY} is rejected.
     *
     * <p>Applies the {@code FLOAT} mapper from {@link FloatToDecimalDataTypeMapper#getProtoPayloadMapperMap()}
     * to negative infinity and expects an {@link InvalidMessageException}, because MaxCompute cannot represent
     * non-finite values as a decimal.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenFloatIsNegativeInfinity() {
        float value = Float.NEGATIVE_INFINITY;

        decimalCastedFloatPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FLOAT).apply(value);
    }

    /**
     * Verifies that converting {@link Float#NaN} is rejected.
     *
     * <p>Applies the {@code FLOAT} mapper from {@link FloatToDecimalDataTypeMapper#getProtoPayloadMapperMap()}
     * to {@code NaN} and expects an {@link InvalidMessageException}, because MaxCompute cannot represent
     * non-finite values as a decimal.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenFloatIsNaN() {
        float value = Float.NaN;

        decimalCastedFloatPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FLOAT).apply(value);
    }
}
