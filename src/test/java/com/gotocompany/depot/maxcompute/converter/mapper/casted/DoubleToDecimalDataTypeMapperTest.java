package com.gotocompany.depot.maxcompute.converter.mapper.casted;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link DoubleToDecimalDataTypeMapper}, the casting primitive mapping strategy that converts
 * the Protobuf {@code DOUBLE} type to a MaxCompute {@code DECIMAL} with a configurable precision and scale.
 *
 * <p>This mapper is selected when the sink is configured to store doubles as fixed-point decimals. Because the
 * decimal precision, scale, and rounding mode are taken from {@link MaxComputeSinkConfig}, each test runs
 * against a {@link DoubleToDecimalDataTypeMapper} built in {@link #setUp()} from a Mockito-mocked
 * configuration stubbed with a precision of {@code 38}, a scale of {@code 18}, and
 * {@code RoundingMode.UNNECESSARY}.</p>
 *
 * <p>The suite verifies the resolved {@code DECIMAL} type, the conversion of a finite double to an equivalent
 * {@link java.math.BigDecimal}, and the rejection of the non-finite IEEE-754 values ({@code NaN}, positive
 * infinity, and negative infinity) with an {@link InvalidMessageException}.</p>
 */
public class DoubleToDecimalDataTypeMapperTest {

    /**
     * The mapper under test, rebuilt in {@link #setUp()} from the mocked configuration.
     */
    private DoubleToDecimalDataTypeMapper decimalCastedDoublePrimitiveProtobufMappingStrategy;

    /**
     * Builds the mapper under test from a Mockito-mocked {@link MaxComputeSinkConfig}.
     *
     * <p>Stubs the configuration to report a decimal precision of {@code 38}, a scale of {@code 18}, and a
     * rounding mode of {@code RoundingMode.UNNECESSARY}, then constructs the
     * {@link DoubleToDecimalDataTypeMapper} exercised by every test.</p>
     */
    @Before
    public void setUp() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getProtoDoubleToDecimalPrecision()).thenReturn(38);
        Mockito.when(maxComputeSinkConfig.getProtoDoubleToDecimalScale()).thenReturn(18);
        Mockito.when(maxComputeSinkConfig.getDecimalRoundingMode()).thenReturn(RoundingMode.UNNECESSARY);
        decimalCastedDoublePrimitiveProtobufMappingStrategy = new DoubleToDecimalDataTypeMapper(maxComputeSinkConfig);
    }


    /**
     * Verifies that the type map resolves the Protobuf {@code DOUBLE} type to a MaxCompute {@code DECIMAL}.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.DOUBLE} in
     * {@link DoubleToDecimalDataTypeMapper#getProtoTypeMap()} and asserts the resolved type reports an
     * {@code OdpsType} of {@code DECIMAL}.</p>
     */
    @Test
    public void shouldMapProtoDoubleToOdpsDecimal() {
        Descriptors.FieldDescriptor.Type protoType = Descriptors.FieldDescriptor.Type.DOUBLE;

        TypeInfo typeInfo = decimalCastedDoublePrimitiveProtobufMappingStrategy.getProtoTypeMap().get(protoType);

        assertEquals(OdpsType.DECIMAL, typeInfo.getOdpsType());
    }

    /**
     * Verifies that converting {@link Double#POSITIVE_INFINITY} is rejected.
     *
     * <p>Applies the {@code DOUBLE} mapper from
     * {@link DoubleToDecimalDataTypeMapper#getProtoPayloadMapperMap()} to positive infinity and expects an
     * {@link InvalidMessageException}, because MaxCompute cannot represent non-finite values as a decimal.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenDoubleIsPositiveInfinity() {
        double value = Double.POSITIVE_INFINITY;

        decimalCastedDoublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);
    }

    /**
     * Verifies that converting {@link Double#NEGATIVE_INFINITY} is rejected.
     *
     * <p>Applies the {@code DOUBLE} mapper from
     * {@link DoubleToDecimalDataTypeMapper#getProtoPayloadMapperMap()} to negative infinity and expects an
     * {@link InvalidMessageException}, because MaxCompute cannot represent non-finite values as a decimal.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenDoubleIsNegativeInfinity() {
        double value = Double.NEGATIVE_INFINITY;

        decimalCastedDoublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);
    }

    /**
     * Verifies that converting {@link Double#NaN} is rejected.
     *
     * <p>Applies the {@code DOUBLE} mapper from
     * {@link DoubleToDecimalDataTypeMapper#getProtoPayloadMapperMap()} to {@code NaN} and expects an
     * {@link InvalidMessageException}, because MaxCompute cannot represent non-finite values as a decimal.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenDoubleIsNaN() {
        double value = Double.NaN;

        decimalCastedDoublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);
    }

    /**
     * Verifies that a finite {@code double} value is converted to an equivalent {@link java.math.BigDecimal}.
     *
     * <p>Applies the {@code DOUBLE} mapper from
     * {@link DoubleToDecimalDataTypeMapper#getProtoPayloadMapperMap()} to a finite value and asserts that the
     * resulting {@code BigDecimal}, when narrowed back with {@code doubleValue()}, equals the original
     * input.</p>
     */
    @Test
    public void shouldMapFloatValue() {
        double value = 123.123123f;

        BigDecimal mappedValue = (BigDecimal) decimalCastedDoublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap()
                .get(Descriptors.FieldDescriptor.Type.DOUBLE)
                .apply(value);

        assertEquals(value, mappedValue.doubleValue(), 0);
    }

}
