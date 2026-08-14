package com.gotocompany.depot.maxcompute.converter.mapper.noncasted;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link DoubleDataTypeMapper}, the primitive mapping strategy responsible for the
 * Protobuf {@code DOUBLE} scalar type.
 *
 * <p>The suite verifies both halves of the strategy contract: the type mapping exposed through
 * {@link DoubleDataTypeMapper#getProtoTypeMap()}, which must resolve {@code DOUBLE} to the MaxCompute
 * {@code DOUBLE} type, and the value-conversion functions exposed through
 * {@link DoubleDataTypeMapper#getProtoPayloadMapperMap()}. The value tests focus on the guard that rejects
 * the non-finite IEEE-754 values ({@code NaN}, positive infinity, and negative infinity) by raising
 * {@link InvalidMessageException}, while finite values pass through unchanged.</p>
 *
 * <p>Each test runs against a fresh, real {@link DoubleDataTypeMapper} instance with no mocking.</p>
 */
public class DoubleDataTypeMapperTest {

    /**
     * The mapper under test, exercised through its public type and value lookup tables.
     */
    private final DoubleDataTypeMapper doublePrimitiveProtobufMappingStrategy = new DoubleDataTypeMapper();

    /**
     * Verifies that the type map resolves the Protobuf {@code DOUBLE} type to a MaxCompute type whose
     * {@code OdpsType} is {@code DOUBLE}.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.DOUBLE} in
     * {@link DoubleDataTypeMapper#getProtoTypeMap()} and asserts the resolved {@code TypeInfo} reports an
     * {@code OdpsType} of {@code DOUBLE}.</p>
     */
    @Test
    public void shouldMapProtoDoubleToOdpsDouble() {
        Descriptors.FieldDescriptor.Type protoType = Descriptors.FieldDescriptor.Type.DOUBLE;

        TypeInfo typeInfo = doublePrimitiveProtobufMappingStrategy.getProtoTypeMap().get(protoType);

        assertEquals(OdpsType.DOUBLE, typeInfo.getOdpsType());
    }

    /**
     * Verifies that converting {@link Double#POSITIVE_INFINITY} is rejected.
     *
     * <p>Applies the {@code DOUBLE} value-conversion function from
     * {@link DoubleDataTypeMapper#getProtoPayloadMapperMap()} to positive infinity and expects an
     * {@link InvalidMessageException}, because MaxCompute cannot store non-finite values.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenDoubleIsPositiveInfinity() {
        double value = Double.POSITIVE_INFINITY;

        doublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);
    }

    /**
     * Verifies that converting {@link Double#NEGATIVE_INFINITY} is rejected.
     *
     * <p>Applies the {@code DOUBLE} value-conversion function from
     * {@link DoubleDataTypeMapper#getProtoPayloadMapperMap()} to negative infinity and expects an
     * {@link InvalidMessageException}, because MaxCompute cannot store non-finite values.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenDoubleIsNegativeInfinity() {
        double value = Double.NEGATIVE_INFINITY;

        doublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);
    }

    /**
     * Verifies that converting {@link Double#NaN} is rejected.
     *
     * <p>Applies the {@code DOUBLE} value-conversion function from
     * {@link DoubleDataTypeMapper#getProtoPayloadMapperMap()} to {@code NaN} and expects an
     * {@link InvalidMessageException}, because MaxCompute cannot store non-finite values.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenDoubleIsNaN() {
        double value = Double.NaN;

        doublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);
    }

    /**
     * Verifies that a finite {@code double} value is converted to an equal value.
     *
     * <p>Applies the {@code DOUBLE} value-conversion function to a finite number and asserts the returned
     * {@code double} equals the input, confirming that valid values pass through the non-finite guard
     * unchanged.</p>
     */
    @Test
    public void shouldMapFloatValue() {
        double value = 123.123123f;

        double mappedValue = (double) doublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);

        assertEquals(value, mappedValue);
    }
}
