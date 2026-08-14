package com.gotocompany.depot.maxcompute.converter.mapper.noncasted;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link FloatDataTypeMapper}, the primitive mapping strategy responsible for the Protobuf
 * {@code FLOAT} scalar type.
 *
 * <p>The suite verifies both halves of the strategy contract: the type mapping exposed through
 * {@link FloatDataTypeMapper#getProtoTypeMap()}, which must resolve {@code FLOAT} to the MaxCompute
 * {@code FLOAT} type, and the value-conversion functions exposed through
 * {@link FloatDataTypeMapper#getProtoPayloadMapperMap()}. The value tests focus on the guard that rejects the
 * non-finite IEEE-754 values ({@code NaN}, positive infinity, and negative infinity) by raising
 * {@link InvalidMessageException}, while finite values pass through unchanged.</p>
 *
 * <p>Each test runs against a fresh, real {@link FloatDataTypeMapper} instance with no mocking.</p>
 */
public class FloatDataTypeMapperTest {

    /**
     * The mapper under test, exercised through its public type and value lookup tables.
     */
    private final FloatDataTypeMapper floatPrimitiveProtobufMappingStrategy = new FloatDataTypeMapper();

    /**
     * Verifies that the type map resolves the Protobuf {@code FLOAT} type to a MaxCompute type whose
     * {@code OdpsType} is {@code FLOAT}.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.FLOAT} in
     * {@link FloatDataTypeMapper#getProtoTypeMap()} and asserts the resolved {@code TypeInfo} reports an
     * {@code OdpsType} of {@code FLOAT}.</p>
     */
    @Test
    public void shouldMapProtoFloatToOdpsFloat() {
        Descriptors.FieldDescriptor.Type protoType = Descriptors.FieldDescriptor.Type.FLOAT;

        TypeInfo typeInfo = floatPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(protoType);

        assertEquals(OdpsType.FLOAT, typeInfo.getOdpsType());
    }

    /**
     * Verifies that converting {@link Float#POSITIVE_INFINITY} is rejected.
     *
     * <p>Applies the {@code FLOAT} value-conversion function from
     * {@link FloatDataTypeMapper#getProtoPayloadMapperMap()} to positive infinity and expects an
     * {@link InvalidMessageException}, because MaxCompute cannot store non-finite values.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenFloatIsPositiveInfinity() {
        float value = Float.POSITIVE_INFINITY;

        floatPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FLOAT).apply(value);
    }

    /**
     * Verifies that converting {@link Float#NEGATIVE_INFINITY} is rejected.
     *
     * <p>Applies the {@code FLOAT} value-conversion function from
     * {@link FloatDataTypeMapper#getProtoPayloadMapperMap()} to negative infinity and expects an
     * {@link InvalidMessageException}, because MaxCompute cannot store non-finite values.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenFloatIsNegativeInfinity() {
        float value = Float.NEGATIVE_INFINITY;

        floatPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FLOAT).apply(value);
    }

    /**
     * Verifies that converting {@link Float#NaN} is rejected.
     *
     * <p>Applies the {@code FLOAT} value-conversion function from
     * {@link FloatDataTypeMapper#getProtoPayloadMapperMap()} to {@code NaN} and expects an
     * {@link InvalidMessageException}, because MaxCompute cannot store non-finite values.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenFloatIsNaN() {
        float value = Float.NaN;

        floatPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FLOAT).apply(value);
    }

    /**
     * Verifies that a finite {@code float} value is converted to an equal value.
     *
     * <p>Applies the {@code FLOAT} value-conversion function to a finite number and asserts the returned
     * {@code float} equals the input, confirming that valid values pass through the non-finite guard
     * unchanged.</p>
     */
    @Test
    public void shouldMapFloatValue() {
        float value = 123.123123f;

        float mappedValue = (float) floatPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FLOAT).apply(value);

        assertEquals(value, mappedValue);
    }

}
