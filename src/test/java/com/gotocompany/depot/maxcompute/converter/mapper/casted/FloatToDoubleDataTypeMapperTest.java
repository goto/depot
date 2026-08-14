package com.gotocompany.depot.maxcompute.converter.mapper.casted;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link FloatToDoubleDataTypeMapper}, the casting primitive mapping strategy that promotes the
 * Protobuf {@code FLOAT} type to a MaxCompute {@code DOUBLE}.
 *
 * <p>This mapper is selected when the sink is configured to widen single-precision floats to double precision.
 * The suite verifies the type mapping (where {@code FLOAT} resolves to {@code TypeInfoFactory.DOUBLE}) and the
 * value-conversion function, which both widens a finite {@code float} to an equal {@code double} and rejects
 * the non-finite IEEE-754 values ({@code NaN}, positive infinity, and negative infinity) with an
 * {@link InvalidMessageException}.</p>
 *
 * <p>Each test runs against a fresh, real {@link FloatToDoubleDataTypeMapper} instance.</p>
 */
public class FloatToDoubleDataTypeMapperTest {

    /**
     * The mapper under test, exercised through its public type and value lookup tables.
     */
    private final FloatToDoubleDataTypeMapper floatToDoubleDataTypeMapper = new FloatToDoubleDataTypeMapper();

    /**
     * Verifies that the type map resolves the Protobuf {@code FLOAT} type to MaxCompute {@code DOUBLE}.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.FLOAT} in
     * {@link FloatToDoubleDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.DOUBLE}.</p>
     */
    @Test
    public void shouldReturnDoubleMaxComputeType() {
        TypeInfo typeInfo = floatToDoubleDataTypeMapper.getProtoTypeMap()
                .get(Descriptors.FieldDescriptor.Type.FLOAT);

        assertEquals(TypeInfoFactory.DOUBLE, typeInfo);
    }

    /**
     * Verifies that a finite {@code float} is widened to an equivalent {@code double}.
     *
     * <p>Applies the {@code FLOAT} mapper from {@link FloatToDoubleDataTypeMapper#getProtoPayloadMapperMap()}
     * to {@link Float#MIN_VALUE} and asserts the result is a {@link Double} equal to the value obtained by
     * parsing the float's string representation, ensuring the widening introduces no precision artifacts.</p>
     */
    @Test
    public void shouldReturnValidFloatValue() {
        float input = Float.MIN_VALUE;
        double expected = Double.parseDouble(Float.toString(Float.MIN_VALUE));
        Object result = floatToDoubleDataTypeMapper.getProtoPayloadMapperMap()
                .get(Descriptors.FieldDescriptor.Type.FLOAT)
                .apply(input);

        assertTrue(result instanceof Double);
        assertEquals(expected, (double) result, 0);
    }

    /**
     * Verifies that widening {@link Float#POSITIVE_INFINITY} is rejected.
     *
     * <p>Applies the {@code FLOAT} mapper from {@link FloatToDoubleDataTypeMapper#getProtoPayloadMapperMap()}
     * to positive infinity and expects an {@link InvalidMessageException}, because MaxCompute cannot store
     * non-finite values.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenFloatValueIsPositiveInfinity() {
        float input = Float.POSITIVE_INFINITY;

        floatToDoubleDataTypeMapper.getProtoPayloadMapperMap()
                .get(Descriptors.FieldDescriptor.Type.FLOAT)
                .apply(input);
    }

    /**
     * Verifies that widening {@link Float#NEGATIVE_INFINITY} is rejected.
     *
     * <p>Applies the {@code FLOAT} mapper from {@link FloatToDoubleDataTypeMapper#getProtoPayloadMapperMap()}
     * to negative infinity and expects an {@link InvalidMessageException}, because MaxCompute cannot store
     * non-finite values.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenFloatValueIsNegativeInfinity() {
        float input = Float.NEGATIVE_INFINITY;

        floatToDoubleDataTypeMapper.getProtoPayloadMapperMap()
                .get(Descriptors.FieldDescriptor.Type.FLOAT)
                .apply(input);
    }

    /**
     * Verifies that widening {@link Float#NaN} is rejected.
     *
     * <p>Applies the {@code FLOAT} mapper from {@link FloatToDoubleDataTypeMapper#getProtoPayloadMapperMap()}
     * to {@code NaN} and expects an {@link InvalidMessageException}, because MaxCompute cannot store non-finite
     * values.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenFloatValueIsNaN() {
        float input = Float.NaN;

        floatToDoubleDataTypeMapper.getProtoPayloadMapperMap()
                .get(Descriptors.FieldDescriptor.Type.FLOAT)
                .apply(input);
    }
}
