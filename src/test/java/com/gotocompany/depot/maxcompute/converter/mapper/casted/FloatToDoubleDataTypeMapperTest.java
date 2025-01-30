package com.gotocompany.depot.maxcompute.converter.mapper.casted;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FloatToDoubleDataTypeMapperTest {

    private final FloatToDoubleDataTypeMapper floatToDoubleDataTypeMapper = new FloatToDoubleDataTypeMapper();

    @Test
    public void shouldReturnDoubleMaxComputeType() {
        TypeInfo typeInfo = floatToDoubleDataTypeMapper.getProtoTypeMap()
                .get(Descriptors.FieldDescriptor.Type.FLOAT);

        assertEquals(TypeInfoFactory.DOUBLE, typeInfo);
    }

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

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenFloatValueIsPositiveInfinity() {
        float input = Float.POSITIVE_INFINITY;

        floatToDoubleDataTypeMapper.getProtoPayloadMapperMap()
                .get(Descriptors.FieldDescriptor.Type.FLOAT)
                .apply(input);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenFloatValueIsNegativeInfinity() {
        float input = Float.NEGATIVE_INFINITY;

        floatToDoubleDataTypeMapper.getProtoPayloadMapperMap()
                .get(Descriptors.FieldDescriptor.Type.FLOAT)
                .apply(input);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenFloatValueIsNaN() {
        float input = Float.NaN;

        floatToDoubleDataTypeMapper.getProtoPayloadMapperMap()
                .get(Descriptors.FieldDescriptor.Type.FLOAT)
                .apply(input);
    }
}
