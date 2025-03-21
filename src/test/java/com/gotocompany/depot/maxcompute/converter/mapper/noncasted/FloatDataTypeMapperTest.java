package com.gotocompany.depot.maxcompute.converter.mapper.noncasted;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FloatDataTypeMapperTest {

    private final FloatDataTypeMapper floatPrimitiveProtobufMappingStrategy = new FloatDataTypeMapper();

    @Test
    public void shouldMapProtoFloatToOdpsFloat() {
        Descriptors.FieldDescriptor.Type protoType = Descriptors.FieldDescriptor.Type.FLOAT;

        TypeInfo typeInfo = floatPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(protoType);

        assertEquals(OdpsType.FLOAT, typeInfo.getOdpsType());
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenFloatIsPositiveInfinity() {
        float value = Float.POSITIVE_INFINITY;

        floatPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FLOAT).apply(value);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenFloatIsNegativeInfinity() {
        float value = Float.NEGATIVE_INFINITY;

        floatPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FLOAT).apply(value);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenFloatIsNaN() {
        float value = Float.NaN;

        floatPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FLOAT).apply(value);
    }

    @Test
    public void shouldMapFloatValue() {
        float value = 123.123123f;

        float mappedValue = (float) floatPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FLOAT).apply(value);

        assertEquals(value, mappedValue);
    }

}
