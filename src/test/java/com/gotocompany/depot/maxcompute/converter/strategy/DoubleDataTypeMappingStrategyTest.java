package com.gotocompany.depot.maxcompute.converter.strategy;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DoubleDataTypeMappingStrategyTest {

    private final DoubleDataTypeMapper doublePrimitiveProtobufMappingStrategy = new DoubleDataTypeMapper();

    @Test
    public void shouldMapProtoDoubleToOdpsDouble() {
        Descriptors.FieldDescriptor.Type protoType = Descriptors.FieldDescriptor.Type.DOUBLE;

        TypeInfo typeInfo = doublePrimitiveProtobufMappingStrategy.getProtoTypeMap().get(protoType);

        assertEquals(OdpsType.DOUBLE, typeInfo.getOdpsType());
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenDoubleIsPositiveInfinity() {
        double value = Double.POSITIVE_INFINITY;

        doublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenDoubleIsNegativeInfinity() {
        double value = Double.NEGATIVE_INFINITY;

        doublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenDoubleIsNaN() {
        double value = Double.NaN;

        doublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);
    }

    @Test
    public void shouldMapFloatValue() {
        double value = 123.123123f;

        double mappedValue = (double) doublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);

        assertEquals(value, mappedValue);
    }
}
