package com.gotocompany.depot.maxcompute.converter.strategy;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class DecimalCastedDoublePrimitiveProtobufMappingStrategyTest {

    private DecimalCastedDoublePrimitiveProtobufMappingStrategy decimalCastedDoublePrimitiveProtobufMappingStrategy;

    @Before
    public void setUp() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getDoubleDecimalFormPrecision()).thenReturn(38);
        Mockito.when(maxComputeSinkConfig.getDoubleDecimalFormScale()).thenReturn(18);
        decimalCastedDoublePrimitiveProtobufMappingStrategy = new DecimalCastedDoublePrimitiveProtobufMappingStrategy(maxComputeSinkConfig);
    }


    @Test
    public void shouldMapProtoDoubleToOdpsDecimal() {
        Descriptors.FieldDescriptor.Type protoType = Descriptors.FieldDescriptor.Type.DOUBLE;

        TypeInfo typeInfo = decimalCastedDoublePrimitiveProtobufMappingStrategy.getProtoTypeMap().get(protoType);

        assertEquals(OdpsType.DECIMAL, typeInfo.getOdpsType());
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenDoubleIsPositiveInfinity() {
        double value = Double.POSITIVE_INFINITY;

        decimalCastedDoublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenDoubleIsNegativeInfinity() {
        double value = Double.NEGATIVE_INFINITY;

        decimalCastedDoublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageWhenDoubleIsNaN() {
        double value = Double.NaN;

        decimalCastedDoublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.DOUBLE).apply(value);
    }

    @Test
    public void shouldMapFloatValue() {
        double value = 123.123123f;

        BigDecimal mappedValue = (BigDecimal) decimalCastedDoublePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap()
                .get(Descriptors.FieldDescriptor.Type.DOUBLE)
                .apply(value);

        assertEquals(value, mappedValue.doubleValue(), 0);
    }

}
