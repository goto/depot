package com.gotocompany.depot.maxcompute.converter.strategy;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.math.BigDecimal;

public class DecimalCastedFloatPrimitiveProtobufMappingStrategyTest {

    private static final int PRECISION = 38;
    private static final int SCALE = 18;
    private DecimalCastedFloatPrimitiveProtobufMappingStrategy decimalCastedFloatPrimitiveProtobufMappingStrategy;

    @Before
    public void setup() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getFloatDecimalFormPrecision()).thenReturn(PRECISION);
        Mockito.when(maxComputeSinkConfig.getFloatDecimalFormScale()).thenReturn(SCALE);
        decimalCastedFloatPrimitiveProtobufMappingStrategy = new DecimalCastedFloatPrimitiveProtobufMappingStrategy(maxComputeSinkConfig);
    }

    @Test
    public void shouldMapProtoFloatToOdpsDecimalType() {
        TypeInfo result = decimalCastedFloatPrimitiveProtobufMappingStrategy.getProtoTypeMap()
                .get(Descriptors.FieldDescriptor.Type.FLOAT);

        Assertions.assertEquals(OdpsType.DECIMAL, result.getOdpsType());
        Assertions.assertEquals(PRECISION, ((DecimalTypeInfo) result).getPrecision());
        Assertions.assertEquals(SCALE, ((DecimalTypeInfo) result).getScale());
    }

    @Test
    public void shouldMapProtoFloatValue() {
        float input = 123123123.123123f;

        BigDecimal result = (BigDecimal) decimalCastedFloatPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap()
                .get(Descriptors.FieldDescriptor.Type.FLOAT)
                .apply(input);

        Assertions.assertEquals(BigDecimal.valueOf(input), result);
        Assertions.assertEquals(PRECISION, result.precision());

    }
}
