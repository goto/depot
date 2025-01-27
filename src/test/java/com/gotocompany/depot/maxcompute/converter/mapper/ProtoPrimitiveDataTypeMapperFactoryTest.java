package com.gotocompany.depot.maxcompute.converter.mapper;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.math.RoundingMode;
import java.util.Map;

public class ProtoPrimitiveDataTypeMapperFactoryTest {
    @Test
    public void shouldReturnDefaultImplementation() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled()).thenReturn(false);
        Mockito.when(maxComputeSinkConfig.isProtoFloatTypeToDecimalEnabled()).thenReturn(false);
        Mockito.when(maxComputeSinkConfig.isProtoDoubleToDecimalEnabled()).thenReturn(false);
        ProtoPrimitiveDataTypeMapper protoPrimitiveDataTypeMapper = ProtoPrimitiveDataTypeMapperFactory.createPrimitiveProtobufMappingStrategy(maxComputeSinkConfig);
        Map<Descriptors.FieldDescriptor.Type, TypeInfo> protoTypeToOdpsTypeMap = protoPrimitiveDataTypeMapper.getProtoTypeMap();

        Assertions.assertEquals(TypeInfoFactory.INT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.INT32));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.INT64));
        Assertions.assertEquals(TypeInfoFactory.STRING, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.STRING));
        Assertions.assertEquals(TypeInfoFactory.DOUBLE, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.DOUBLE));
        Assertions.assertEquals(TypeInfoFactory.FLOAT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.FLOAT));
        Assertions.assertEquals(TypeInfoFactory.BINARY, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.BYTES));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.UINT64));
        Assertions.assertEquals(TypeInfoFactory.INT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.UINT32));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.FIXED64));
        Assertions.assertEquals(TypeInfoFactory.INT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.FIXED32));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.SFIXED64));
        Assertions.assertEquals(TypeInfoFactory.INT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.SFIXED32));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.SINT64));
        Assertions.assertEquals(TypeInfoFactory.INT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.SINT32));
    }

    @Test
    public void shouldReturnCustomImplementation() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled()).thenReturn(true);
        Mockito.when(maxComputeSinkConfig.isProtoFloatTypeToDecimalEnabled()).thenReturn(true);
        int precision = 38;
        int scale = 18;
        Mockito.when(maxComputeSinkConfig.getProtoFloatToDecimalPrecision()).thenReturn(precision);
        Mockito.when(maxComputeSinkConfig.getProtoFloatToDecimalScale()).thenReturn(scale);
        Mockito.when(maxComputeSinkConfig.isProtoDoubleToDecimalEnabled()).thenReturn(true);
        Mockito.when(maxComputeSinkConfig.getProtoDoubleToDecimalPrecision()).thenReturn(precision);
        Mockito.when(maxComputeSinkConfig.getProtoDoubleToDecimalScale()).thenReturn(scale);
        Mockito.when(maxComputeSinkConfig.getDecimalRoundingMode()).thenReturn(RoundingMode.UNNECESSARY);

        ProtoPrimitiveDataTypeMapper protoPrimitiveDataTypeMapper = ProtoPrimitiveDataTypeMapperFactory.createPrimitiveProtobufMappingStrategy(maxComputeSinkConfig);
        Map<Descriptors.FieldDescriptor.Type, TypeInfo> protoTypeToOdpsTypeMap = protoPrimitiveDataTypeMapper.getProtoTypeMap();

        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.INT32));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.INT64));
        Assertions.assertEquals(TypeInfoFactory.STRING, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.STRING));
        Assertions.assertEquals(TypeInfoFactory.getDecimalTypeInfo(precision, scale), protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.DOUBLE));
        Assertions.assertEquals(TypeInfoFactory.getDecimalTypeInfo(precision, scale), protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.FLOAT));
        Assertions.assertEquals(TypeInfoFactory.BINARY, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.BYTES));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.UINT64));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.UINT32));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.FIXED64));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.FIXED32));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.SFIXED64));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.SFIXED32));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.SINT64));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.SINT32));
    }
}
