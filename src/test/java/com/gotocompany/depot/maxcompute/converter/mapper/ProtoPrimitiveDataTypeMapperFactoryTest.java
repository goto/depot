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

/**
 * Unit tests for {@link ProtoPrimitiveDataTypeMapperFactory}, which assembles the primitive
 * Protobuf-to-MaxCompute type mapping by selecting and merging the configured mapping strategies.
 *
 * <p>The factory inspects feature flags on {@link MaxComputeSinkConfig} to decide whether integers are widened
 * to {@code BIGINT}, and whether floats and doubles are mapped to their natural MaxCompute types or cast to
 * {@code DECIMAL} (or, for floats, to {@code DOUBLE}). Each test builds the factory from a Mockito-mocked
 * configuration representing one such combination and asserts that the merged
 * {@code Map<Descriptors.FieldDescriptor.Type, TypeInfo>} returned by
 * {@link ProtoPrimitiveDataTypeMapperFactory#getProtoTypeMap()} maps every primitive Protobuf type to the
 * expected MaxCompute type.</p>
 */
public class ProtoPrimitiveDataTypeMapperFactoryTest {
    /**
     * Verifies the default mapping produced when all casting feature flags are disabled.
     *
     * <p>Builds the factory from a configuration where integer-to-bigint, float-to-decimal, and
     * double-to-decimal conversions are all turned off, then asserts the merged type map applies the default
     * policy: the 32-bit integer families map to {@code INT}, the 64-bit integer families to {@code BIGINT},
     * {@code DOUBLE} to {@code DOUBLE}, {@code FLOAT} to {@code FLOAT}, {@code BYTES} to {@code BINARY}, and
     * {@code STRING} to {@code STRING}.</p>
     */
    @Test
    public void shouldReturnDefaultImplementation() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled()).thenReturn(false);
        Mockito.when(maxComputeSinkConfig.isProtoFloatTypeToDecimalEnabled()).thenReturn(false);
        Mockito.when(maxComputeSinkConfig.isProtoDoubleToDecimalEnabled()).thenReturn(false);
        ProtoPrimitiveDataTypeMapperFactory protoPrimitiveDataTypeMapperFactory = new ProtoPrimitiveDataTypeMapperFactory(maxComputeSinkConfig);

        Map<Descriptors.FieldDescriptor.Type, TypeInfo> protoTypeToOdpsTypeMap = protoPrimitiveDataTypeMapperFactory.getProtoTypeMap();

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

    /**
     * Verifies the mapping produced when integer-widening and decimal casting are enabled.
     *
     * <p>Builds the factory from a configuration with integer-to-bigint, float-to-decimal, and
     * double-to-decimal conversions enabled (precision {@code 38}, scale {@code 18}). Asserts that every
     * integer family resolves to {@code BIGINT}, and that both {@code DOUBLE} and {@code FLOAT} resolve to a
     * {@code DECIMAL} type with the configured precision and scale, while {@code STRING} and {@code BYTES}
     * remain unchanged.</p>
     */
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
        ProtoPrimitiveDataTypeMapperFactory protoPrimitiveDataTypeMapperFactory = new ProtoPrimitiveDataTypeMapperFactory(maxComputeSinkConfig);

        Map<Descriptors.FieldDescriptor.Type, TypeInfo> protoTypeToOdpsTypeMap = protoPrimitiveDataTypeMapperFactory.getProtoTypeMap();

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

    /**
     * Verifies the mapping produced when integer-widening, float-to-double, and double-to-decimal casting are
     * enabled.
     *
     * <p>Builds the factory from a configuration that promotes integers to {@code BIGINT}, floats to
     * {@code DOUBLE}, and doubles to {@code DECIMAL} (precision {@code 38}, scale {@code 18}). Asserts that
     * every integer family resolves to {@code BIGINT}, {@code DOUBLE} resolves to the configured
     * {@code DECIMAL} type, and {@code FLOAT} resolves to {@code DOUBLE} rather than {@code DECIMAL}.</p>
     */
    @Test
    public void shouldReturnCustomImplementationWithFloatToDouble() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled()).thenReturn(true);
        int precision = 38;
        int scale = 18;
        Mockito.when(maxComputeSinkConfig.isProtoFloatTypeToDoubleEnabled()).thenReturn(true);
        Mockito.when(maxComputeSinkConfig.isProtoDoubleToDecimalEnabled()).thenReturn(true);
        Mockito.when(maxComputeSinkConfig.getProtoDoubleToDecimalPrecision()).thenReturn(precision);
        Mockito.when(maxComputeSinkConfig.getProtoDoubleToDecimalScale()).thenReturn(scale);
        Mockito.when(maxComputeSinkConfig.getDecimalRoundingMode()).thenReturn(RoundingMode.UNNECESSARY);
        ProtoPrimitiveDataTypeMapperFactory protoPrimitiveDataTypeMapperFactory = new ProtoPrimitiveDataTypeMapperFactory(maxComputeSinkConfig);

        Map<Descriptors.FieldDescriptor.Type, TypeInfo> protoTypeToOdpsTypeMap = protoPrimitiveDataTypeMapperFactory.getProtoTypeMap();

        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.INT32));
        Assertions.assertEquals(TypeInfoFactory.BIGINT, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.INT64));
        Assertions.assertEquals(TypeInfoFactory.STRING, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.STRING));
        Assertions.assertEquals(TypeInfoFactory.getDecimalTypeInfo(precision, scale), protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.DOUBLE));
        Assertions.assertEquals(TypeInfoFactory.DOUBLE, protoTypeToOdpsTypeMap.get(Descriptors.FieldDescriptor.Type.FLOAT));
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
