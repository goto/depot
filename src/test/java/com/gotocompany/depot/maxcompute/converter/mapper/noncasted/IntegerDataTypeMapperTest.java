package com.gotocompany.depot.maxcompute.converter.mapper.noncasted;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Unit tests for {@link IntegerDataTypeMapper}, the primitive mapping strategy that handles the integral
 * Protobuf scalar types without widening their MaxCompute storage type.
 *
 * <p>The suite asserts the default (non-upcast) mapping policy:</p>
 * <ul>
 *     <li>the 64-bit families ({@code INT64}, {@code UINT64}, {@code FIXED64}, {@code SFIXED64},
 *     {@code SINT64}) map to MaxCompute {@code BIGINT};</li>
 *     <li>the 32-bit families ({@code INT32}, {@code UINT32}, {@code FIXED32}, {@code SFIXED32},
 *     {@code SINT32}) map to MaxCompute {@code INT}.</li>
 * </ul>
 *
 * <p>It also verifies that every value-conversion function returned by
 * {@link IntegerDataTypeMapper#getProtoPayloadMapperMap()} is an identity mapping that returns the numeric
 * value unchanged. Each test runs against a fresh, real {@link IntegerDataTypeMapper} instance.</p>
 */
public class IntegerDataTypeMapperTest {

    /**
     * The mapper under test, exercised through its public type and value lookup tables.
     */
    private final IntegerDataTypeMapper integerPrimitiveProtobufMappingStrategy = new IntegerDataTypeMapper();

    /**
     * Verifies that the type map resolves the Protobuf {@code INT64} type to the MaxCompute {@code BIGINT}
     * type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.INT64} in
     * {@link IntegerDataTypeMapper#getProtoTypeMap()} and asserts it equals {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoInt64ToOdpsBigInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.INT64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the type map resolves the Protobuf {@code INT32} type to the MaxCompute {@code INT} type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.INT32} in
     * {@link IntegerDataTypeMapper#getProtoTypeMap()} and asserts it equals {@code TypeInfoFactory.INT}.</p>
     */
    @Test
    public void shouldMapProtoInt32ToOdpsInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.INT32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.INT);
    }

    /**
     * Verifies that the type map resolves the Protobuf {@code UINT64} type to the MaxCompute {@code BIGINT}
     * type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.UINT64} in
     * {@link IntegerDataTypeMapper#getProtoTypeMap()} and asserts it equals {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoUint64ToOdpsBigInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.UINT64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the type map resolves the Protobuf {@code UINT32} type to the MaxCompute {@code INT} type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.UINT32} in
     * {@link IntegerDataTypeMapper#getProtoTypeMap()} and asserts it equals {@code TypeInfoFactory.INT}.</p>
     */
    @Test
    public void shouldMapProtoUint32ToOdpsInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.UINT32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.INT);
    }

    /**
     * Verifies that the type map resolves the Protobuf {@code FIXED64} type to the MaxCompute {@code BIGINT}
     * type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.FIXED64} in
     * {@link IntegerDataTypeMapper#getProtoTypeMap()} and asserts it equals {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoFixed64ToOdpsBigInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.FIXED64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the type map resolves the Protobuf {@code FIXED32} type to the MaxCompute {@code INT}
     * type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.FIXED32} in
     * {@link IntegerDataTypeMapper#getProtoTypeMap()} and asserts it equals {@code TypeInfoFactory.INT}.</p>
     */
    @Test
    public void shouldMapProtoFixed32ToOdpsInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.FIXED32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.INT);
    }

    /**
     * Verifies that the type map resolves the Protobuf {@code SFIXED64} type to the MaxCompute {@code BIGINT}
     * type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.SFIXED64} in
     * {@link IntegerDataTypeMapper#getProtoTypeMap()} and asserts it equals {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoSfixed64ToOdpsBigInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SFIXED64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the type map resolves the Protobuf {@code SFIXED32} type to the MaxCompute {@code INT}
     * type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.SFIXED32} in
     * {@link IntegerDataTypeMapper#getProtoTypeMap()} and asserts it equals {@code TypeInfoFactory.INT}.</p>
     */
    @Test
    public void shouldMapProtoSfixed32ToOdpsInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SFIXED32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.INT);
    }

    /**
     * Verifies that the type map resolves the Protobuf {@code SINT64} type to the MaxCompute {@code BIGINT}
     * type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.SINT64} in
     * {@link IntegerDataTypeMapper#getProtoTypeMap()} and asserts it equals {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoSint64ToOdpsBigInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SINT64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the type map resolves the Protobuf {@code SINT32} type to the MaxCompute {@code INT} type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.SINT32} in
     * {@link IntegerDataTypeMapper#getProtoTypeMap()} and asserts it equals {@code TypeInfoFactory.INT}.</p>
     */
    @Test
    public void shouldMapProtoSint32ToOdpsInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SINT32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.INT);
    }

    /**
     * Verifies that the {@code INT64} value-conversion function is an identity mapping.
     *
     * <p>Applies the {@code INT64} mapper from {@link IntegerDataTypeMapper#getProtoPayloadMapperMap()} to a
     * sample {@code long} and asserts the result equals the input unchanged.</p>
     */
    @Test
    public void shouldMapProtoInt64ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.INT64).apply(1L)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code INT32} value-conversion function is an identity mapping.
     *
     * <p>Applies the {@code INT32} mapper from {@link IntegerDataTypeMapper#getProtoPayloadMapperMap()} to a
     * sample {@code int} and asserts the result equals the input unchanged.</p>
     */
    @Test
    public void shouldMapProtoInt32ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.INT32).apply(1)).isEqualTo(1);
    }

    /**
     * Verifies that the {@code UINT64} value-conversion function is an identity mapping.
     *
     * <p>Applies the {@code UINT64} mapper from {@link IntegerDataTypeMapper#getProtoPayloadMapperMap()} to a
     * sample {@code long} and asserts the result equals the input unchanged.</p>
     */
    @Test
    public void shouldMapProtoUint64ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.UINT64).apply(1L)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code UINT32} value-conversion function is an identity mapping.
     *
     * <p>Applies the {@code UINT32} mapper from {@link IntegerDataTypeMapper#getProtoPayloadMapperMap()} to a
     * sample {@code int} and asserts the result equals the input unchanged.</p>
     */
    @Test
    public void shouldMapProtoUint32ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.UINT32).apply(1)).isEqualTo(1);
    }

    /**
     * Verifies that the {@code FIXED64} value-conversion function is an identity mapping.
     *
     * <p>Applies the {@code FIXED64} mapper from {@link IntegerDataTypeMapper#getProtoPayloadMapperMap()} to a
     * sample {@code long} and asserts the result equals the input unchanged.</p>
     */
    @Test
    public void shouldMapProtoFixed64ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FIXED64).apply(1L)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code FIXED32} value-conversion function is an identity mapping.
     *
     * <p>Applies the {@code FIXED32} mapper from {@link IntegerDataTypeMapper#getProtoPayloadMapperMap()} to a
     * sample {@code int} and asserts the result equals the input unchanged.</p>
     */
    @Test
    public void shouldMapProtoFixed32ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FIXED32).apply(1)).isEqualTo(1);
    }

    /**
     * Verifies that the {@code SFIXED64} value-conversion function is an identity mapping.
     *
     * <p>Applies the {@code SFIXED64} mapper from {@link IntegerDataTypeMapper#getProtoPayloadMapperMap()} to
     * a sample {@code long} and asserts the result equals the input unchanged.</p>
     */
    @Test
    public void shouldMapProtoSfixed64ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SFIXED64).apply(1L)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code SFIXED32} value-conversion function is an identity mapping.
     *
     * <p>Applies the {@code SFIXED32} mapper from {@link IntegerDataTypeMapper#getProtoPayloadMapperMap()} to
     * a sample {@code int} and asserts the result equals the input unchanged.</p>
     */
    @Test
    public void shouldMapProtoSfixed32ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SFIXED32).apply(1)).isEqualTo(1);
    }

    /**
     * Verifies that the {@code SINT64} value-conversion function is an identity mapping.
     *
     * <p>Applies the {@code SINT64} mapper from {@link IntegerDataTypeMapper#getProtoPayloadMapperMap()} to a
     * sample {@code long} and asserts the result equals the input unchanged.</p>
     */
    @Test
    public void shouldMapProtoSint64ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SINT64).apply(1L)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code SINT32} value-conversion function is an identity mapping.
     *
     * <p>Applies the {@code SINT32} mapper from {@link IntegerDataTypeMapper#getProtoPayloadMapperMap()} to a
     * sample {@code int} and asserts the result equals the input unchanged.</p>
     */
    @Test
    public void shouldMapProtoSint32ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SINT32).apply(1)).isEqualTo(1);
    }
}
