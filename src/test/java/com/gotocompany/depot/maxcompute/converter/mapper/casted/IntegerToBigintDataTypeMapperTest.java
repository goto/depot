package com.gotocompany.depot.maxcompute.converter.mapper.casted;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link IntegerToBigintDataTypeMapper}, the upcasting primitive mapping strategy that widens
 * every integral Protobuf scalar type to MaxCompute {@code BIGINT}.
 *
 * <p>Unlike the default integer strategy (which preserves the 32-bit families as MaxCompute {@code INT}), this
 * mapper is selected when the sink is configured to promote all integers to {@code BIGINT}. The suite
 * therefore asserts that:</p>
 * <ul>
 *     <li>every handled Protobuf integer type, in both the 64-bit and 32-bit families, resolves through
 *     {@link IntegerToBigintDataTypeMapper#getProtoTypeMap()} to {@code TypeInfoFactory.BIGINT};</li>
 *     <li>every value-conversion function from
 *     {@link IntegerToBigintDataTypeMapper#getProtoPayloadMapperMap()} widens its numeric input to a
 *     {@code long} value equal to {@code 1L}.</li>
 * </ul>
 *
 * <p>Each test runs against a fresh, real {@link IntegerToBigintDataTypeMapper} instance.</p>
 */
public class IntegerToBigintDataTypeMapperTest {

    /**
     * The mapper under test, exercised through its public type and value lookup tables.
     */
    private final IntegerToBigintDataTypeMapper upcastedIntegerPrimitiveProtobufMappingStrategy = new IntegerToBigintDataTypeMapper();

    /**
     * Verifies that the upcasting type map resolves the Protobuf {@code INT64} type to MaxCompute
     * {@code BIGINT}.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.INT64} in
     * {@link IntegerToBigintDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoInt64ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.INT64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the upcasting type map resolves the Protobuf {@code INT32} type to MaxCompute
     * {@code BIGINT} (widened from the default {@code INT}).
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.INT32} in
     * {@link IntegerToBigintDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoInt32ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.INT32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the upcasting type map resolves the Protobuf {@code UINT64} type to MaxCompute
     * {@code BIGINT}.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.UINT64} in
     * {@link IntegerToBigintDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoUint64ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.UINT64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the upcasting type map resolves the Protobuf {@code UINT32} type to MaxCompute
     * {@code BIGINT} (widened from the default {@code INT}).
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.UINT32} in
     * {@link IntegerToBigintDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoUint32ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.UINT32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the upcasting type map resolves the Protobuf {@code FIXED64} type to MaxCompute
     * {@code BIGINT}.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.FIXED64} in
     * {@link IntegerToBigintDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoFixed64ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.FIXED64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the upcasting type map resolves the Protobuf {@code FIXED32} type to MaxCompute
     * {@code BIGINT} (widened from the default {@code INT}).
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.FIXED32} in
     * {@link IntegerToBigintDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoFixed32ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.FIXED32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the upcasting type map resolves the Protobuf {@code SFIXED64} type to MaxCompute
     * {@code BIGINT}.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.SFIXED64} in
     * {@link IntegerToBigintDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoSfixed64ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SFIXED64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the upcasting type map resolves the Protobuf {@code SFIXED32} type to MaxCompute
     * {@code BIGINT} (widened from the default {@code INT}).
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.SFIXED32} in
     * {@link IntegerToBigintDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoSfixed32ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SFIXED32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the upcasting type map resolves the Protobuf {@code SINT64} type to MaxCompute
     * {@code BIGINT}.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.SINT64} in
     * {@link IntegerToBigintDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoSint64ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SINT64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the upcasting type map resolves the Protobuf {@code SINT32} type to MaxCompute
     * {@code BIGINT} (widened from the default {@code INT}).
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.SINT32} in
     * {@link IntegerToBigintDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldMapProtoSint32ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SINT32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    /**
     * Verifies that the {@code INT64} value-conversion function returns its {@code long} input as
     * {@code BIGINT}.
     *
     * <p>Applies the {@code INT64} mapper from
     * {@link IntegerToBigintDataTypeMapper#getProtoPayloadMapperMap()} to the {@code long} value {@code 1L}
     * and asserts the result equals {@code 1L}.</p>
     */
    @Test
    public void shouldMapProtoInt64Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.INT64).apply(1L)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code INT32} value-conversion function widens its {@code int} input to a
     * {@code long}.
     *
     * <p>Applies the {@code INT32} mapper from
     * {@link IntegerToBigintDataTypeMapper#getProtoPayloadMapperMap()} to the {@code int} value {@code 1} and
     * asserts the result equals {@code 1L}.</p>
     */
    @Test
    public void shouldMapProtoInt32Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.INT32).apply(1)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code UINT64} value-conversion function returns its {@code long} input as
     * {@code BIGINT}.
     *
     * <p>Applies the {@code UINT64} mapper from
     * {@link IntegerToBigintDataTypeMapper#getProtoPayloadMapperMap()} to the {@code long} value {@code 1L}
     * and asserts the result equals {@code 1L}.</p>
     */
    @Test
    public void shouldMapProtoUint64Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.UINT64).apply(1L)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code UINT32} value-conversion function widens its {@code int} input to a
     * {@code long}.
     *
     * <p>Applies the {@code UINT32} mapper from
     * {@link IntegerToBigintDataTypeMapper#getProtoPayloadMapperMap()} to the {@code int} value {@code 1} and
     * asserts the result equals {@code 1L}.</p>
     */
    @Test
    public void shouldMapProtoUint32Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.UINT32).apply(1)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code FIXED64} value-conversion function returns its {@code long} input as
     * {@code BIGINT}.
     *
     * <p>Applies the {@code FIXED64} mapper from
     * {@link IntegerToBigintDataTypeMapper#getProtoPayloadMapperMap()} to the {@code long} value {@code 1L}
     * and asserts the result equals {@code 1L}.</p>
     */
    @Test
    public void shouldMapProtoFixed64Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FIXED64).apply(1L)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code FIXED32} value-conversion function widens its {@code int} input to a
     * {@code long}.
     *
     * <p>Applies the {@code FIXED32} mapper from
     * {@link IntegerToBigintDataTypeMapper#getProtoPayloadMapperMap()} to the {@code int} value {@code 1} and
     * asserts the result equals {@code 1L}.</p>
     */
    @Test
    public void shouldMapProtoFixed32Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FIXED32).apply(1)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code SFIXED64} value-conversion function returns its {@code long} input as
     * {@code BIGINT}.
     *
     * <p>Applies the {@code SFIXED64} mapper from
     * {@link IntegerToBigintDataTypeMapper#getProtoPayloadMapperMap()} to the {@code long} value {@code 1L}
     * and asserts the result equals {@code 1L}.</p>
     */
    @Test
    public void shouldMapProtoSfixed64Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SFIXED64).apply(1L)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code SFIXED32} value-conversion function widens its {@code int} input to a
     * {@code long}.
     *
     * <p>Applies the {@code SFIXED32} mapper from
     * {@link IntegerToBigintDataTypeMapper#getProtoPayloadMapperMap()} to the {@code int} value {@code 1} and
     * asserts the result equals {@code 1L}.</p>
     */
    @Test
    public void shouldMapProtoSfixed32Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SFIXED32).apply(1)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code SINT64} value-conversion function returns its {@code long} input as
     * {@code BIGINT}.
     *
     * <p>Applies the {@code SINT64} mapper from
     * {@link IntegerToBigintDataTypeMapper#getProtoPayloadMapperMap()} to the {@code long} value {@code 1L}
     * and asserts the result equals {@code 1L}.</p>
     */
    @Test
    public void shouldMapProtoSint64Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SINT64).apply(1L)).isEqualTo(1L);
    }

    /**
     * Verifies that the {@code SINT32} value-conversion function widens its {@code int} input to a
     * {@code long}.
     *
     * <p>Applies the {@code SINT32} mapper from
     * {@link IntegerToBigintDataTypeMapper#getProtoPayloadMapperMap()} to the {@code int} value {@code 1} and
     * asserts the result equals {@code 1L}.</p>
     */
    @Test
    public void shouldMapProtoSint32Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SINT32).apply(1)).isEqualTo(1L);
    }

}
