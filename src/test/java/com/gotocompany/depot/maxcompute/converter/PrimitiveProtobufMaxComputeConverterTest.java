package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link PrimitiveProtobufMaxComputeConverter}, which maps scalar (and repeated scalar)
 * Protobuf fields to their MaxCompute types and values.
 *
 * <p>The converter delegates to the primitive mapping strategies assembled by the configured mapper factory.
 * With all casting feature flags disabled (the configuration stubbed in {@link #init()}), the suite asserts
 * the default mapping for every primitive Protobuf type, covering:</p>
 * <ul>
 *     <li>type resolution via {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} for
 *     each scalar field;</li>
 *     <li>value conversion via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)} for
 *     singular fields, including the rejection of non-finite floating point values with an
 *     {@link InvalidMessageException};</li>
 *     <li>value conversion for repeated fields, asserting the element runtime types of the produced list.</li>
 * </ul>
 *
 * <p>Inputs are driven through the generated {@code TestFields} and {@code TestFieldsRepeated} fixtures, and
 * fields are resolved both by name (for the type tests) and by positional index (for the value tests).</p>
 */
public class PrimitiveProtobufMaxComputeConverterTest {

    /**
     * The converter under test, rebuilt in {@link #init()} from the mocked configuration.
     */
    private PrimitiveProtobufMaxComputeConverter primitiveProtobufMaxComputeConverter;

    /**
     * Descriptor of the {@code TestFields} fixture message, holding one field per singular scalar type.
     */
    private Descriptors.Descriptor descriptor;

    /**
     * Descriptor of the {@code TestFieldsRepeated} fixture message, holding one repeated field per scalar type.
     */
    private Descriptors.Descriptor descriptorRepeated;

    /**
     * Builds the converter under test from a Mockito-mocked {@link MaxComputeSinkConfig}.
     *
     * <p>Resolves the {@code TestFields} and {@code TestFieldsRepeated} descriptors and stubs the configuration
     * with integer-to-bigint, double-to-decimal, and float-to-decimal casting all disabled, so that the
     * default primitive mapping is exercised, then constructs the
     * {@link PrimitiveProtobufMaxComputeConverter}.</p>
     */
    @Before
    public void init() {
        this.descriptor = TestMaxComputeTypeInfo.TestFields.getDescriptor();
        this.descriptorRepeated = TestMaxComputeTypeInfo.TestFieldsRepeated.getDescriptor();
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled()).thenReturn(false);
        Mockito.when(maxComputeSinkConfig.isProtoDoubleToDecimalEnabled()).thenReturn(false);
        Mockito.when(maxComputeSinkConfig.isProtoFloatTypeToDecimalEnabled()).thenReturn(false);
        this.primitiveProtobufMaxComputeConverter = new PrimitiveProtobufMaxComputeConverter(maxComputeSinkConfig);
    }

    /**
     * Verifies that the {@code bytes_field} resolves to the MaxCompute {@code BINARY} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.BINARY}.</p>
     */
    @Test
    public void shouldConvertToBinary() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("bytes_field")));

        assertEquals(TypeInfoFactory.BINARY, typeInfo);
    }

    /**
     * Verifies that the {@code string_field} resolves to the MaxCompute {@code STRING} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.STRING}.</p>
     */
    @Test
    public void shouldConvertToString() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("string_field")));

        assertEquals(TypeInfoFactory.STRING, typeInfo);
    }

    /**
     * Verifies that the {@code enum_field} resolves to the MaxCompute {@code STRING} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.STRING}, reflecting that enums are stored as their string name.</p>
     */
    @Test
    public void shouldConvertEnumToString() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("enum_field")));

        assertEquals(TypeInfoFactory.STRING, typeInfo);
    }

    /**
     * Verifies that the {@code double_field} resolves to the MaxCompute {@code DOUBLE} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.DOUBLE}.</p>
     */
    @Test
    public void shouldConvertToDouble() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("double_field")));

        assertEquals(TypeInfoFactory.DOUBLE, typeInfo);
    }

    /**
     * Verifies that the {@code float_field} resolves to the MaxCompute {@code FLOAT} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.FLOAT}.</p>
     */
    @Test
    public void shouldConvertToFloat() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("float_field")));

        assertEquals(TypeInfoFactory.FLOAT, typeInfo);
    }

    /**
     * Verifies that the {@code bool_field} resolves to the MaxCompute {@code BOOLEAN} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.BOOLEAN}.</p>
     */
    @Test
    public void shouldConvertToBoolean() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("bool_field")));

        assertEquals(TypeInfoFactory.BOOLEAN, typeInfo);
    }

    /**
     * Verifies that the {@code int64_field} resolves to the MaxCompute {@code BIGINT} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldConvertToBigInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("int64_field")));

        assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    /**
     * Verifies that the {@code uint64_field} resolves to the MaxCompute {@code BIGINT} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldConvertUInt64ToBigInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("uint64_field")));

        assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    /**
     * Verifies that the {@code int32_field} resolves to the MaxCompute {@code INT} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.INT}.</p>
     */
    @Test
    public void shouldConvertToInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("int32_field")));

        assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    /**
     * Verifies that the {@code uint32_field} resolves to the MaxCompute {@code INT} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.INT}.</p>
     */
    @Test
    public void shouldConvertUInt32ToInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("uint32_field")));

        assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    /**
     * Verifies that the {@code fixed64_field} resolves to the MaxCompute {@code BIGINT} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldConvertFixed64ToBigInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("fixed64_field")));

        assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    /**
     * Verifies that the {@code fixed32_field} resolves to the MaxCompute {@code INT} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.INT}.</p>
     */
    @Test
    public void shouldConvertFixed32ToInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("fixed32_field")));

        assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    /**
     * Verifies that the {@code sfixed32_field} resolves to the MaxCompute {@code INT} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.INT}.</p>
     */
    @Test
    public void shouldConvertSFixed32ToInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("sfixed32_field")));

        assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    /**
     * Verifies that the {@code sfixed64_field} resolves to the MaxCompute {@code BIGINT} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldConvertSFixed64ToBigInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("sfixed64_field")));

        assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    /**
     * Verifies that the {@code sint32_field} resolves to the MaxCompute {@code INT} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.INT}.</p>
     */
    @Test
    public void shouldConvertSInt32ToInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("sint32_field")));

        assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    /**
     * Verifies that the {@code sint64_field} resolves to the MaxCompute {@code BIGINT} type.
     *
     * <p>Resolves the field by name and asserts
     * {@link PrimitiveProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.BIGINT}.</p>
     */
    @Test
    public void shouldConvertSInt64ToBigInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.findFieldByName("sint64_field")));

        assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    /**
     * Verifies that a {@code BYTES} value is converted to a MaxCompute {@link Binary}.
     *
     * <p>Builds a {@code TestFields} message with a bytes field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * {@link Binary} whose backing data equals the original bytes.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsByteArray() {
        byte[] bytes = "bytes".getBytes(StandardCharsets.UTF_8);
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setBytesField(ByteString.copyFrom(bytes)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(0), message.getField(descriptor.getFields().get(0)), 0));

        assertTrue(result instanceof Binary);
        assertArrayEquals(bytes, ((Binary) result).data());
    }

    /**
     * Verifies that a {@code STRING} value is converted to an equal {@link String}.
     *
     * <p>Builds a {@code TestFields} message with a string field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * {@link String} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsString() {
        String value = "test";
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setStringField(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(1), message.getField(descriptor.getFields().get(1)), 0));

        assertTrue(result instanceof String);
        assertEquals(value, result);
    }

    /**
     * Verifies that an {@code ENUM} value is converted to its name as a {@link String}.
     *
     * <p>Builds a {@code TestFields} message with an enum field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * {@link String} equal to the enum constant's {@code name()}.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsEnum() {
        TestMaxComputeTypeInfo.TestEnum value = TestMaxComputeTypeInfo.TestEnum.TEST_1;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setEnumField(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(2), message.getField(descriptor.getFields().get(2)), 0));

        assertTrue(result instanceof String);
        assertEquals(value.name(), result);
    }

    /**
     * Verifies that a finite {@code DOUBLE} value is converted to an equal {@link Double}.
     *
     * <p>Builds a {@code TestFields} message with a double field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * {@link Double} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsDouble() {
        double value = 1.23;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setDoubleField(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));

        assertTrue(result instanceof Double);
        assertEquals(value, result);
    }

    /**
     * Verifies that a positive-infinity {@code DOUBLE} value is rejected.
     *
     * <p>Builds a {@code TestFields} message whose double field is {@link Double#POSITIVE_INFINITY}, converts
     * it via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and expects an
     * {@link InvalidMessageException}.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenDoublePositiveInfinity() {
        double value = Double.POSITIVE_INFINITY;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setDoubleField(value).build();

        primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));
    }

    /**
     * Verifies that a negative-infinity {@code DOUBLE} value is rejected.
     *
     * <p>Builds a {@code TestFields} message whose double field is {@link Double#NEGATIVE_INFINITY}, converts
     * it via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and expects an
     * {@link InvalidMessageException}.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenDoubleNegativeInfinity() {
        double value = Double.NEGATIVE_INFINITY;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setDoubleField(value).build();

        primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));
    }

    /**
     * Verifies that a {@code NaN} {@code DOUBLE} value is rejected.
     *
     * <p>Builds a {@code TestFields} message whose double field is {@link Double#NaN}, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and expects an
     * {@link InvalidMessageException}.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenDoubleNaN() {
        double value = Double.NaN;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setDoubleField(value).build();

        primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));
    }

    /**
     * Verifies that a finite {@code FLOAT} value is converted to an equal {@link Float}.
     *
     * <p>Builds a {@code TestFields} message with a float field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * {@link Float} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsFloat() {
        float value = 1.23f;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setFloatField(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(4), message.getField(descriptor.getFields().get(4)), 0));

        assertTrue(result instanceof Float);
        assertEquals(value, result);
    }

    /**
     * Verifies that a positive-infinity {@code FLOAT} value is rejected.
     *
     * <p>Builds a {@code TestFields} message whose float field is {@link Float#POSITIVE_INFINITY}, converts it
     * via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and expects an
     * {@link InvalidMessageException}.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenFloatPositiveInfinity() {
        float value = Float.POSITIVE_INFINITY;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setFloatField(value).build();

        primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(4), message.getField(descriptor.getFields().get(4)), 0));
    }

    /**
     * Verifies that a negative-infinity {@code FLOAT} value is rejected.
     *
     * <p>Builds a {@code TestFields} message whose float field is {@link Float#NEGATIVE_INFINITY}, converts it
     * via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and expects an
     * {@link InvalidMessageException}.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenFloatNegativeInfinity() {
        float value = Float.NEGATIVE_INFINITY;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setFloatField(value).build();

        primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(4), message.getField(descriptor.getFields().get(4)), 0));
    }

    /**
     * Verifies that a {@code NaN} {@code FLOAT} value is rejected.
     *
     * <p>Builds a {@code TestFields} message whose float field is {@link Float#NaN}, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and expects an
     * {@link InvalidMessageException}.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenFloatNaN() {
        float value = Float.NaN;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setFloatField(value).build();

        primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(4), message.getField(descriptor.getFields().get(4)), 0));
    }

    /**
     * Verifies that a {@code BOOL} value is converted to an equal {@link Boolean}.
     *
     * <p>Builds a {@code TestFields} message with a boolean field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * {@link Boolean} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsBoolean() {
        boolean value = true;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setBoolField(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(5), message.getField(descriptor.getFields().get(5)), 0));

        assertTrue(result instanceof Boolean);
        assertEquals(value, result);
    }

    /**
     * Verifies that an {@code INT64} value is converted to an equal {@link Long}.
     *
     * <p>Builds a {@code TestFields} message with an int64 field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * {@link Long} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsInt64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setInt64Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(6), message.getField(descriptor.getFields().get(6)), 0));

        assertTrue(result instanceof Long);
        assertEquals(value, result);
    }

    /**
     * Verifies that a {@code UINT64} value is converted to an equal {@link Long}.
     *
     * <p>Builds a {@code TestFields} message with a uint64 field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * {@link Long} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsUInt64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setUint64Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(7), message.getField(descriptor.getFields().get(7)), 0));

        assertTrue(result instanceof Long);
        assertEquals(value, result);
    }

    /**
     * Verifies that an {@code INT32} value is converted to an equal {@link Integer}.
     *
     * <p>Builds a {@code TestFields} message with an int32 field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is an
     * {@link Integer} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsInt32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setInt32Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(8), message.getField(descriptor.getFields().get(8)), 0));

        assertTrue(result instanceof Integer);
        assertEquals(value, result);
    }

    /**
     * Verifies that a {@code UINT32} value is converted to an equal {@link Integer}.
     *
     * <p>Builds a {@code TestFields} message with a uint32 field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is an
     * {@link Integer} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsUInt32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setUint32Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(9), message.getField(descriptor.getFields().get(9)), 0));

        assertTrue(result instanceof Integer);
        assertEquals(value, result);
    }

    /**
     * Verifies that a {@code FIXED64} value is converted to an equal {@link Long}.
     *
     * <p>Builds a {@code TestFields} message with a fixed64 field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * {@link Long} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsFixed64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setFixed64Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(10), message.getField(descriptor.getFields().get(10)), 0));

        assertTrue(result instanceof Long);
        assertEquals(value, result);
    }

    /**
     * Verifies that a {@code FIXED32} value is converted to an equal {@link Integer}.
     *
     * <p>Builds a {@code TestFields} message with a fixed32 field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is an
     * {@link Integer} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsFixed32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setFixed32Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(11), message.getField(descriptor.getFields().get(11)), 0));

        assertTrue(result instanceof Integer);
        assertEquals(value, result);
    }

    /**
     * Verifies that an {@code SFIXED32} value is converted to an equal {@link Integer}.
     *
     * <p>Builds a {@code TestFields} message with an sfixed32 field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is an
     * {@link Integer} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsSFixed32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setSfixed32Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(12), message.getField(descriptor.getFields().get(12)), 0));

        assertTrue(result instanceof Integer);
        assertEquals(value, result);
    }

    /**
     * Verifies that an {@code SFIXED64} value is converted to an equal {@link Long}.
     *
     * <p>Builds a {@code TestFields} message with an sfixed64 field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * {@link Long} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsSFixed64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setSfixed64Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(13), message.getField(descriptor.getFields().get(13)), 0));

        assertTrue(result instanceof Long);
        assertEquals(value, result);
    }

    /**
     * Verifies that an {@code SINT32} value is converted to an equal {@link Integer}.
     *
     * <p>Builds a {@code TestFields} message with an sint32 field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is an
     * {@link Integer} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsSInt32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setSint32Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(14), message.getField(descriptor.getFields().get(14)), 0));

        assertTrue(result instanceof Integer);
        assertEquals(value, result);
    }

    /**
     * Verifies that an {@code SINT64} value is converted to an equal {@link Long}.
     *
     * <p>Builds a {@code TestFields} message with an sint64 field, converts it via
     * {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * {@link Long} equal to the input.</p>
     */
    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsSInt64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setSint64Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(15), message.getField(descriptor.getFields().get(15)), 0));

        assertTrue(result instanceof Long);
        assertEquals(value, result);
    }

    /**
     * Verifies that a repeated {@code BYTES} field is converted to a list of {@link Binary} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element bytes list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is a {@link Binary}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsByteArrayList() {
        byte[] bytes = "bytes".getBytes(StandardCharsets.UTF_8);
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllBytesFields(Collections.singletonList(ByteString.copyFrom(bytes))).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(0), message.getField(descriptorRepeated.getFields().get(0)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Binary));
    }

    /**
     * Verifies that a repeated {@code STRING} field is converted to a list of {@link String} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element string list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is a {@link String}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsStringList() {
        String value = "test";
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllStringFields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(1), message.getField(descriptorRepeated.getFields().get(1)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof String));
    }

    /**
     * Verifies that a repeated {@code ENUM} field is converted to a list of {@link String} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element enum list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is a {@link String}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsEnumList() {
        TestMaxComputeTypeInfo.TestEnum value = TestMaxComputeTypeInfo.TestEnum.TEST_1;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllEnumFields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(2), message.getField(descriptorRepeated.getFields().get(2)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof String));
    }

    /**
     * Verifies that a repeated {@code DOUBLE} field is converted to a list of {@link Double} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element double list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is a {@link Double}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsDoubleList() {
        double value = 1.23;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllDoubleFields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(3), message.getField(descriptorRepeated.getFields().get(3)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Double));
    }

    /**
     * Verifies that a repeated {@code FLOAT} field is converted to a list of {@link Float} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element float list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is a {@link Float}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsFloatList() {
        float value = 1.23f;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllFloatFields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(4), message.getField(descriptorRepeated.getFields().get(4)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Float));
    }

    /**
     * Verifies that a repeated {@code BOOL} field is converted to a list of {@link Boolean} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element boolean list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is a {@link Boolean}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsBooleanList() {
        boolean value = true;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllBoolFields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(5), message.getField(descriptorRepeated.getFields().get(5)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Boolean));
    }

    /**
     * Verifies that a repeated {@code INT64} field is converted to a list of {@link Long} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element int64 list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is a {@link Long}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsInt64List() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllInt64Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(6), message.getField(descriptorRepeated.getFields().get(6)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Long));
    }

    /**
     * Verifies that a repeated {@code UINT64} field is converted to a list of {@link Long} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element uint64 list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is a {@link Long}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsUInt64List() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllUint64Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(7), message.getField(descriptorRepeated.getFields().get(7)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Long));
    }

    /**
     * Verifies that a repeated {@code INT32} field is converted to a list of {@link Integer} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element int32 list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is an {@link Integer}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsInt32List() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllInt32Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(8), message.getField(descriptorRepeated.getFields().get(8)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Integer));
    }

    /**
     * Verifies that a repeated {@code UINT32} field is converted to a list of {@link Integer} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element uint32 list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is an {@link Integer}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsUInt32List() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllUint32Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(9), message.getField(descriptorRepeated.getFields().get(9)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Integer));
    }

    /**
     * Verifies that a repeated {@code FIXED64} field is converted to a list of {@link Long} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element fixed64 list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is a {@link Long}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsFixed64List() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllFixed64Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(10), message.getField(descriptorRepeated.getFields().get(10)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Long));
    }

    /**
     * Verifies that a repeated {@code FIXED32} field is converted to a list of {@link Integer} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element fixed32 list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is an {@link Integer}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsFixed32List() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllFixed32Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(11), message.getField(descriptorRepeated.getFields().get(11)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Integer));
    }

    /**
     * Verifies that a repeated {@code SFIXED32} field is converted to a list of {@link Integer} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element sfixed32 list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is an {@link Integer}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsSFixed32List() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllSfixed32Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(12), message.getField(descriptorRepeated.getFields().get(12)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Integer));
    }

    /**
     * Verifies that a repeated {@code SFIXED64} field is converted to a list of {@link Long} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element sfixed64 list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is a {@link Long}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsSFixed64List() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllSfixed64Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(13), message.getField(descriptorRepeated.getFields().get(13)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Long));
    }

    /**
     * Verifies that a repeated {@code SINT32} field is converted to a list of {@link Integer} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element sint32 list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is an {@link Integer}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsSInt32List() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllSint32Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(14), message.getField(descriptorRepeated.getFields().get(14)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Integer));
    }

    /**
     * Verifies that a repeated {@code SINT64} field is converted to a list of {@link Long} values.
     *
     * <p>Builds a {@code TestFieldsRepeated} message with a single-element sint64 list, converts the repeated
     * field via {@link PrimitiveProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@link java.util.List} whose every element is a {@link Long}.</p>
     */
    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsSInt64List() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllSint64Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(15), message.getField(descriptorRepeated.getFields().get(15)), 0));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Long));
    }

}
