package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.maxcompute.converter.type.PrimitiveTypeInfoConverter;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.nio.charset.StandardCharsets;

public class PrimitivePayloadConverterTest {

    private final PrimitiveTypeInfoConverter primitiveTypeInfoConverter = new PrimitiveTypeInfoConverter();
    private final PrimitivePayloadConverter primitivePayloadConverter = new PrimitivePayloadConverter(primitiveTypeInfoConverter);
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestFields.getDescriptor();

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsByteArray() {
        byte[] bytes = "bytes".getBytes(StandardCharsets.UTF_8);
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setBytesField(ByteString.copyFrom(bytes))
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(0), message.getField(descriptor.getFields().get(0)));

        Assertions.assertTrue(result instanceof byte[]);
        Assertions.assertArrayEquals(bytes, (byte[]) result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsString() {
        String value = "test";
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setStringField(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(1), message.getField(descriptor.getFields().get(1)));

        Assertions.assertTrue(result instanceof String);
        Assertions.assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsEnum() {
        TestMaxComputeTypeInfo.TestEnum value = TestMaxComputeTypeInfo.TestEnum.TEST_1;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setEnumField(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(2), message.getField(descriptor.getFields().get(2)));

        Assertions.assertTrue(result instanceof String);
        Assertions.assertEquals(value.name(), result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsDouble() {
        double value = 1.23;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setDoubleField(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)));

        Assertions.assertTrue(result instanceof Double);
        Assertions.assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsFloat() {
        float value = 1.23f;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setFloatField(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(4), message.getField(descriptor.getFields().get(4)));

        Assertions.assertTrue(result instanceof Float);
        Assertions.assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsBoolean() {
        boolean value = true;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setBoolField(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(5), message.getField(descriptor.getFields().get(5)));

        Assertions.assertTrue(result instanceof Boolean);
        Assertions.assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsInt64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setInt64Field(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(6), message.getField(descriptor.getFields().get(6)));

        Assertions.assertTrue(result instanceof Long);
        Assertions.assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsUInt64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setUint64Field(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(7), message.getField(descriptor.getFields().get(7)));

        Assertions.assertTrue(result instanceof Long);
        Assertions.assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsInt32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setInt32Field(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(8), message.getField(descriptor.getFields().get(8)));

        Assertions.assertTrue(result instanceof Integer);
        Assertions.assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsUInt32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setUint32Field(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(9), message.getField(descriptor.getFields().get(9)));

        Assertions.assertTrue(result instanceof Integer);
        Assertions.assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsFixed64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setFixed64Field(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(10), message.getField(descriptor.getFields().get(10)));

        Assertions.assertTrue(result instanceof Long);
        Assertions.assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsFixed32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setFixed32Field(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(11), message.getField(descriptor.getFields().get(11)));

        Assertions.assertTrue(result instanceof Integer);
        Assertions.assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsSFixed32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setSfixed32Field(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(12), message.getField(descriptor.getFields().get(12)));

        Assertions.assertTrue(result instanceof Integer);
        Assertions.assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsSFixed64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setSfixed64Field(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(13), message.getField(descriptor.getFields().get(13)));

        Assertions.assertTrue(result instanceof Long);
        Assertions.assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsSInt32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setSint32Field(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(14), message.getField(descriptor.getFields().get(14)));

        Assertions.assertTrue(result instanceof Integer);
        Assertions.assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsSInt64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder()
                .setSint64Field(value)
                .build();

        Object result = primitivePayloadConverter.convertSingular(descriptor.getFields().get(15), message.getField(descriptor.getFields().get(15)));

        Assertions.assertTrue(result instanceof Long);
        Assertions.assertEquals(value, result);
    }


}
