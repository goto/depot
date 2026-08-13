package com.gotocompany.depot.kafka.mapping;

import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.NullValue;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.WrappersProto;
import com.gotocompany.depot.TestKafkaOutputMessage;
import com.gotocompany.depot.TestKafkaServiceType;
import com.gotocompany.depot.TestKafkaSinkLocation;
import com.gotocompany.depot.TestKafkaSinkProto;
import com.gotocompany.depot.TestKafkaSourceLocation;
import com.gotocompany.depot.exception.ProtoMappingException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CelValueConverterTest {

    private CelValueConverter converter;

    @Before
    public void setup() {
        converter = new CelValueConverter();
    }

    private Descriptors.FieldDescriptor getField(String name) {
        return TestKafkaOutputMessage.getDescriptor().findFieldByName(name);
    }

    @Test
    public void shouldReturnNullForNullCelValues() {
        assertNull(converter.toFieldValue(null, getField("order_id")));
        assertNull(converter.toFieldValue(NullValue.NULL_VALUE, getField("order_id")));
    }

    @Test
    public void shouldConvertStringValue() {
        assertEquals("order-1", converter.toFieldValue("order-1", getField("order_id")));
    }

    @Test
    public void shouldFailForNonStringValueOnStringField() {
        ProtoMappingException exception = assertThrows(ProtoMappingException.class,
                () -> converter.toFieldValue(12L, getField("order_id")));
        assertTrue(exception.getMessage().contains("order_id"));
    }

    @Test
    public void shouldConvertLongValueToInt32Field() {
        assertEquals(42, converter.toFieldValue(42L, getField("order_amount")));
    }

    @Test
    public void shouldFailForOutOfRangeValueOnInt32Field() {
        ProtoMappingException exception = assertThrows(ProtoMappingException.class,
                () -> converter.toFieldValue(Long.MAX_VALUE, getField("order_amount")));
        assertTrue(exception.getMessage().contains("out of range"));
    }

    @Test
    public void shouldConvertUnsignedValueToUint32Field() {
        assertEquals(-1, converter.toFieldValue(UnsignedLong.valueOf(4294967295L), getField("retry_count")));
    }

    @Test
    public void shouldFailForOutOfRangeValueOnUint32Field() {
        assertThrows(ProtoMappingException.class,
                () -> converter.toFieldValue(4294967296L, getField("retry_count")));
    }

    @Test
    public void shouldConvertNumberToLongField() {
        assertEquals(99L, converter.toFieldValue(99L, getField("service_area_id")));
        assertEquals(99L, converter.toFieldValue(UnsignedLong.valueOf(99L), getField("total_count")));
    }

    @Test
    public void shouldFailForNonNumberValueOnNumericFields() {
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue("12", getField("service_area_id")));
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue("12", getField("order_amount")));
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue("1.2", getField("float_amount")));
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue("1.2", getField("order_lat")));
    }

    @Test
    public void shouldConvertNumberToFloatAndDoubleFields() {
        assertEquals(1.25f, converter.toFieldValue(1.25, getField("float_amount")));
        assertEquals(1.25, converter.toFieldValue(1.25, getField("order_lat")));
    }

    @Test
    public void shouldConvertBooleanValue() {
        assertEquals(true, converter.toFieldValue(true, getField("is_valid")));
    }

    @Test
    public void shouldFailForNonBooleanValueOnBoolField() {
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue("true", getField("is_valid")));
    }

    @Test
    public void shouldConvertByteStringValue() {
        assertEquals(ByteString.copyFromUtf8("abc"), converter.toFieldValue(ByteString.copyFromUtf8("abc"), getField("payload")));
    }

    @Test
    public void shouldFailForNonBytesValueOnBytesField() {
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue("abc", getField("payload")));
    }

    @Test
    public void shouldConvertEnumFromNumber() {
        Object enumValue = converter.toFieldValue(2L, getField("service_type"));
        assertEquals(TestKafkaServiceType.Enum.GO_FOOD.getValueDescriptor(), enumValue);
    }

    @Test
    public void shouldConvertEnumFromName() {
        Object enumValue = converter.toFieldValue("GO_RIDE", getField("service_type"));
        assertEquals(TestKafkaServiceType.Enum.GO_RIDE.getValueDescriptor(), enumValue);
    }

    @Test
    public void shouldConvertEnumFromEnumValueDescriptor() {
        Object enumValue = converter.toFieldValue(TestKafkaServiceType.Enum.GO_FOOD.getValueDescriptor(), getField("service_type"));
        assertEquals(TestKafkaServiceType.Enum.GO_FOOD.getValueDescriptor(), enumValue);
    }

    @Test
    public void shouldFailForUnknownEnumNumber() {
        ProtoMappingException exception = assertThrows(ProtoMappingException.class,
                () -> converter.toFieldValue(99L, getField("service_type")));
        assertTrue(exception.getMessage().contains("not a valid enum value"));
    }

    @Test
    public void shouldFailForUnknownEnumName() {
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue("UNKNOWN_ENUM", getField("service_type")));
    }

    @Test
    public void shouldFailForInvalidValueTypeOnEnumField() {
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue(true, getField("service_type")));
    }

    @Test
    public void shouldPassThroughMessageWithSameDescriptor() {
        TestKafkaSourceLocation location = TestKafkaSourceLocation.newBuilder().setLatitude(1.5).setLongitude(2.5).build();
        assertEquals(location, converter.toFieldValue(location, getField("origin")));
    }

    @Test
    public void shouldRebuildMessageWithSameFullNameAndDifferentDescriptor() throws Exception {
        Descriptors.FileDescriptor rebuiltFile = Descriptors.FileDescriptor.buildFrom(
                TestKafkaSinkProto.getDescriptor().toProto(),
                new Descriptors.FileDescriptor[]{TimestampProto.getDescriptor(), WrappersProto.getDescriptor()});
        Descriptors.Descriptor rebuiltDescriptor = rebuiltFile.findMessageTypeByName("TestKafkaSourceLocation");
        DynamicMessage rebuiltLocation = DynamicMessage.newBuilder(rebuiltDescriptor)
                .setField(rebuiltDescriptor.findFieldByName("latitude"), 1.5)
                .setField(rebuiltDescriptor.findFieldByName("longitude"), 2.5)
                .build();
        Object converted = converter.toFieldValue(rebuiltLocation, getField("origin"));
        TestKafkaSourceLocation location = TestKafkaSourceLocation.parseFrom(((DynamicMessage) converted).toByteArray());
        assertEquals(1.5, location.getLatitude(), 0.000001);
        assertEquals(2.5, location.getLongitude(), 0.000001);
        assertEquals(TestKafkaSourceLocation.getDescriptor(), ((DynamicMessage) converted).getDescriptorForType());
    }

    @Test
    public void shouldFailForMessageWithDifferentFullName() {
        TestKafkaSinkLocation location = TestKafkaSinkLocation.newBuilder().setLat(1.5).setLng(2.5).build();
        ProtoMappingException exception = assertThrows(ProtoMappingException.class,
                () -> converter.toFieldValue(location, getField("origin")));
        assertTrue(exception.getMessage().contains("can not be assigned"));
    }

    @Test
    public void shouldWrapPrimitiveValueIntoWrapperMessage() {
        Object wrapped = converter.toFieldValue("hello", getField("order_note"));
        DynamicMessage wrapperMessage = (DynamicMessage) wrapped;
        assertEquals("hello", wrapperMessage.getField(wrapperMessage.getDescriptorForType().findFieldByName("value")));
    }

    @Test
    public void shouldFailForPrimitiveValueOnNonWrapperMessageField() {
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue(12L, getField("origin")));
    }

    @Test
    public void shouldConvertListValuesForRepeatedField() {
        Object converted = converter.toFieldValue(Arrays.asList(1L, 2L, 3L), getField("order_labels"));
        assertEquals(Arrays.asList(1L, 2L, 3L), converted);
    }

    @Test
    public void shouldFailForNonListValueOnRepeatedField() {
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue(1L, getField("order_labels")));
    }

    @Test
    public void shouldConvertMapValuesForMapField() {
        Map<String, String> celMap = new LinkedHashMap<>();
        celMap.put("k1", "v1");
        celMap.put("k2", "v2");
        Object converted = converter.toFieldValue(celMap, getField("labels"));
        List<?> entries = (List<?>) converted;
        assertEquals(2, entries.size());
        DynamicMessage firstEntry = (DynamicMessage) entries.get(0);
        Descriptors.Descriptor entryDescriptor = getField("labels").getMessageType();
        assertEquals("k1", firstEntry.getField(entryDescriptor.findFieldByName("key")));
        assertEquals("v1", firstEntry.getField(entryDescriptor.findFieldByName("value")));
    }

    @Test
    public void shouldFailForNonMapValueOnMapField() {
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue(Collections.singletonList("a"), getField("labels")));
    }

    @Test
    public void shouldFailForBooleanValueOnIntField() {
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue(true, getField("order_amount")));
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue(true, getField("service_area_id")));
    }

    @Test
    public void shouldConvertMapFieldWithIntegerKeysAndMessageValues() {
        Descriptors.FieldDescriptor mapField = com.gotocompany.depot.TestMapMessage.getDescriptor().findFieldByName("metadata");
        com.gotocompany.depot.TestMessage message = com.gotocompany.depot.TestMessage.newBuilder().setOrderNumber("order-1").build();
        Map<Object, Object> celMap = new LinkedHashMap<>();
        celMap.put(4L, message);
        Object converted = converter.toFieldValue(celMap, mapField);
        List<?> entries = (List<?>) converted;
        assertEquals(1, entries.size());
        DynamicMessage entry = (DynamicMessage) entries.get(0);
        Descriptors.Descriptor entryDescriptor = mapField.getMessageType();
        assertEquals(4, entry.getField(entryDescriptor.findFieldByName("key")));
        assertEquals(message, entry.getField(entryDescriptor.findFieldByName("value")));
    }

    @Test
    public void shouldFailForInvalidValueTypeInsideMapField() {
        Map<Object, Object> celMap = new LinkedHashMap<>();
        celMap.put("k1", 12L);
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue(celMap, getField("labels")));
    }

    @Test
    public void shouldFailForInvalidElementTypeInsideRepeatedField() {
        Descriptors.FieldDescriptor repeatedStringField = com.gotocompany.depot.TestTypesMessage.getDescriptor().findFieldByName("list_values");
        assertThrows(ProtoMappingException.class,
                () -> converter.toFieldValue(Arrays.asList("a", 12L), repeatedStringField));
    }

    @Test
    public void shouldConvertRepeatedMessageFieldElements() {
        Descriptors.FieldDescriptor repeatedMessageField = com.gotocompany.depot.TestTypesMessage.getDescriptor().findFieldByName("list_message_values");
        com.gotocompany.depot.TestMessage firstMessage = com.gotocompany.depot.TestMessage.newBuilder().setOrderNumber("order-1").build();
        com.gotocompany.depot.TestMessage secondMessage = com.gotocompany.depot.TestMessage.newBuilder().setOrderNumber("order-2").build();
        Object converted = converter.toFieldValue(Arrays.asList(firstMessage, secondMessage), repeatedMessageField);
        assertEquals(Arrays.asList(firstMessage, secondMessage), converted);
    }

    @Test
    public void shouldConvertEmptyListForRepeatedField() {
        Object converted = converter.toFieldValue(Collections.emptyList(), getField("order_labels"));
        assertEquals(Collections.emptyList(), converted);
    }

    @Test
    public void shouldConvertEmptyMapForMapField() {
        Object converted = converter.toFieldValue(Collections.emptyMap(), getField("labels"));
        assertEquals(Collections.emptyList(), converted);
    }

    @Test
    public void shouldWrapBooleanValueIntoBoolValueWrapper() {
        Descriptors.FieldDescriptor wrapperField = com.gotocompany.depot.TestTypesMessage.getDescriptor().findFieldByName("wrapped_bool_value");
        DynamicMessage wrapped = (DynamicMessage) converter.toFieldValue(true, wrapperField);
        assertEquals(true, wrapped.getField(wrapped.getDescriptorForType().findFieldByName("value")));
    }

    @Test
    public void shouldFailForWrongPrimitiveTypeOnWrapperField() {
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue(12L, getField("order_note")));
        Descriptors.FieldDescriptor wrapperField = com.gotocompany.depot.TestTypesMessage.getDescriptor().findFieldByName("wrapped_bool_value");
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue("true", wrapperField));
    }

    @Test
    public void shouldFailForMapValueOnSingularMessageField() {
        Map<Object, Object> celMap = new LinkedHashMap<>();
        celMap.put("latitude", 1.5);
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue(celMap, getField("origin")));
    }

    @Test
    public void shouldFailForListValueOnSingularStringField() {
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue(Collections.singletonList("a"), getField("order_id")));
    }

    @Test
    public void shouldRoundTripMaxUnsignedLongValue() {
        Object converted = converter.toFieldValue(UnsignedLong.MAX_VALUE, getField("total_count"));
        assertEquals(-1L, converted);
    }

    @Test
    public void shouldConvertNumberToEnumAtBoundaries() {
        assertEquals(com.gotocompany.depot.TestKafkaServiceType.Enum.UNKNOWN.getValueDescriptor(),
                converter.toFieldValue(0L, getField("service_type")));
        assertThrows(ProtoMappingException.class, () -> converter.toFieldValue(-1L, getField("service_type")));
    }
}
