package com.gotocompany.depot.kafka.mapping;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMapMessage;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestTypesMessage;
import dev.cel.common.types.ListType;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.common.types.StructTypeReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CelTypeMapperTest {

    private static Descriptors.FieldDescriptor getTypesField(String name) {
        return TestTypesMessage.getDescriptor().findFieldByName(name);
    }

    private static Descriptors.FieldDescriptor getMapField(String name) {
        return TestMapMessage.getDescriptor().findFieldByName(name);
    }

    @Test
    public void shouldMapSignedIntegerFieldsToIntType() {
        assertEquals(SimpleType.INT, CelTypeMapper.toCelType(getTypesField("int32_value")));
        assertEquals(SimpleType.INT, CelTypeMapper.toCelType(getTypesField("int64_value")));
        assertEquals(SimpleType.INT, CelTypeMapper.toCelType(getTypesField("sint32_value")));
        assertEquals(SimpleType.INT, CelTypeMapper.toCelType(getTypesField("sint64_value")));
        assertEquals(SimpleType.INT, CelTypeMapper.toCelType(getTypesField("sfixed32_value")));
        assertEquals(SimpleType.INT, CelTypeMapper.toCelType(getTypesField("sfixed64_value")));
    }

    @Test
    public void shouldMapUnsignedIntegerFieldsToUintType() {
        assertEquals(SimpleType.UINT, CelTypeMapper.toCelType(getTypesField("uint32_value")));
        assertEquals(SimpleType.UINT, CelTypeMapper.toCelType(getTypesField("uint64_value")));
        assertEquals(SimpleType.UINT, CelTypeMapper.toCelType(getTypesField("fixed32_value")));
        assertEquals(SimpleType.UINT, CelTypeMapper.toCelType(getTypesField("fixed64_value")));
    }

    @Test
    public void shouldMapFloatingPointFieldsToDoubleType() {
        assertEquals(SimpleType.DOUBLE, CelTypeMapper.toCelType(getTypesField("float_value")));
        assertEquals(SimpleType.DOUBLE, CelTypeMapper.toCelType(getTypesField("double_value")));
    }

    @Test
    public void shouldMapScalarFields() {
        assertEquals(SimpleType.BOOL, CelTypeMapper.toCelType(getTypesField("bool_value")));
        assertEquals(SimpleType.STRING, CelTypeMapper.toCelType(getTypesField("string_value")));
        assertEquals(SimpleType.BYTES, CelTypeMapper.toCelType(getTypesField("bytes_value")));
    }

    @Test
    public void shouldMapEnumFieldToDynType() {
        assertEquals(SimpleType.DYN, CelTypeMapper.toCelType(getTypesField("enum_value")));
    }

    @Test
    public void shouldMapMessageFieldToStructTypeReference() {
        assertEquals(StructTypeReference.create(TestMessage.getDescriptor().getFullName()),
                CelTypeMapper.toCelType(getTypesField("message_value")));
    }

    @Test
    public void shouldMapWellKnownTypes() {
        assertEquals(SimpleType.TIMESTAMP, CelTypeMapper.toCelType(getTypesField("timestamp_value")));
        assertEquals(SimpleType.DURATION, CelTypeMapper.toCelType(getTypesField("duration_value")));
        assertEquals(SimpleType.DYN, CelTypeMapper.toCelType(getTypesField("struct_value")));
        assertEquals(SimpleType.DYN, CelTypeMapper.toCelType(getTypesField("wrapped_bool_value")));
        assertEquals(SimpleType.DYN, CelTypeMapper.toCelType(getTypesField("any_value")));
    }

    @Test
    public void shouldMapRepeatedFieldsToListType() {
        assertEquals(ListType.create(SimpleType.STRING), CelTypeMapper.toCelType(getTypesField("list_values")));
        assertEquals(ListType.create(StructTypeReference.create(TestMessage.getDescriptor().getFullName())),
                CelTypeMapper.toCelType(getTypesField("list_message_values")));
    }

    @Test
    public void shouldMapMapFieldsToMapType() {
        assertEquals(MapType.create(SimpleType.STRING, SimpleType.STRING), CelTypeMapper.toCelType(getMapField("current_state")));
        assertEquals(MapType.create(SimpleType.INT, StructTypeReference.create(TestMessage.getDescriptor().getFullName())),
                CelTypeMapper.toCelType(getMapField("metadata")));
    }

    @Test
    public void shouldMapMapFieldsWithWellKnownValueTypes() {
        assertEquals(MapType.create(SimpleType.STRING, SimpleType.DURATION), CelTypeMapper.toCelType(getMapField("durations")));
        assertEquals(MapType.create(SimpleType.STRING, SimpleType.TIMESTAMP), CelTypeMapper.toCelType(getMapField("time_stamps")));
        assertEquals(MapType.create(SimpleType.STRING, SimpleType.DYN), CelTypeMapper.toCelType(getMapField("struct_map")));
    }

    @Test
    public void shouldMapRepeatedWellKnownAndEnumFields() {
        assertEquals(ListType.create(SimpleType.TIMESTAMP),
                CelTypeMapper.toCelType(com.gotocompany.depot.TestNestedRepeatedMessage.getDescriptor().findFieldByName("repeated_timestamp")));
        assertEquals(ListType.create(SimpleType.DYN),
                CelTypeMapper.toCelType(com.gotocompany.depot.TestEnumMessage.getDescriptor().findFieldByName("status_history")));
    }
}
