package com.gotocompany.depot.bigquery.models;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.depot.message.proto.ProtoField;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link ProtoField}, the lightweight wrapper around a Protobuf
 * {@link DescriptorProtos.FieldDescriptorProto} used while building BigQuery schemas.
 *
 * <p>The tests assert how {@link ProtoField} classifies fields as nested or scalar (notably treating
 * the well-known timestamp and struct types as non-nested) and how it renders its recursive
 * {@code toString} representation, including for the empty default instance.</p>
 */
public class ProtoFieldTest {
    /**
     * Verifies that a generic message-typed field is reported as nested.
     *
     * <p>Given the {@code duration_value} field (a message type) wrapped in a {@link ProtoField}, when
     * {@code isNested()} is queried, then it returns {@code true}.</p>
     */
    @Test
    public void shouldReturnNestedAsTrueWhenProtobufFieldTypeIsAMessage() {
        DescriptorProtos.FieldDescriptorProto fieldDescriptorProto = TestTypesMessage.getDescriptor().findFieldByName("duration_value").toProto();
        ProtoField protoField = new ProtoField(fieldDescriptorProto);

        assertTrue(protoField.isNested());
    }

    /**
     * Verifies that the well-known timestamp message type is not treated as nested.
     *
     * <p>Given the {@code timestamp_value} field wrapped in a {@link ProtoField}, when
     * {@code isNested()} is queried, then it returns {@code false}, since timestamps map to a scalar
     * BigQuery column.</p>
     */
    @Test
    public void shouldReturnNestedAsFalseWhenProtobufFieldTypeIsTimestamp() {
        DescriptorProtos.FieldDescriptorProto fieldDescriptorProto = TestTypesMessage.getDescriptor().findFieldByName("timestamp_value").toProto();
        ProtoField protoField = new ProtoField(fieldDescriptorProto);

        assertFalse(protoField.isNested());
    }

    /**
     * Verifies that the well-known struct message type is not treated as nested.
     *
     * <p>Given the {@code struct_value} field wrapped in a {@link ProtoField}, when {@code isNested()}
     * is queried, then it returns {@code false}, since structs are serialized to a scalar string
     * column.</p>
     */
    @Test
    public void shouldReturnNestedAsFalseWhenProtobufFieldTypeIsStruct() {
        DescriptorProtos.FieldDescriptorProto fieldDescriptorProto = TestTypesMessage.getDescriptor().findFieldByName("struct_value").toProto();
        ProtoField protoField = new ProtoField(fieldDescriptorProto);

        assertFalse(protoField.isNested());
    }

    /**
     * Verifies that a scalar-mapped field is not reported as nested.
     *
     * <p>The test resolves the {@code timestamp_value} field (a scalar-mapped well-known type) and
     * asserts that {@code isNested()} returns {@code false}.</p>
     */
    @Test
    public void shouldReturnNestedAsFalseWhenProtobufFieldIsScalarValueTypes() {
        DescriptorProtos.FieldDescriptorProto fieldDescriptorProto = TestTypesMessage.getDescriptor().findFieldByName("timestamp_value").toProto();
        ProtoField protoField = new ProtoField(fieldDescriptorProto);

        assertFalse(protoField.isNested());
    }

    /**
     * Verifies the recursive {@code toString} rendering of a populated {@link ProtoField}.
     *
     * <p>Given the {@code message_value} field with its three child string fields appended, when
     * {@code toString()} is invoked, then it returns the nested representation listing the field name,
     * type, child count and the formatted nested fields.</p>
     */
    @Test
    public void shouldReturnProtoFieldString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestTypesMessage.getDescriptor().findFieldByName("message_value");
        DescriptorProtos.FieldDescriptorProto fieldDescriptorProto = fieldDescriptor.toProto();
        ProtoField protoField = new ProtoField(fieldDescriptorProto);

        List<Descriptors.FieldDescriptor> childFields = fieldDescriptor.getMessageType().getFields();
        List<ProtoField> fieldList = childFields.stream().map(fd -> new ProtoField(fd.toProto())).collect(Collectors.toList());
        fieldList.forEach(pf ->
                protoField.addField(pf));

        String protoString = protoField.toString();

        assertEquals("{name='message_value', type=TYPE_MESSAGE, len=3, nested=["
                + "{name='order_number', type=TYPE_STRING, len=0, nested=[]}, "
                + "{name='order_url', type=TYPE_STRING, len=0, nested=[]}, "
                + "{name='order_details', type=TYPE_STRING, len=0, nested=[]}]}", protoString);
    }


    /**
     * Verifies the {@code toString} rendering of an empty default {@link ProtoField}.
     *
     * <p>Given a no-argument {@link ProtoField}, when {@code toString()} is invoked, then it returns
     * {@code "{name='null', type=null, len=0, nested=[]}"}.</p>
     */
    @Test
    public void shouldReturnEmptyProtoFieldString() {
        String protoString = new ProtoField().toString();

        assertEquals("{name='null', type=null, len=0, nested=[]}", protoString);
    }
}
