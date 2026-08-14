package com.gotocompany.depot.message.proto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link ProtoMapper}, which generates the JSON column mapping (field index to column
 * name) consumed by the BigQuery proto schema.
 *
 * <p>Each test assembles {@link ProtoField} trees through {@link TestProtoUtil} and compares the JSON
 * produced by {@link ProtoMapper#generateColumnMappings(java.util.List)} against an expected
 * {@code ObjectNode} serialized with a Jackson {@link ObjectMapper}. Coverage spans a flat
 * first-level mapping, a nested mapping (including the {@code record_name} marker for the nested
 * message) and the empty-field case.</p>
 */
public class ProtoMapperTest {

    /** Jackson mapper used to serialize the expected mapping for comparison. */
    private final ObjectMapper objectMapper = new ObjectMapper();


    /**
     * Verifies that a flat list of fields maps to a JSON object of index to column name.
     *
     * <p>Given five top-level fields, when
     * {@link ProtoMapper#generateColumnMappings(java.util.List)} is called, then the JSON maps each
     * field index ({@code 1} through {@code 5}) to its name, matching the expected serialized
     * object.</p>
     *
     * @throws IOException if serializing the expected mapping fails
     */
    @Test
    public void shouldTestShouldCreateFirstLevelColumnMappingSuccessfully() throws IOException {
        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("order_number", 1));
            add(TestProtoUtil.createProtoField("order_url", 2));
            add(TestProtoUtil.createProtoField("order_details", 3));
            add(TestProtoUtil.createProtoField("created_at", 4));
            add(TestProtoUtil.createProtoField("status", 5));
        }});

        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");
        objNode.put("3", "order_details");
        objNode.put("4", "created_at");
        objNode.put("5", "status");

        String columnMapping = ProtoMapper.generateColumnMappings(protoField.getFields());

        String expectedProtoMapping = objectMapper.writeValueAsString(objNode);
        assertEquals(expectedProtoMapping, columnMapping);
    }

    /**
     * Verifies that a nested message field produces a nested mapping with a record-name marker.
     *
     * <p>Given a field whose second entry is a message with sub-fields {@code host} and {@code url},
     * when {@link ProtoMapper#generateColumnMappings(java.util.List)} is called, then index {@code 2}
     * maps to a nested object holding the sub-field indices and a {@code record_name} entry naming the
     * message, alongside the flat top-level entries.</p>
     *
     * @throws IOException if serializing the expected mapping fails
     */
    @Test
    public void shouldTestShouldCreateNestedMapping() throws IOException {
        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("order_number", 1));
            add(TestProtoUtil.createProtoField("order_url", "some.type.name", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, 2, new ArrayList<ProtoField>() {{
                add(TestProtoUtil.createProtoField("host", 1));
                add(TestProtoUtil.createProtoField("url", 2));
            }}));
            add(TestProtoUtil.createProtoField("order_details", 3));
        }});

        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        ObjectNode innerObjNode = JsonNodeFactory.instance.objectNode();
        innerObjNode.put("1", "host");
        innerObjNode.put("2", "url");
        innerObjNode.put("record_name", "order_url");
        objNode.put("1", "order_number");
        objNode.put("2", innerObjNode);
        objNode.put("3", "order_details");


        String columnMapping = ProtoMapper.generateColumnMappings(protoField.getFields());
        String expectedProtoMapping = objectMapper.writeValueAsString(objNode);
        assertEquals(expectedProtoMapping, columnMapping);
    }

    /**
     * Verifies that an empty field list maps to an empty JSON object.
     *
     * <p>Given no fields, when {@link ProtoMapper#generateColumnMappings(java.util.List)} is called,
     * then it returns {@code "{}"}.</p>
     *
     * @throws IOException if mapping generation fails
     */
    @Test
    public void generateColumnMappingsForNoFields() throws IOException {
        String protoMapping = ProtoMapper.generateColumnMappings(new ArrayList<>());
        assertEquals(protoMapping, "{}");
    }
}
