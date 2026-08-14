package com.gotocompany.depot.message.proto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.List;

/**
 * Serialises a {@link ProtoField} schema tree into the JSON column-mapping format that maps Protobuf
 * field numbers to column names for schema-aware sinks.
 *
 * <p>The mapping is a nested JSON object keyed by field number ({@code index}). A scalar field maps
 * its number to its name, while a nested message maps its number to a child object that recursively
 * describes the message and carries the message's own name under
 * {@link Constants.Config#RECORD_NAME}. The class exposes only static helpers and is not
 * instantiated.</p>
 *
 * @see ProtoField
 * @see Constants.Config#RECORD_NAME
 */
public class ProtoMapper {

    /**
     * Generates the JSON column-mapping string for the given proto fields.
     *
     * @param fields the list of fields to serialise, typically the children of a parsed root
     *     {@link ProtoField}
     * @return the column mapping rendered as a JSON string
     * @throws IOException if the mapping cannot be serialised to JSON
     */
    public static String generateColumnMappings(List<ProtoField> fields) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = generateColumnMappingsJson(fields);
        return objectMapper.writeValueAsString(objectNode);
    }

    /**
     * Recursively builds the JSON object node representing the column mapping for the given fields.
     *
     * <p>An empty input yields an empty object. Otherwise each field contributes an entry keyed by its
     * field number: a nested message field contributes a child object built by recursion and augmented
     * with the field's name under {@link Constants.Config#RECORD_NAME}, whereas a scalar field
     * contributes its name directly.</p>
     *
     * @param fields the fields to serialise at this level
     * @return an {@link ObjectNode} mapping each field number to its name or nested object
     */
    private static ObjectNode generateColumnMappingsJson(List<ProtoField> fields) {
        if (fields.size() == 0) {
            return JsonNodeFactory.instance.objectNode();
        }

        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        for (ProtoField field : fields) {
            if (field.isNested()) {
                ObjectNode innerJSONValue = generateColumnMappingsJson(field.getFields());
                innerJSONValue.put(Constants.Config.RECORD_NAME, field.getName());
                objNode.set(String.valueOf(field.getIndex()), innerJSONValue);
            } else {
                objNode.put(String.valueOf(field.getIndex()), field.getName());
            }
        }
        return objNode;
    }
}
