package com.gotocompany.depot.schema.json;

import com.gotocompany.depot.schema.LogicalType;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import org.json.JSONObject;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Schema interface implementation from JSONObject.
 * This implementation derives schema from JSONObject by type checking the JSON values.
 *
 * <p>Each top-level entry of the document becomes a {@link GenericJsonSchemaField}, and the type of
 * every field is determined by inspecting its value. Because the schema is read from concrete data,
 * the document acts as both the value and its own type description.</p>
 *
 * @see Schema
 * @see GenericJsonSchemaField
 */
public class GenericJsonSchema implements Schema {
    /**
     * JSON document from which this schema is inferred.
     */
    private final JSONObject json;

    /**
     * Creates a schema that infers its structure from the given JSON document.
     *
     * @param json the JSON object whose structure defines this schema
     */
    public GenericJsonSchema(JSONObject json) {
        this.json = json;
    }

    /**
     * Returns the name of the type described by this schema.
     *
     * <p>JSON documents have no declared type name, so the constant {@code "root"} is always
     * returned.</p>
     *
     * @return the literal name {@code "root"}
     */
    @Override
    public String getFullName() {
        return "root";
    }

    /**
     * Returns the inferred fields of the JSON document.
     *
     * <p>Each key present in the document is turned into a {@link GenericJsonSchemaField} whose type is
     * inferred from the corresponding value.</p>
     *
     * @return the inferred fields of the JSON document
     */
    @Override
    public List<SchemaField> getFields() {
        return json.keySet().stream().map(s -> new GenericJsonSchemaField(s, json.get(s))).collect(Collectors.toList());
    }

    /**
     * Returns the field declared under the given name.
     *
     * <p>Reads the value stored under {@code name} from the JSON document and wraps it in a
     * {@link GenericJsonSchemaField} whose type is inferred from that value.</p>
     *
     * @param name the name of the field to look up
     * @return a {@link GenericJsonSchemaField} for the requested field
     * @throws org.json.JSONException if the document has no entry with the given name
     */
    @Override
    public SchemaField getFieldByName(String name) {
        Object value = json.get(name);
        return new GenericJsonSchemaField(name, value);
    }

    /**
     * Returns the logical type of this schema.
     *
     * <p>A JSON document is always treated as an ordinary message, so {@link LogicalType#MESSAGE} is
     * returned.</p>
     *
     * @return {@link LogicalType#MESSAGE}
     */
    @Override
    public LogicalType logicalType() {
        return LogicalType.MESSAGE;
    }
}
