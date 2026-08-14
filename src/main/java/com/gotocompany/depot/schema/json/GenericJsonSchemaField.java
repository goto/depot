package com.gotocompany.depot.schema.json;

import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.SchemaFieldType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigDecimal;

/**
 * {@link SchemaField} implementation describing a single field inferred from a JSON value.
 *
 * <p>Used by {@link GenericJsonSchema}, this field derives its {@link SchemaFieldType} from the
 * runtime type of the JSON value it is constructed with and retains that value so a nested object's
 * schema can be produced on demand. A field whose value is a {@link JSONArray} is treated as
 * repeated.</p>
 *
 * @see SchemaField
 * @see GenericJsonSchema
 */
public class GenericJsonSchemaField implements SchemaField {
    /**
     * The field's name, used both as its declared name and its JSON name.
     */
    private final String name;

    /**
     * The field's type, inferred from its value at construction time.
     */
    private final SchemaFieldType type;

    /**
     * Whether the field is repeated, that is, whether its value is a {@link JSONArray}.
     */
    private final boolean isRepeated;
    /**
     * Raw JSON value backing this field, retained so a nested {@link GenericJsonSchema} can be built
     * from it.
     */
    private final Object value;

    /**
     * Creates a schema field by inferring its type from the supplied JSON value.
     *
     * <p>The field's type is computed from {@code value}, and the field is marked repeated when
     * {@code value} is a {@link JSONArray}.</p>
     *
     * @param fieldName the name of the field
     * @param value the JSON value used to infer the field's type and to back nested-schema lookups
     */
    public GenericJsonSchemaField(String fieldName, Object value) {
        this.name = fieldName;
        this.type = getFieldType(value);
        this.value = value;
        this.isRepeated = value instanceof JSONArray;
    }

    /**
     * Returns the name of the field.
     *
     * @return the field's name
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns the JSON name of the field.
     *
     * <p>JSON fields use a single name, so this returns the same value as {@link #getName()}.</p>
     *
     * @return the field's name
     */
    @Override
    public String getJsonName() {
        return name;
    }

    /**
     * Infers the {@link SchemaFieldType} corresponding to a JSON value.
     *
     * <p>The mapping is: a {@link JSONObject} becomes {@link SchemaFieldType#MESSAGE}; a
     * {@link JSONArray} takes the type of its first element, or {@link SchemaFieldType#STRING} when
     * empty; a {@link BigDecimal} becomes {@link SchemaFieldType#DOUBLE}; an {@link Integer},
     * {@link Float} and {@link Long} map to {@link SchemaFieldType#INT}, {@link SchemaFieldType#FLOAT}
     * and {@link SchemaFieldType#LONG} respectively; a {@link Boolean} becomes
     * {@link SchemaFieldType#BOOLEAN}; and any other value defaults to
     * {@link SchemaFieldType#STRING}.</p>
     *
     * @param inputValue the JSON value whose type is to be inferred
     * @return the inferred {@link SchemaFieldType}
     */
    private SchemaFieldType getFieldType(Object inputValue) {
        if (inputValue instanceof JSONObject) {
            return SchemaFieldType.MESSAGE;
        } else if (inputValue instanceof JSONArray) {
            JSONArray jsonArray = (JSONArray) inputValue;
            return jsonArray.isEmpty() ? SchemaFieldType.STRING : getFieldType(jsonArray.get(0));
        } else if (inputValue instanceof BigDecimal) {
            return SchemaFieldType.DOUBLE;
        } else if (inputValue instanceof Integer) {
            return SchemaFieldType.INT;
        } else if (inputValue instanceof Float) {
            return SchemaFieldType.FLOAT;
        } else if (inputValue instanceof Long) {
            return SchemaFieldType.LONG;
        } else if (inputValue == Boolean.TRUE || inputValue == Boolean.FALSE) {
            return SchemaFieldType.BOOLEAN;
        }
        return SchemaFieldType.STRING;
    }

    /**
     * Returns the type inferred for this field at construction time.
     *
     * @return the field's type
     */
    @Override
    public SchemaFieldType getType() {
        return type;
    }

    /**
     * Returns the schema of this field's nested JSON object value.
     *
     * <p>Interprets this field's value as a nested JSON object and returns a {@link GenericJsonSchema}
     * describing it.</p>
     *
     * @return the schema of the nested JSON object value
     * @throws ClassCastException if this field's value is not a {@link JSONObject}
     */
    @Override
    public Schema getValueType() {
        return new GenericJsonSchema((JSONObject) value);
    }

    /**
     * Reports whether this field is repeated.
     *
     * @return {@code true} if this field's value is a {@link JSONArray}, {@code false} otherwise
     */
    @Override
    public boolean isRepeated() {
        return isRepeated;
    }
}
