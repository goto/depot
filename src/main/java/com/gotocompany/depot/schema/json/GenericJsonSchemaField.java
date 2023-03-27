package com.gotocompany.depot.schema.json;

import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.SchemaFieldType;
import org.json.JSONArray;
import org.json.JSONObject;

public class GenericJsonSchemaField implements SchemaField {
    private final String name;

    private final SchemaFieldType type;

    private final boolean isRepeated;
    private final Object value;

    public GenericJsonSchemaField(String fieldName, Object value) {
        this.name = fieldName;
        this.type = getFieldType(value);
        this.value = value;
        this.isRepeated = value instanceof JSONArray;
    }

    @Override
    public String getName() {
        return name;
    }

    private SchemaFieldType getFieldType(Object value) {
        if (value instanceof JSONObject) {
            return SchemaFieldType.MESSAGE;
        } else if (value instanceof Double) {
            return SchemaFieldType.DOUBLE;
        } else if (value instanceof Integer) {
            return SchemaFieldType.INT;
        } else if (value instanceof Float) {
            return SchemaFieldType.FLOAT;
        } else if (value instanceof Long) {
            return SchemaFieldType.LONG;
        } else if (value == Boolean.TRUE || value == Boolean.FALSE) {
            return SchemaFieldType.BOOLEAN;
        }
        return SchemaFieldType.STRING;
    }

    @Override
    public SchemaFieldType getType() {
        return type;
    }

    @Override
    public Schema getValueType() {
        return new GenericJsonSchema((JSONObject) value);
    }

    @Override
    public boolean isRepeated() {
        return isRepeated;
    }
}
