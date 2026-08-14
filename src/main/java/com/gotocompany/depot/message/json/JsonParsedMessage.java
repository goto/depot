package com.gotocompany.depot.message.json;

import com.gotocompany.depot.message.LogicalValue;
import com.gotocompany.depot.message.MessageUtils;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.ProtoUnknownFieldValidationType;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.json.GenericJsonSchema;
import com.gotocompany.depot.schema.json.GenericJsonSchemaField;
import com.jayway.jsonpath.Configuration;
import org.json.JSONObject;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link ParsedMessage} backed by a JSON object, produced by {@link JsonMessageParser}.
 *
 * <p>This adapter exposes a parsed {@link JSONObject} through Depot's schema-agnostic
 * {@link ParsedMessage} contract: it returns the raw object, exposes it as JSON, lists its top-level
 * fields, looks up a field by JSONPath name and derives a {@link GenericJsonSchema}. JSON messages
 * carry no Protobuf unknown-field notion, so {@link #validate(ProtoUnknownFieldValidationType)} is a
 * no-op and {@link #getLogicalValue()} returns {@code null}.</p>
 *
 * @see ParsedMessage
 * @see JsonMessageParser
 */
public class JsonParsedMessage implements ParsedMessage {
    /**
     * The parsed JSON object exposed through the {@link ParsedMessage} contract.
     */
    private final JSONObject jsonObject;
    /**
     * JSONPath configuration used for field-by-name lookups against the wrapped object.
     */
    private final Configuration jsonPathConfig;
    /**
     * Schema derived from the wrapped JSON object's structure.
     */
    private final Schema schema;

    /**
     * Creates a parsed-message view over a JSON object.
     *
     * <p>A {@link GenericJsonSchema} is derived from the supplied object at construction time.</p>
     *
     * @param jsonObject the parsed JSON object to wrap
     * @param jsonPathConfig the JSONPath configuration used for field-by-name lookups
     */
    public JsonParsedMessage(JSONObject jsonObject, Configuration jsonPathConfig) {
        this.jsonObject = jsonObject;
        this.jsonPathConfig = jsonPathConfig;
        this.schema = new GenericJsonSchema(jsonObject);
    }

    /**
     * Returns the compact JSON string representation of the wrapped object.
     *
     * @return the wrapped object rendered via {@link JSONObject#toString()}
     */
    public String toString() {
        return jsonObject.toString();
    }

    /**
     * Returns the underlying JSON object without any conversion.
     *
     * @return the wrapped {@link JSONObject}
     */
    @Override
    public Object getRaw() {
        return jsonObject;
    }

    /**
     * Returns the wrapped object as JSON.
     *
     * @return the wrapped {@link JSONObject} itself
     */
    @Override
    public JSONObject toJson() {
        return jsonObject;
    }

    /**
     * Performs no validation, since JSON messages have no concept of Protobuf unknown fields.
     *
     * <p>This implementation runs no checks and never throws; it exists only to satisfy the
     * {@link ParsedMessage} contract.</p>
     *
     * @param protoUnknownFieldValidationType ignored by this implementation
     */
    public void validate(ProtoUnknownFieldValidationType protoUnknownFieldValidationType) { }

    /**
     * Returns the top-level fields of the JSON object keyed by their schema field.
     *
     * <p>Each key of the JSON object becomes a {@link GenericJsonSchemaField} capturing the key and
     * its value, mapped to that value.</p>
     *
     * @return a map from each top-level {@link SchemaField} to its value
     */
    @Override
    public Map<SchemaField, Object> getFields() {
        return jsonObject.keySet().stream().collect(Collectors.toMap(s -> new GenericJsonSchemaField(s, jsonObject.get(s)), jsonObject::get));
    }

    /**
     * Returns the value of a field addressed by a JSONPath-style name.
     *
     * <p>The lookup is delegated to {@link MessageUtils#getFieldFromJsonObject(String, Object,
     * Configuration)} using the wrapped object and the configured JSONPath provider.</p>
     *
     * @param name the field name, expressed as a JSONPath expression relative to the object root;
     *     must be non-{@code null} and non-empty
     * @return the value located at {@code name}
     * @throws IllegalArgumentException if {@code name} is {@code null} or empty, or if it does not
     *     resolve to a field in the object
     */
    public Object getFieldByName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Invalid field config : name can not be empty");
        }
        return MessageUtils.getFieldFromJsonObject(name, jsonObject, jsonPathConfig);
    }

    /**
     * Returns the schema describing the wrapped JSON object.
     *
     * @return the {@link GenericJsonSchema} derived at construction time
     */
    @Override
    public Schema getSchema() {
        return schema;
    }

    /**
     * Returns the logical value of this message, which is always {@code null} for JSON messages.
     *
     * <p>JSON messages do not support the well-known-type interpretation provided by
     * {@link LogicalValue}.</p>
     *
     * @return {@code null} always
     */
    @Override
    public LogicalValue getLogicalValue() {
        return null;
    }
}
