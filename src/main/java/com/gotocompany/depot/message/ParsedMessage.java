package com.gotocompany.depot.message;

import org.json.JSONObject;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;

import java.util.Map;

/**
 * Format-agnostic, decoded view of one portion (key or value) of a {@link Message}.
 *
 * <p>A {@code ParsedMessage} is produced by a {@link MessageParser} and shields the rest of a sink
 * from the concrete payload encoding. Whether the underlying record was Protobuf or JSON, callers can
 * uniformly obtain its {@link Schema}, enumerate its populated fields, look up individual fields by
 * name, render it as JSON and adapt it to a {@link LogicalValue}. Implementations include the
 * Protobuf-backed and JSON-backed parsed messages.</p>
 *
 * @see MessageParser
 * @see Schema
 * @see LogicalValue
 */
public interface ParsedMessage {
    /**
     * Returns the underlying decoded payload without further conversion.
     *
     * <p>The concrete type depends on the implementation, for example a Protobuf
     * {@link com.google.protobuf.Message} or a {@link JSONObject}.</p>
     *
     * @return the raw decoded payload object backing this parsed message
     */
    Object getRaw();

    /**
     * Renders this message as a {@link JSONObject}.
     *
     * @return a JSON representation of the decoded payload
     */
    JSONObject toJson();

    /**
     * Validates the decoded payload, rejecting messages that carry unknown fields.
     *
     * <p>The supplied {@link ProtoUnknownFieldValidationType} controls how deeply a Protobuf message
     * tree is searched for fields that are not present in the schema. Implementations for formats that
     * cannot carry unknown fields may treat this as a no-op.</p>
     *
     * @param validationType the strategy controlling how unknown fields are searched for
     * @throws com.gotocompany.depot.exception.UnknownFieldsException if the payload contains unknown
     *     fields according to the given strategy
     */
    void validate(ProtoUnknownFieldValidationType validationType);

    /**
     * Returns the populated fields of the message keyed by their schema definition.
     *
     * <p>Only fields that actually carry a value are included; the value associated with each
     * {@link SchemaField} is the decoded field value, with nested messages and repeated fields
     * represented according to the implementation.</p>
     *
     * @return a map from each populated {@link SchemaField} to its decoded value
     */
    Map<SchemaField, Object> getFields();

    /**
     * Returns the value of a single field addressed by name.
     *
     * @param name the field name, interpreted as a JSONPath expression relative to the message root
     * @return the value located at {@code name}
     */
    Object getFieldByName(String name);

    /**
     * Returns the schema describing this message.
     *
     * @return the {@link Schema} of the decoded payload
     */
    Schema getSchema();

    /**
     * Adapts this message to a {@link LogicalValue} for well-known-type interpretation.
     *
     * @return a {@link LogicalValue} view of the decoded payload
     */
    LogicalValue getLogicalValue();
}
