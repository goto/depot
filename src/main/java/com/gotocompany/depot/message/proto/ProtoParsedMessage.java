package com.gotocompany.depot.message.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.message.ProtoUnknownFieldValidationType;
import com.jayway.jsonpath.Configuration;
import com.google.protobuf.Message;
import com.gotocompany.depot.exception.UnknownFieldsException;
import com.gotocompany.depot.message.LogicalValue;
import com.gotocompany.depot.message.MessageUtils;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.proto.ProtoSchema;
import com.gotocompany.depot.schema.proto.ProtoSchemaField;
import com.gotocompany.depot.utils.ProtoUtils;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link ParsedMessage} backed by a single Protobuf {@link Message} (typically a
 * {@link DynamicMessage}) produced by {@link ProtoMessageParser}.
 *
 * <p>This adapter exposes a decoded Protobuf message through Depot's schema-agnostic
 * {@link ParsedMessage} contract: it returns the raw message, renders it to JSON, extracts populated
 * fields (recursing into nested messages), looks up a field by JSONPath name, exposes the
 * {@link com.gotocompany.depot.schema.proto.ProtoSchema} and adapts the message to a
 * {@link LogicalValue}. It also validates the message for unknown fields according to a configurable
 * {@link ProtoUnknownFieldValidationType}.</p>
 *
 * <p>The class is annotated with Lombok's {@code @Slf4j} and logs the unknown-field contents when
 * validation fails.</p>
 *
 * @see ParsedMessage
 * @see ProtoMessageParser
 * @see ProtoLogicalValue
 */
@Slf4j
public class ProtoParsedMessage implements ParsedMessage {

    /**
     * The wrapped Protobuf message exposed through the {@link ParsedMessage} contract.
     */
    private final Message dynamicMessage;
    /**
     * JSONPath configuration, backed by a {@link ProtoJsonProvider}, used for field-by-name lookups
     * and propagated to nested parsed messages.
     */
    private final Configuration jsonPathConfig;

    /**
     * Creates a parsed-message view over a {@link DynamicMessage}.
     *
     * @param dynamicMessage the decoded dynamic message to wrap
     * @param jsonPathConfig the JSONPath configuration used for field-by-name lookups
     */
    public ProtoParsedMessage(DynamicMessage dynamicMessage, Configuration jsonPathConfig) {
        this.dynamicMessage = dynamicMessage;
        this.jsonPathConfig = jsonPathConfig;
    }

    /**
     * Creates a parsed-message view over any Protobuf {@link Message}.
     *
     * <p>This overload is used when wrapping nested message values that are exposed as the more
     * general {@link Message} type rather than as a concrete {@link DynamicMessage}.</p>
     *
     * @param dynamicMessage the Protobuf message to wrap
     * @param jsonPathConfig the JSONPath configuration used for field-by-name lookups
     */
    public ProtoParsedMessage(Message dynamicMessage, Configuration jsonPathConfig) {
        this.dynamicMessage = dynamicMessage;
        this.jsonPathConfig = jsonPathConfig;

    }

    /**
     * Returns the Protobuf text-format representation of the wrapped message.
     *
     * @return the wrapped message rendered through its own {@code toString()}
     */
    public String toString() {
        return dynamicMessage.toString();
    }

    /**
     * Returns the underlying Protobuf message without any conversion.
     *
     * @return the wrapped {@link Message} instance
     */
    @Override
    public Object getRaw() {
        return dynamicMessage;
    }

    /**
     * Renders the wrapped message as a {@link JSONObject} using the canonical Protobuf JSON mapping.
     *
     * @return a {@link JSONObject} representing the wrapped message
     * @throws DeserializerException if the message cannot be converted to JSON
     */
    @Override
    public JSONObject toJson() {
        String json;
        try {
            json = JsonFormat.printer().print(dynamicMessage);
        } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
            throw new DeserializerException(e.getMessage());
        }
        return new JSONObject(json);
    }

    /**
     * Validates that the wrapped message contains no unknown Protobuf fields.
     *
     * <p>The check is delegated to
     * {@link com.gotocompany.depot.utils.ProtoUtils#hasUnknownField} using the supplied strategy.
     * When unknown fields are present their decoded representation is logged at error level and an
     * exception is raised.</p>
     *
     * @param protoUnknownFieldValidationType the strategy controlling how deeply unknown fields are
     *     searched for within the message tree
     * @throws UnknownFieldsException if the message (or, depending on the strategy, a nested message)
     *     contains unknown fields
     */
    @Override
    public void validate(ProtoUnknownFieldValidationType protoUnknownFieldValidationType) {
        if (ProtoUtils.hasUnknownField(dynamicMessage, protoUnknownFieldValidationType)) {
            log.error("Unknown fields {}", UnknownProtoFields.toString(dynamicMessage.toByteArray()));
            throw new UnknownFieldsException(dynamicMessage);
        }
    }

    /**
     * Converts a single Protobuf field value into the representation exposed by {@link #getFields()}.
     *
     * <p>Enum values are rendered as their name, nested messages are wrapped in their own
     * {@link ProtoParsedMessage} so traversal can continue recursively, and all other values are
     * returned unchanged.</p>
     *
     * @param fd the descriptor of the field being converted
     * @param value the raw field value as returned by the Protobuf API
     * @return the converted value: a {@link String} for enums, a {@link ProtoParsedMessage} for
     *     nested messages, or {@code value} itself for scalars
     */
    private Object getProtoValue(Descriptors.FieldDescriptor fd, Object value) {
        switch (fd.getJavaType()) {
            case ENUM:
                return value.toString();
            case MESSAGE:
                return new ProtoParsedMessage((Message) value, jsonPathConfig);
            default:
                return value;
        }
    }

    /**
     * Returns the populated fields of the wrapped message keyed by their schema field.
     *
     * <p>Only fields that carry a value are included: a repeated field must be non-empty and a
     * singular field must have a non-empty string representation. Each retained field is keyed by a
     * {@link com.gotocompany.depot.schema.proto.ProtoSchemaField} and its value is converted with
     * {@link #getProtoValue(Descriptors.FieldDescriptor, Object)}; repeated fields are converted
     * element-wise into a list.</p>
     *
     * @return a map from each populated {@link SchemaField} to its converted value
     */
    @Override
    public Map<SchemaField, Object> getFields() {
        return dynamicMessage.getDescriptorForType().getFields().stream().filter(fd -> {
            Object value = dynamicMessage.getField(fd);
            if (value == null) {
                return false;
            }
            if (fd.isRepeated()) {
                return !((List<?>) value).isEmpty();
            }
            return !value.toString().isEmpty();
        }).collect(Collectors.toMap(ProtoSchemaField::new, fd -> {
            Object value = dynamicMessage.getField(fd);
            if (fd.isRepeated()) {
                return ((List<?>) value).stream().map(v -> getProtoValue(fd, v)).collect(Collectors.toList());
            }
            return getProtoValue(fd, value);
        }));
    }

    /**
     * Returns the value of a field addressed by a JSONPath-style name.
     *
     * <p>The lookup is delegated to {@link MessageUtils#getFieldFromJsonObject(String, Object,
     * Configuration)} using the wrapped message and the configured JSONPath provider.</p>
     *
     * @param name the field name, expressed as a JSONPath expression relative to the message root;
     *     must be non-{@code null} and non-empty
     * @return the value located at {@code name}
     * @throws IllegalArgumentException if {@code name} is {@code null} or empty, or if it does not
     *     resolve to a field in the message
     */
    public Object getFieldByName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Invalid field config : name can not be empty");
        }
        return MessageUtils.getFieldFromJsonObject(name, dynamicMessage, jsonPathConfig);
    }

    /**
     * Returns the schema describing the wrapped message.
     *
     * @return a {@link ProtoSchema} built from the message's descriptor
     */
    @Override
    public Schema getSchema() {
        return new ProtoSchema(dynamicMessage.getDescriptorForType());
    }

    /**
     * Adapts the wrapped message to a {@link LogicalValue} for well-known-type interpretation.
     *
     * @return a {@link ProtoLogicalValue} built from the wrapped message and its schema
     */
    @Override
    public LogicalValue getLogicalValue() {
        return new ProtoLogicalValue(dynamicMessage, getSchema());
    }
}
