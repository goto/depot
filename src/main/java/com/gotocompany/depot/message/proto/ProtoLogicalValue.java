package com.gotocompany.depot.message.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.message.LogicalValue;
import com.gotocompany.depot.schema.LogicalType;
import com.gotocompany.depot.schema.Schema;
import org.json.JSONObject;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Adapts a Protobuf {@link Message} that represents one of the well-known wrapper types into Depot's
 * {@link LogicalValue} abstraction.
 *
 * <p>A {@code LogicalValue} exposes a Protobuf value through a logical lens (timestamp, struct or
 * duration) that is decoupled from its concrete wire representation. This implementation lazily
 * reinterprets the wrapped message's serialized bytes as the requested well-known type whenever one
 * of the typed accessors is invoked, which allows a {@link com.google.protobuf.DynamicMessage}
 * carrying, for example, {@code google.protobuf.Timestamp} content to be read directly as an
 * {@link Instant}.</p>
 *
 * <p>The {@link Schema} supplied at construction determines the {@link LogicalType} reported by
 * {@link #getType()}; callers are expected to invoke only the accessor that matches that logical
 * type.</p>
 *
 * @see LogicalValue
 * @see ProtoParsedMessage#getLogicalValue()
 */
public class ProtoLogicalValue implements LogicalValue {
    /**
     * The Protobuf message whose serialized form is reinterpreted as a well-known type on demand.
     */
    private final Message message;

    /**
     * Schema describing the wrapped message, used to derive its {@link LogicalType}.
     */
    private final Schema schema;

    /**
     * Creates a logical-value view over the given Protobuf message.
     *
     * @param message the Protobuf message to adapt; its serialized bytes are reinterpreted on demand
     *     by the typed accessors
     * @param schema the schema describing {@code message}, used to report its {@link LogicalType}
     */
    public ProtoLogicalValue(Message message, Schema schema) {
        this.message = message;
        this.schema = schema;
    }

    /**
     * Returns the logical type of the wrapped value as declared by its schema.
     *
     * @return the {@link LogicalType} reported by the backing {@link Schema}
     */
    @Override
    public LogicalType getType() {
        return schema.logicalType();
    }

    /**
     * Reinterprets the wrapped message as a {@code google.protobuf.Timestamp} and converts it to an
     * {@link Instant}.
     *
     * <p>The message's serialized bytes are re-parsed as a {@link Timestamp}, and the resulting
     * seconds and nanoseconds are combined into an {@link Instant} through
     * {@link Instant#ofEpochSecond(long, long)}.</p>
     *
     * @return the timestamp value as an {@link Instant}
     * @throws DeserializerException if the wrapped message's bytes cannot be parsed as a
     *     {@link Timestamp}
     */
    @Override
    public Instant getTimestamp() {
        try {
            Timestamp timestamp = Timestamp.parseFrom(message.toByteArray());
            return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException(getErrMessage("Timestamp"), e);
        }
    }

    /**
     * Converts a single {@code google.protobuf.Value} into its plain Java representation.
     *
     * <p>The mapping is driven by the value's kind: booleans, numbers and strings are returned as
     * their Java equivalents; a nested struct is converted with {@link #getStructValue(Struct)}; a
     * list value is converted element-wise via recursion; and a null or unset value is represented
     * as {@link JSONObject#NULL}.</p>
     *
     * @param value the dynamically typed protobuf value to convert
     * @return the corresponding Java value, which may be a {@link Boolean}, {@link Double}, a
     *     {@link String}, a {@link Map} for a struct value, a {@link java.util.List} for a list
     *     value, or {@link JSONObject#NULL} for null and unrecognised kinds
     */
    private Object getValue(Value value) {
        switch (value.getKindCase()) {
            case BOOL_VALUE:
                return value.getBoolValue();
            case NUMBER_VALUE:
                return value.getNumberValue();
            case STRING_VALUE:
                return value.getStringValue();
            case STRUCT_VALUE:
                return getStructValue(value.getStructValue());
            case LIST_VALUE:
                return value.getListValue().getValuesList().stream().map(this::getValue).collect(Collectors.toList());
            default:
                return JSONObject.NULL;
        }
    }

    /**
     * Recursively converts a {@code google.protobuf.Struct} into a plain {@link Map}.
     *
     * <p>Each entry of the struct's field map is converted with {@link #getValue(Value)}, preserving
     * the original field names as map keys.</p>
     *
     * @param s the protobuf struct to convert
     * @return a map from field name to the converted Java value of each struct field
     */
    private Map<String, Object> getStructValue(Struct s) {
        Map<String, Value> fieldsMap = s.getFieldsMap();
        return fieldsMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> getValue(e.getValue())));
    }

    /**
     * Reinterprets the wrapped message as a {@code google.protobuf.Struct} and converts it to a map.
     *
     * <p>The message's serialized bytes are re-parsed as a {@link Struct} and then converted with
     * {@link #getStructValue(Struct)}, so nested structs and lists are converted recursively.</p>
     *
     * @return a map representation of the struct
     * @throws DeserializerException if the wrapped message's bytes cannot be parsed as a
     *     {@link Struct}
     */
    @Override
    public Map<String, Object> getStruct() {
        try {
            Struct s = Struct.parseFrom(message.toByteArray());
            return getStructValue(s);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException(getErrMessage("Struct"), e);
        }
    }

    /**
     * Reinterprets the wrapped message as a {@code google.protobuf.Duration} and converts it to a
     * {@link Duration}.
     *
     * <p>The message's serialized bytes are re-parsed as a {@link com.google.protobuf.Duration}, and
     * the resulting seconds and nanoseconds are combined into a {@link Duration} through
     * {@link Duration#ofSeconds(long, long)}.</p>
     *
     * @return the duration value as a {@link Duration}
     * @throws DeserializerException if the wrapped message's bytes cannot be parsed as a
     *     {@link com.google.protobuf.Duration}
     */
    @Override
    public Duration getDuration() {
        try {
            com.google.protobuf.Duration duration = com.google.protobuf.Duration.parseFrom(message.toByteArray());
            return Duration.ofSeconds(duration.getSeconds(), duration.getNanos());
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException(getErrMessage("Duration"), e);
        }
    }

    /**
     * Builds a descriptive error message for a failed deserialization attempt.
     *
     * @param type the human-readable name of the well-known type the message was being read as, for
     *     example {@code "Timestamp"}, {@code "Struct"} or {@code "Duration"}
     * @return a formatted message naming the wrapped message's fully qualified type and the target type
     */
    private String getErrMessage(String type) {
        return String.format("Failed while deserializing given \"%s\" to %s", message.getDescriptorForType().getFullName(), type);
    }
}
