package com.gotocompany.depot.kafka.mapping;

import com.google.common.primitives.UnsignedInts;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.NullValue;
import com.gotocompany.depot.exception.ProtoMappingException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts CEL evaluation results into proto field values compatible with the target sink schema.
 *
 * <p>The converter validates the runtime type of each CEL value against its target field, handles map,
 * repeated and singular cardinalities, wraps primitives into well known wrapper messages and rebuilds
 * messages that share a full name but originate from a different descriptor instance.
 */
public class CelValueConverter {

    private static final String MAP_KEY_FIELD = "key";
    private static final String MAP_VALUE_FIELD = "value";
    private static final String WRAPPER_VALUE_FIELD = "value";
    private static final Set<String> WRAPPER_TYPES = new HashSet<>(Arrays.asList(
            "google.protobuf.BoolValue",
            "google.protobuf.BytesValue",
            "google.protobuf.DoubleValue",
            "google.protobuf.FloatValue",
            "google.protobuf.Int32Value",
            "google.protobuf.Int64Value",
            "google.protobuf.StringValue",
            "google.protobuf.UInt32Value",
            "google.protobuf.UInt64Value"));

    /**
     * Converts a CEL value into a proto field value for the given field.
     *
     * @param celValue        the value produced by the CEL evaluation
     * @param fieldDescriptor the target proto field
     * @return the converted proto field value, or {@code null} when the CEL value represents null
     * @throws ProtoMappingException if the value cannot be assigned to the field
     */
    public Object toFieldValue(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        if (isNullValue(celValue)) {
            return null;
        }
        if (fieldDescriptor.isMapField()) {
            return toMapFieldValue(celValue, fieldDescriptor);
        }
        if (fieldDescriptor.isRepeated()) {
            return toRepeatedFieldValue(celValue, fieldDescriptor);
        }
        return toSingularFieldValue(celValue, fieldDescriptor);
    }

    /**
     * Converts a CEL map value into a list of proto map entry messages.
     *
     * @param celValue        the CEL map value
     * @param fieldDescriptor the target map field
     * @return the list of map entry messages
     * @throws ProtoMappingException if the value is not a map or an entry cannot be converted
     */
    private Object toMapFieldValue(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        if (!(celValue instanceof Map)) {
            throw typeMismatch(celValue, fieldDescriptor);
        }
        Descriptors.Descriptor entryDescriptor = fieldDescriptor.getMessageType();
        Descriptors.FieldDescriptor keyField = entryDescriptor.findFieldByName(MAP_KEY_FIELD);
        Descriptors.FieldDescriptor valueField = entryDescriptor.findFieldByName(MAP_VALUE_FIELD);
        List<Message> entries = new ArrayList<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) celValue).entrySet()) {
            entries.add(DynamicMessage.newBuilder(entryDescriptor)
                    .setField(keyField, toSingularFieldValue(entry.getKey(), keyField))
                    .setField(valueField, toSingularFieldValue(entry.getValue(), valueField))
                    .build());
        }
        return entries;
    }

    /**
     * Converts a CEL list value into a list of proto values for a repeated field.
     *
     * @param celValue        the CEL list value
     * @param fieldDescriptor the target repeated field
     * @return the list of converted element values
     * @throws ProtoMappingException if the value is not a list or an element cannot be converted
     */
    private Object toRepeatedFieldValue(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        if (!(celValue instanceof List)) {
            throw typeMismatch(celValue, fieldDescriptor);
        }
        List<Object> values = new ArrayList<>();
        for (Object element : (List<?>) celValue) {
            values.add(toSingularFieldValue(element, fieldDescriptor));
        }
        return values;
    }

    /**
     * Converts a CEL value into the singular proto value for a field based on its java type.
     *
     * @param celValue        the CEL value
     * @param fieldDescriptor the target field
     * @return the converted singular value
     * @throws ProtoMappingException if the value cannot be assigned to the field
     */
    private Object toSingularFieldValue(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        switch (fieldDescriptor.getJavaType()) {
            case INT:
                return toIntValue(celValue, fieldDescriptor);
            case LONG:
                return toLongValue(celValue, fieldDescriptor);
            case FLOAT:
                return toFloatValue(celValue, fieldDescriptor);
            case DOUBLE:
                return toDoubleValue(celValue, fieldDescriptor);
            case BOOLEAN:
                return toBooleanValue(celValue, fieldDescriptor);
            case STRING:
                return toStringValue(celValue, fieldDescriptor);
            case BYTE_STRING:
                return toBytesValue(celValue, fieldDescriptor);
            case ENUM:
                return toEnumValue(celValue, fieldDescriptor);
            case MESSAGE:
                return toMessageValue(celValue, fieldDescriptor);
            default:
                throw typeMismatch(celValue, fieldDescriptor);
        }
    }

    /**
     * Converts a CEL number into a proto 32-bit integer value, validating the supported range.
     *
     * @param celValue        the CEL value
     * @param fieldDescriptor the target 32-bit integer field
     * @return the converted integer value
     * @throws ProtoMappingException if the value is not a number or is out of range for the field
     */
    private Object toIntValue(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        if (!(celValue instanceof Number)) {
            throw typeMismatch(celValue, fieldDescriptor);
        }
        long longValue = ((Number) celValue).longValue();
        try {
            if (isUnsigned(fieldDescriptor)) {
                return UnsignedInts.checkedCast(longValue);
            }
            return Math.toIntExact(longValue);
        } catch (ArithmeticException | IllegalArgumentException e) {
            throw new ProtoMappingException(
                    String.format("value %s is out of range for the field %s", celValue, fieldDescriptor.getFullName()), e);
        }
    }

    /**
     * Converts a CEL number into a proto 64-bit integer value.
     *
     * @param celValue        the CEL value
     * @param fieldDescriptor the target 64-bit integer field
     * @return the converted long value
     * @throws ProtoMappingException if the value is not a number
     */
    private Object toLongValue(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        if (!(celValue instanceof Number)) {
            throw typeMismatch(celValue, fieldDescriptor);
        }
        return ((Number) celValue).longValue();
    }

    /**
     * Converts a CEL number into a proto float value.
     *
     * @param celValue        the CEL value
     * @param fieldDescriptor the target float field
     * @return the converted float value
     * @throws ProtoMappingException if the value is not a number
     */
    private Object toFloatValue(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        if (!(celValue instanceof Number)) {
            throw typeMismatch(celValue, fieldDescriptor);
        }
        return ((Number) celValue).floatValue();
    }

    /**
     * Converts a CEL number into a proto double value.
     *
     * @param celValue        the CEL value
     * @param fieldDescriptor the target double field
     * @return the converted double value
     * @throws ProtoMappingException if the value is not a number
     */
    private Object toDoubleValue(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        if (!(celValue instanceof Number)) {
            throw typeMismatch(celValue, fieldDescriptor);
        }
        return ((Number) celValue).doubleValue();
    }

    /**
     * Converts a CEL boolean into a proto boolean value.
     *
     * @param celValue        the CEL value
     * @param fieldDescriptor the target boolean field
     * @return the converted boolean value
     * @throws ProtoMappingException if the value is not a boolean
     */
    private Object toBooleanValue(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        if (!(celValue instanceof Boolean)) {
            throw typeMismatch(celValue, fieldDescriptor);
        }
        return celValue;
    }

    /**
     * Converts a CEL string into a proto string value.
     *
     * @param celValue        the CEL value
     * @param fieldDescriptor the target string field
     * @return the converted string value
     * @throws ProtoMappingException if the value is not a string
     */
    private Object toStringValue(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        if (!(celValue instanceof String)) {
            throw typeMismatch(celValue, fieldDescriptor);
        }
        return celValue;
    }

    /**
     * Converts a CEL bytes value into a proto bytes value.
     *
     * @param celValue        the CEL value
     * @param fieldDescriptor the target bytes field
     * @return the converted bytes value
     * @throws ProtoMappingException if the value is not a bytes value
     */
    private Object toBytesValue(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        if (!(celValue instanceof ByteString)) {
            throw typeMismatch(celValue, fieldDescriptor);
        }
        return celValue;
    }

    /**
     * Converts a CEL value into a proto enum value, accepting an enum descriptor, a number or a name.
     *
     * @param celValue        the CEL value
     * @param fieldDescriptor the target enum field
     * @return the resolved enum value descriptor
     * @throws ProtoMappingException if the value type is unsupported or does not resolve to a valid enum value
     */
    private Object toEnumValue(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        Descriptors.EnumDescriptor enumDescriptor = fieldDescriptor.getEnumType();
        Descriptors.EnumValueDescriptor enumValue = null;
        if (celValue instanceof Descriptors.EnumValueDescriptor) {
            enumValue = enumDescriptor.findValueByNumber(((Descriptors.EnumValueDescriptor) celValue).getNumber());
        } else if (celValue instanceof Number) {
            enumValue = enumDescriptor.findValueByNumber(((Number) celValue).intValue());
        } else if (celValue instanceof String) {
            enumValue = enumDescriptor.findValueByName((String) celValue);
        } else {
            throw typeMismatch(celValue, fieldDescriptor);
        }
        if (enumValue == null) {
            throw new ProtoMappingException(
                    String.format("value %s is not a valid enum value for the field %s", celValue, fieldDescriptor.getFullName()));
        }
        return enumValue;
    }

    /**
     * Converts a CEL value into a proto message value, rebuilding or wrapping it where required.
     *
     * @param celValue        the CEL value
     * @param fieldDescriptor the target message field
     * @return the message assignable to the field, either passed through, rebuilt or wrapped
     * @throws ProtoMappingException if the message type does not match and cannot be wrapped
     */
    private Object toMessageValue(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        Descriptors.Descriptor targetDescriptor = fieldDescriptor.getMessageType();
        if (celValue instanceof Message) {
            Message message = (Message) celValue;
            if (message.getDescriptorForType() == targetDescriptor) {
                return message;
            }
            if (message.getDescriptorForType().getFullName().equals(targetDescriptor.getFullName())) {
                return rebuildMessage(message, targetDescriptor, fieldDescriptor);
            }
            throw new ProtoMappingException(
                    String.format("message of type %s can not be assigned to the field %s of type %s",
                            message.getDescriptorForType().getFullName(), fieldDescriptor.getFullName(), targetDescriptor.getFullName()));
        }
        if (WRAPPER_TYPES.contains(targetDescriptor.getFullName())) {
            Descriptors.FieldDescriptor wrapperValueField = targetDescriptor.findFieldByName(WRAPPER_VALUE_FIELD);
            return DynamicMessage.newBuilder(targetDescriptor)
                    .setField(wrapperValueField, toSingularFieldValue(celValue, wrapperValueField))
                    .build();
        }
        throw typeMismatch(celValue, fieldDescriptor);
    }

    /**
     * Rebuilds a message under the target descriptor instance by re-parsing its serialized bytes.
     *
     * @param message         the source message with a matching full name but a different descriptor instance
     * @param targetDescriptor the descriptor instance the field expects
     * @param fieldDescriptor  the target message field
     * @return the message rebuilt under the target descriptor
     * @throws ProtoMappingException if the message bytes cannot be parsed under the target descriptor
     */
    private Message rebuildMessage(Message message, Descriptors.Descriptor targetDescriptor, Descriptors.FieldDescriptor fieldDescriptor) {
        try {
            return DynamicMessage.parseFrom(targetDescriptor, message.toByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new ProtoMappingException(
                    String.format("failed to convert message of type %s for the field %s",
                            targetDescriptor.getFullName(), fieldDescriptor.getFullName()), e);
        }
    }

    /**
     * Returns whether a CEL value represents a null value.
     *
     * @param celValue the CEL value to inspect
     * @return {@code true} if the value is a java null or a proto null value, {@code false} otherwise
     */
    private boolean isNullValue(Object celValue) {
        return celValue == null || celValue instanceof NullValue;
    }

    /**
     * Returns whether a field uses an unsigned proto integer type.
     *
     * @param fieldDescriptor the proto field to inspect
     * @return {@code true} if the field is an unsigned integer type, {@code false} otherwise
     */
    private boolean isUnsigned(Descriptors.FieldDescriptor fieldDescriptor) {
        switch (fieldDescriptor.getType()) {
            case UINT32:
            case UINT64:
            case FIXED32:
            case FIXED64:
                return true;
            default:
                return false;
        }
    }

    /**
     * Builds a mapping exception describing why a CEL value cannot be assigned to a field.
     *
     * @param celValue        the offending CEL value
     * @param fieldDescriptor the target field
     * @return the constructed mapping exception
     */
    private ProtoMappingException typeMismatch(Object celValue, Descriptors.FieldDescriptor fieldDescriptor) {
        return new ProtoMappingException(
                String.format("value of type %s can not be assigned to the field %s of type %s",
                        celValue == null ? "null" : celValue.getClass().getName(),
                        fieldDescriptor.getFullName(),
                        fieldDescriptor.getJavaType().name()));
    }
}
