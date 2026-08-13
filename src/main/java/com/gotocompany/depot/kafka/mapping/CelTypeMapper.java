package com.gotocompany.depot.kafka.mapping;

import com.google.protobuf.Descriptors;
import dev.cel.common.types.CelType;
import dev.cel.common.types.ListType;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.common.types.StructTypeReference;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Maps proto field descriptors to the CEL types used as the declared result type of mapping expressions.
 *
 * <p>The mapped type is supplied to the CEL compiler so that a mismatch between an expression result
 * type and its target proto field is rejected at startup, before any message is processed.
 */
public final class CelTypeMapper {

    private static final String TIMESTAMP_TYPE = "google.protobuf.Timestamp";
    private static final String DURATION_TYPE = "google.protobuf.Duration";
    private static final Set<String> DYN_MESSAGE_TYPES = new HashSet<>(Arrays.asList(
            "google.protobuf.Any",
            "google.protobuf.Struct",
            "google.protobuf.Value",
            "google.protobuf.ListValue",
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
     * Prevents instantiation of this utility class.
     */
    private CelTypeMapper() {
    }

    /**
     * Resolves the CEL type for a field, accounting for map, repeated and singular cardinalities.
     *
     * @param fieldDescriptor the proto field whose CEL type is required
     * @return the CEL type matching the field cardinality and value type
     */
    public static CelType toCelType(Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.isMapField()) {
            Descriptors.FieldDescriptor keyField = fieldDescriptor.getMessageType().findFieldByName("key");
            Descriptors.FieldDescriptor valueField = fieldDescriptor.getMessageType().findFieldByName("value");
            return MapType.create(toSingularCelType(keyField), toSingularCelType(valueField));
        }
        if (fieldDescriptor.isRepeated()) {
            return ListType.create(toSingularCelType(fieldDescriptor));
        }
        return toSingularCelType(fieldDescriptor);
    }

    /**
     * Resolves the CEL type for the singular value of a field, ignoring its cardinality.
     *
     * @param fieldDescriptor the proto field whose singular CEL type is required
     * @return the CEL type matching the field value type
     */
    public static CelType toSingularCelType(Descriptors.FieldDescriptor fieldDescriptor) {
        switch (fieldDescriptor.getJavaType()) {
            case INT:
            case LONG:
                return isUnsigned(fieldDescriptor) ? SimpleType.UINT : SimpleType.INT;
            case FLOAT:
            case DOUBLE:
                return SimpleType.DOUBLE;
            case BOOLEAN:
                return SimpleType.BOOL;
            case STRING:
                return SimpleType.STRING;
            case BYTE_STRING:
                return SimpleType.BYTES;
            case ENUM:
                return SimpleType.DYN;
            case MESSAGE:
                return toMessageCelType(fieldDescriptor.getMessageType());
            default:
                return SimpleType.DYN;
        }
    }

    /**
     * Resolves the CEL type for a message-typed field, mapping well known types to their CEL equivalents.
     *
     * @param messageDescriptor the descriptor of the message field type
     * @return the CEL type for the message, using a dynamic type for dynamically typed well known types
     */
    private static CelType toMessageCelType(Descriptors.Descriptor messageDescriptor) {
        String fullName = messageDescriptor.getFullName();
        if (TIMESTAMP_TYPE.equals(fullName)) {
            return SimpleType.TIMESTAMP;
        }
        if (DURATION_TYPE.equals(fullName)) {
            return SimpleType.DURATION;
        }
        if (DYN_MESSAGE_TYPES.contains(fullName)) {
            return SimpleType.DYN;
        }
        return StructTypeReference.create(fullName);
    }

    /**
     * Returns whether a field uses an unsigned proto integer type.
     *
     * @param fieldDescriptor the proto field to inspect
     * @return {@code true} if the field is an unsigned integer type, {@code false} otherwise
     */
    private static boolean isUnsigned(Descriptors.FieldDescriptor fieldDescriptor) {
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
}
