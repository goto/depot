package com.gotocompany.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Factory that selects the appropriate {@link ProtoField} conversion strategy for a Protobuf field.
 *
 * <p>Given a field descriptor and its value, the factory evaluates a fixed, ordered list of candidate
 * strategies and returns the first whose {@link ProtoField#matches()} reports a match. The order is
 * significant because some categories overlap: the well-known message types (duration, timestamp,
 * struct) are tested before the generic {@link MessageProtoField}, ensuring they are handled by their
 * specialised strategies. When no candidate matches, a {@link DefaultProtoField} that returns the raw
 * value is used.</p>
 *
 * @see ProtoField
 * @see DefaultProtoField
 */
public class ProtoFieldFactory {

    /**
     * Selects the conversion strategy that applies to the given field and value.
     *
     * <p>The candidate strategies are evaluated in a fixed order: duration, timestamp, enum, struct,
     * float, integer and finally the generic message strategy. The first whose
     * {@link ProtoField#matches()} returns {@code true} is returned; if none match, a
     * {@link DefaultProtoField} wrapping the raw value is returned.</p>
     *
     * @param descriptor the descriptor of the field whose value is to be converted
     * @param fieldValue the raw field value (a scalar, a message, or a collection for repeated fields)
     * @return the matching {@link ProtoField} strategy, or a {@link DefaultProtoField} if none apply
     */
    public static ProtoField getField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        List<ProtoField> protoFields = Arrays.asList(
                new DurationProtoField(descriptor, fieldValue),
                new TimestampProtoField(descriptor, fieldValue),
                new EnumProtoField(descriptor, fieldValue),
                new StructProtoField(descriptor, fieldValue),
                new FloatProtoField(descriptor, fieldValue),
                new IntegerProtoField(descriptor, fieldValue),
                new MessageProtoField(descriptor, fieldValue)
        );
        Optional<ProtoField> first = protoFields
                .stream()
                .filter(ProtoField::matches)
                .findFirst();
        return first.orElseGet(() -> new DefaultProtoField(fieldValue));
    }
}
