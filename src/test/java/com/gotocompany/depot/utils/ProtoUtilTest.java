package com.gotocompany.depot.utils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestLocation;
import com.gotocompany.depot.message.ProtoUnknownFieldValidationType;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link ProtoUtils#hasUnknownField(com.google.protobuf.Message,
 * ProtoUnknownFieldValidationType)}, covering detection of Protobuf unknown fields.
 *
 * <p>Each test builds a {@link DynamicMessage} from the {@link TestBookingLogMessage} and
 * {@link TestLocation} descriptors, optionally attaching an {@link UnknownFieldSet} at the root or on
 * a nested message, and asserts whether the {@link ProtoUnknownFieldValidationType#MESSAGE}
 * traversal reports unknown fields.
 */
public class ProtoUtilTest {
    /**
     * Verifies that unknown fields on the root message are detected.
     *
     * <p>Builds a {@link DynamicMessage} for {@link TestBookingLogMessage} with a nested location and
     * an {@link UnknownFieldSet} attached at the root, then asserts {@code hasUnknownField} returns
     * {@code true}.
     */
    @Test
    public void shouldReturnTrueWhenUnknownFieldsExistOnRootLevelFields() {
        Descriptors.Descriptor bookingLogMessage = TestBookingLogMessage.getDescriptor();
        Descriptors.Descriptor location = TestLocation.getDescriptor();

        Descriptors.FieldDescriptor fieldDescriptor = bookingLogMessage.findFieldByName("driver_pickup_location");
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(bookingLogMessage)
                .setField(fieldDescriptor, DynamicMessage.newBuilder(location)
                        .build())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .addField(2, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();

        boolean unknownFieldExist = ProtoUtils.hasUnknownField(dynamicMessage, ProtoUnknownFieldValidationType.MESSAGE);
        assertTrue(unknownFieldExist);
    }

    /**
     * Verifies that unknown fields on a nested message are detected.
     *
     * <p>Builds a {@link DynamicMessage} whose nested {@link TestLocation} carries an
     * {@link UnknownFieldSet} while the root has none, then asserts {@code hasUnknownField} returns
     * {@code true}, confirming the traversal descends into child messages.
     */
    @Test
    public void shouldReturnTrueWhenUnknownFieldsExistOnNestedChildFields() {
        Descriptors.Descriptor bookingLogMessage = TestBookingLogMessage.getDescriptor();
        Descriptors.Descriptor location = TestLocation.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = bookingLogMessage.findFieldByName("driver_pickup_location");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(bookingLogMessage)
                .setField(fieldDescriptor, DynamicMessage.newBuilder(location)
                        .setUnknownFields(UnknownFieldSet.newBuilder()
                                .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                                .addField(2, UnknownFieldSet.Field.getDefaultInstance())
                                .build())
                        .build())
                .build();

        boolean unknownFieldExist = ProtoUtils.hasUnknownField(dynamicMessage, ProtoUnknownFieldValidationType.MESSAGE);
        assertTrue(unknownFieldExist);
    }

    /**
     * Verifies that a message free of unknown fields is reported as clean.
     *
     * <p>Builds a {@link DynamicMessage} with a nested location but no unknown fields anywhere, then
     * asserts {@code hasUnknownField} returns {@code false}.
     */
    @Test
    public void shouldReturnFalseWhenNoUnknownFieldsExist() {
        Descriptors.Descriptor bookingLogMessage = TestBookingLogMessage.getDescriptor();
        Descriptors.Descriptor location = TestLocation.getDescriptor();

        Descriptors.FieldDescriptor fieldDescriptor = bookingLogMessage.findFieldByName("driver_pickup_location");
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(bookingLogMessage)
                .setField(fieldDescriptor, DynamicMessage.newBuilder(location).build())
                .build();

        boolean unknownFieldExist = ProtoUtils.hasUnknownField(dynamicMessage, ProtoUnknownFieldValidationType.MESSAGE);
        assertFalse(unknownFieldExist);
    }

    /**
     * Verifies that a {@code null} root message is reported as having no unknown fields.
     *
     * <p>Asserts {@code hasUnknownField(null, ...)} returns {@code false} rather than throwing.
     */
    @Test
    public void shouldReturnFalseWhenRootIsNull() {
        boolean unknownFieldExist = ProtoUtils.hasUnknownField(null, ProtoUnknownFieldValidationType.MESSAGE);
        assertFalse(unknownFieldExist);
    }
}

