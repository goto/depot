package com.gotocompany.depot.message.proto;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.StatusBQ;
import com.gotocompany.depot.TestMessageBQ;
import com.gotocompany.depot.TestNestedMessageBQ;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Test helper that builds Protobuf message fixtures and {@link ProtoField} instances for the proto
 * message tests.
 *
 * <p>{@link #generateTestMessage(java.time.Instant)} and
 * {@link #generateTestNestedMessage(String, TestMessageBQ)} assemble populated {@link TestMessageBQ}
 * and {@link TestNestedMessageBQ} protos, while the overloaded {@code createProtoField} factories
 * construct {@link ProtoField} descriptors with varying combinations of name, type name, type, label,
 * sub-fields and index. A static {@link #call} counter makes successive generated messages
 * distinguishable.</p>
 */
public class TestProtoUtil {
    /** Nanosecond component applied to the generated trip duration and interval durations. */
    public static final int TRIP_DURATION_NANOS = 1000;
    /** Seconds for the first generated interval duration. */
    private static final long TRIP_DURATION_SECONDS_1 = 12;
    /** Seconds for the second generated interval duration. */
    private static final long TRIP_DURATION_SECONDS_2 = 15;
    /** Fixed price assigned to every generated message. */
    private static final float PRICE = 12.12f;
    /** Invocation counter used to give each generated message unique field values. */
    private static int call = 0;

    /**
     * Builds a fully populated {@link TestMessageBQ} whose textual fields embed an incrementing
     * counter.
     *
     * <p>Sets the order number, URL and details (suffixed with the {@link #call} counter), the
     * created-at timestamp derived from {@code now}, a fixed price, a user token, a {@code COMPLETED}
     * status, a trip duration, two repeated updated-at timestamps and two interval durations.</p>
     *
     * @param now the instant used to populate the created-at timestamp
     * @return a populated {@link TestMessageBQ}
     */
    public static TestMessageBQ generateTestMessage(Instant now) {
        call++;
        Timestamp createdAt = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        return TestMessageBQ.newBuilder()
                .setOrderNumber("order-" + call)
                .setOrderUrl("order-url-" + call)
                .setOrderDetails("order-details-" + call)
                .setCreatedAt(createdAt)
                .setPrice(PRICE)
                .setUserToken(ByteString.copyFrom("test-token".getBytes()))
                .setStatus(StatusBQ.COMPLETED)
                .setTripDuration(Duration.newBuilder().setSeconds(1).setNanos(TRIP_DURATION_NANOS).build())
                .addUpdatedAt(createdAt)
                .addUpdatedAt(createdAt)
                .addIntervals(Duration.newBuilder().setSeconds(TRIP_DURATION_SECONDS_1).setNanos(TRIP_DURATION_NANOS).build())
                .addIntervals(Duration.newBuilder().setSeconds(TRIP_DURATION_SECONDS_2).setNanos(TRIP_DURATION_NANOS).build())
                .build();

    }

    /**
     * Wraps a message inside a {@link TestNestedMessageBQ} with the given nested id.
     *
     * @param nestedId the value for the nested id field
     * @param message the inner message set as the single message
     * @return a {@link TestNestedMessageBQ} containing {@code message} and {@code nestedId}
     */
    public static TestNestedMessageBQ generateTestNestedMessage(String nestedId, TestMessageBQ message) {
        return TestNestedMessageBQ.newBuilder()
                .setSingleMessage(message)
                .setNestedId(nestedId)
                .build();
    }

    /**
     * Creates a leaf {@link ProtoField} with a name, type and label and no sub-fields.
     *
     * @param name the field name
     * @param type the Protobuf field type
     * @param label the Protobuf field label (optional, required or repeated)
     * @return a {@link ProtoField} with an empty type name, no sub-fields and index {@code 0}
     */
    public static ProtoField createProtoField(String name, DescriptorProtos.FieldDescriptorProto.Type type, DescriptorProtos.FieldDescriptorProto.Label label) {
        return new ProtoField(name, "", type, label, new ArrayList<>(), 0);
    }

    /**
     * Creates a container {@link ProtoField} that only holds sub-fields.
     *
     * @param subFields the nested fields to attach
     * @return a {@link ProtoField} with empty name and type name, {@code null} type and label, the
     *     given sub-fields and index {@code 0}
     */
    public static ProtoField createProtoField(List<ProtoField> subFields) {
        return new ProtoField("", "", null, null, subFields, 0);
    }

    /**
     * Creates a leaf {@link ProtoField} with a name, type name, type and label and no sub-fields.
     *
     * @param name the field name
     * @param typeName the fully-qualified Protobuf type name
     * @param type the Protobuf field type
     * @param label the Protobuf field label
     * @return a {@link ProtoField} with no sub-fields and index {@code 0}
     */
    public static ProtoField createProtoField(String name, String typeName, DescriptorProtos.FieldDescriptorProto.Type type, DescriptorProtos.FieldDescriptorProto.Label label) {
        return new ProtoField(name, typeName, type, label, new ArrayList<>(), 0);
    }

    /**
     * Creates a {@link ProtoField} with a name, type name, type, label and nested fields.
     *
     * @param name the field name
     * @param typeName the fully-qualified Protobuf type name
     * @param type the Protobuf field type
     * @param label the Protobuf field label
     * @param fields the nested fields to attach
     * @return a {@link ProtoField} with the given sub-fields and index {@code 0}
     */
    public static ProtoField createProtoField(String name, String typeName, DescriptorProtos.FieldDescriptorProto.Type type, DescriptorProtos.FieldDescriptorProto.Label label, List<ProtoField> fields) {
        return new ProtoField(name, typeName, type, label, fields, 0);
    }

    /**
     * Creates a minimal {@link ProtoField} with only a name and an index.
     *
     * @param name the field name
     * @param index the field index
     * @return a {@link ProtoField} with empty type name, {@code null} type and label and no sub-fields
     */
    public static ProtoField createProtoField(String name, int index) {
        return new ProtoField(name, "", null, null, new ArrayList<>(), index);
    }

    /**
     * Creates a {@link ProtoField} with a name, type name, type, index and nested fields.
     *
     * @param name the field name
     * @param typeName the fully-qualified Protobuf type name
     * @param type the Protobuf field type
     * @param index the field index
     * @param fields the nested fields to attach
     * @return a {@link ProtoField} with a {@code null} label and the given sub-fields
     */
    public static ProtoField createProtoField(String name, String typeName, DescriptorProtos.FieldDescriptorProto.Type type, int index, List<ProtoField> fields) {
        return new ProtoField(name, typeName, type, null, fields, index);
    }
}
