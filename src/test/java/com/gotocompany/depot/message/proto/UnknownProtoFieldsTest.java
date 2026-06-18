package com.gotocompany.depot.message.proto;

import com.gotocompany.depot.TestMessage;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;


/**
 * Unit tests for {@link UnknownProtoFields}, which renders the unknown fields of an opaque Protobuf
 * byte payload as text.
 *
 * <p>The tests pass raw bytes to {@link UnknownProtoFields#toString(byte[])} and assert the textual
 * rendering: arbitrary non-proto bytes yield an empty string, whereas a serialized {@link TestMessage}
 * is rendered as its field-number-to-value text.</p>
 */
public class UnknownProtoFieldsTest {

    /**
     * Verifies that non-Protobuf bytes render as an empty string.
     *
     * <p>Given the arbitrary bytes of {@code "abcd"}, when {@link UnknownProtoFields#toString(byte[])}
     * is called, then it returns an empty string.</p>
     */
    @Test
    public void shouldGetEmptyStringWithWrongMessageBytes() {
        String out = UnknownProtoFields.toString("abcd".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals("", out);
    }

    /**
     * Verifies that serialized Protobuf bytes render their fields as numbered text.
     *
     * <p>Given a {@link TestMessage} with its order-details field (number {@code 3}) set to
     * {@code "test"}, when {@link UnknownProtoFields#toString(byte[])} is called on its serialized
     * bytes, then it returns {@code "3: \"test\"\n"}.</p>
     */
    @Test
    public void shouldGetUnknownFields() {
        TestMessage message = TestMessage.newBuilder().setOrderDetails("test").build();
        String out = UnknownProtoFields.toString(message.toByteArray());
        Assert.assertEquals("3: \"test\"\n", out);
    }
}
