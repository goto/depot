package com.gotocompany.depot.message.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import com.gotocompany.depot.message.Message;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link ProtoMessageParser}, which deserializes Protobuf payloads into a
 * {@link ParsedMessage} for a configured message class.
 *
 * <p>The shared {@link #configMap} disables the Stencil schema registry and pins the proto message
 * class to {@code TestMessage}. Each test builds a parser from a {@link SinkConfig}, a mocked
 * {@link StatsDReporter} and a mocked {@link DepotStencilUpdateListener}, then parses key and message
 * payloads in {@link SinkConnectorSchemaMessageMode#LOG_KEY} and
 * {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE} modes. Coverage includes successful parsing, the
 * {@link InvalidProtocolBufferException} raised for malformed bytes and the error raised when no mode
 * is supplied.</p>
 */
public class ProtoMessageParserTest {

    /** Parser configuration: Stencil disabled, proto message class pinned to {@code TestMessage}. */
    private final HashMap<String, String> configMap = new HashMap<String, String>() {{
        put("SCHEMA_REGISTRY_STENCIL_ENABLE", "false");
        put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessage");
    }};

    /**
     * Verifies that a valid Protobuf payload is parsed in log-message mode.
     *
     * <p>Given a serialized {@link TestMessage}, when it is parsed in
     * {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE} mode for the configured class, then the raw
     * parsed message equals the original.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldParseLogMessage() throws IOException {
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        DepotStencilUpdateListener protoUpdateListener = mock(DepotStencilUpdateListener.class);
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("order-1").build();
        Message message = new Message(null, testMessage.toByteArray());
        ParsedMessage parsedMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestMessage");
        assertEquals(testMessage, parsedMessage.getRaw());

    }

    /**
     * Verifies that malformed message bytes fail to parse.
     *
     * <p>Given invalid bytes, when they are parsed in
     * {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE} mode, then an
     * {@link InvalidProtocolBufferException} is thrown.</p>
     */
    @Test
    public void shouldThrowErrorOnInvalidMessage() {
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        DepotStencilUpdateListener protoUpdateListener = mock(DepotStencilUpdateListener.class);
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        byte[] invalidMessageBytes = "invalid message".getBytes();
        Message message = new Message(null, invalidMessageBytes);
        assertThrows(InvalidProtocolBufferException.class, () -> {
            protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestMessage");
        });
    }

    /**
     * Verifies that a valid Protobuf payload is parsed in log-key mode.
     *
     * <p>Given a serialized {@link TestMessage} supplied as the key, when it is parsed in
     * {@link SinkConnectorSchemaMessageMode#LOG_KEY} mode, then the raw parsed message equals the
     * original.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldParseLogKey() throws IOException {
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        DepotStencilUpdateListener protoUpdateListener = mock(DepotStencilUpdateListener.class);
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        TestMessage testKey = TestMessage.newBuilder().setOrderNumber("order-1").build();
        Message message = new Message(testKey.toByteArray(), null);
        ParsedMessage parsedMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, "com.gotocompany.depot.TestMessage");
        assertEquals(testKey, parsedMessage.getRaw());

    }

    /**
     * Verifies that malformed key bytes fail to parse.
     *
     * <p>Given invalid key bytes, when they are parsed in
     * {@link SinkConnectorSchemaMessageMode#LOG_KEY} mode, then an
     * {@link InvalidProtocolBufferException} is thrown.</p>
     */
    @Test
    public void shouldThrowErrorOnInvalidKey() {
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        DepotStencilUpdateListener protoUpdateListener = mock(DepotStencilUpdateListener.class);
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        byte[] invalidKeyBytes = "invalid message".getBytes();
        Message message = new Message(invalidKeyBytes, null);
        assertThrows(InvalidProtocolBufferException.class, () -> {
            protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, "com.gotocompany.depot.TestMessage");
        });
    }

    /**
     * Verifies that parsing without a message mode is rejected.
     *
     * <p>Given a valid payload and a {@code null} mode, when it is parsed, then an {@link IOException}
     * with the message {@code "parser mode not defined"} is thrown.</p>
     */
    @Test
    public void shouldThrowErrorWhenModeNotDefined() {
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        DepotStencilUpdateListener protoUpdateListener = mock(DepotStencilUpdateListener.class);
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        byte[] validKeyBytes = TestMessage.newBuilder().setOrderNumber("order-1").build().toByteArray();
        Message message = new Message(validKeyBytes, null);
        IOException ioException = assertThrows(IOException.class, () -> {
            protoMessageParser.parse(message, null, null);
        });
        assertEquals("parser mode not defined", ioException.getMessage());
    }
}
