package com.gotocompany.depot.http.request.body;

import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.json.JsonMessageParser;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MessageBody}, the {@link RequestBody} implementation that serialises only the
 * parsed log message to JSON (without wrapping it under separate key/message fields).
 *
 * <p>Real {@link ProtoMessageParser} and {@link JsonMessageParser} instances obtained via
 * {@link MessageParserFactory} parse the inputs, while the {@link MessageContainer} and
 * {@link StatsDReporter} are Mockito mocks. The rendered body is compared to an expected JSON string
 * with an exact {@code assertEquals}, so field ordering matters.</p>
 */
public class MessageBodyTest {

    /**
     * Mocked container supplying the parsed log message and raw message to the body builder.
     */
    @Mock
    private MessageContainer messageContainer;
    /**
     * Mocked metrics reporter required when constructing the message parsers.
     */
    @Mock
    private StatsDReporter statsDReporter;
    /**
     * HTTP sink configuration rebuilt per scenario from {@link #configuration}.
     */
    private HttpSinkConfig sinkConfig;
    /**
     * Mutable configuration map seeded in {@link #setup()} and overridden per test before the config
     * is rebuilt.
     */
    private final Map<String, String> configuration = new HashMap<>();

    /**
     * Parses a representative protobuf message and stubs the container before each test.
     *
     * <p>Builds a {@link com.gotocompany.depot.TestTypesMessage} value (including a timestamp), wraps
     * it into a {@link Message} carrying a {@code message_topic} metadata tuple, parses the log message
     * with a real {@link ProtoMessageParser}, and stubs the container to return the parsed message and
     * the raw message.</p>
     *
     * @throws IOException if parser construction or message parsing fails
     */
    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        Instant time = Instant.ofEpochSecond(1669160207, 0);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestTypesMessage");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        ProtoMessageParser protoMessageParser = (ProtoMessageParser) MessageParserFactory.getParser(sinkConfig, statsDReporter);
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build();
        TestMessage testMessage = TestMessage.newBuilder()
                .setOrderNumber("test-order-1")
                .setOrderDetails("ORDER-DETAILS-1")
                .build();
        TestTypesMessage testTypesMessage = TestTypesMessage.newBuilder()
                .setStringValue("test-string")
                .setFloatValue(10.0f)
                .setBoolValue(true)
                .addListValues("test-list-1").addListValues("test-list-2").addListValues("test-list-3")
                .addListMessageValues(testMessage).addListMessageValues(testMessage)
                .setMessageValue(testMessage)
                .setTimestampValue(timestamp)
                .build();
        Message message = new Message(testMessage.toByteArray(), testTypesMessage.toByteArray(), new Tuple<>("message_topic", "sample-topic"));
        ParsedMessage parsedMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        when(messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass())).thenReturn(parsedMessage);
        when(messageContainer.getMessage()).thenReturn(message);
    }

    /**
     * Verifies that a protobuf log message is serialised to its JSON representation.
     *
     * <p>Given the parsed {@link com.gotocompany.depot.TestTypesMessage} from {@link #setup()}, when
     * {@link MessageBody#build(MessageContainer)} is invoked, then the result equals the expected JSON
     * object (including nested messages, repeated fields and the timestamp rendered as
     * {@code 2022-11-22T23:36:47Z}).</p>
     *
     * @throws IOException never in practice; declared because building the body is a checked operation
     */
    @Test
    public void shouldReturnJsonPayloadFromProtoInput() throws IOException {
        RequestBody body = new MessageBody(sinkConfig);
        String stringBody = body.build(messageContainer);
        String expected = "{\"listValues\":[\"test-list-1\",\"test-list-2\",\"test-list-3\"],"
                + "\"stringValue\":\"test-string\","
                + "\"listMessageValues\":[{\"orderDetails\":\"ORDER-DETAILS-1\",\"orderNumber\":\"test-order-1\"},"
                + "{\"orderDetails\":\"ORDER-DETAILS-1\",\"orderNumber\":\"test-order-1\"}],"
                + "\"timestampValue\":\"2022-11-22T23:36:47Z\",\"floatValue\":10,\"boolValue\":true,"
                + "\"messageValue\":{\"orderDetails\":\"ORDER-DETAILS-1\",\"orderNumber\":\"test-order-1\"}}";
        assertEquals(expected, stringBody);
    }

    /**
     * Verifies that a JSON log message is passed through unchanged.
     *
     * <p>Given a JSON schema data type and a message whose value is {@code {"first_name": "john doe"}}
     * parsed with a real {@link JsonMessageParser}, when {@link MessageBody#build(MessageContainer)} is
     * invoked, then the result equals {@code {"first_name":"john doe"}}.</p>
     *
     * @throws IOException never in practice; declared because building the body is a checked operation
     */
    @Test
    public void shouldReturnJsonPayloadFromJsonInput() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("SINK_CONNECTOR_SCHEMA_DATA_TYPE", "JSON");
        HttpSinkConfig httpSinkConfig = ConfigFactory.create(HttpSinkConfig.class, config);
        JsonMessageParser jsonMessageParser = (JsonMessageParser) MessageParserFactory.getParser(httpSinkConfig, statsDReporter);
        Message jsonMessage = new Message("".getBytes(), "{\"first_name\": \"john doe\"}".getBytes());
        ParsedMessage jsonParsedMessage = jsonMessageParser.parse(jsonMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "");
        when(messageContainer.getParsedLogMessage("")).thenReturn(jsonParsedMessage);
        RequestBody body = new MessageBody(httpSinkConfig);
        String stringBody = body.build(messageContainer);
        String expected = "{\"first_name\":\"john doe\"}";
        assertEquals(expected, stringBody);
    }
}
