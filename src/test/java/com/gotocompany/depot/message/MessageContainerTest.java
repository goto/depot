package com.gotocompany.depot.message;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.TestBookingLogKey;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestServiceType;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link MessageContainer}, which lazily parses the key and message portions of a
 * {@link Message} on demand.
 *
 * <p>The {@link #setup()} fixture configures an {@link HttpSinkConfig} with proto key and message
 * classes, obtains a {@link ProtoMessageParser} from {@code MessageParserFactory} and builds a
 * booking-log {@link Message}. Each test compares the {@link ParsedMessage} returned by the container
 * against the result of parsing the same message directly with the parser, asserting that the raw
 * parsed payloads match.</p>
 */
public class MessageContainerTest {
    /** Mutable configuration map populated with the proto key and message classes. */
    private final Map<String, String> configuration = new HashMap<>();
    /** Proto parser obtained from the factory and used as the parsing baseline. */
    private ProtoMessageParser messageParser;
    /** Booking-log message whose key and value are parsed by the container. */
    private Message message;
    /** Mock reporter passed to the parser factory. */
    @Mock
    private StatsDReporter statsDReporter;
    /** Declared as a mock but reassigned in setup to a real {@link HttpSinkConfig}. */
    @Mock
    private HttpSinkConfig sinkConfig;

    /**
     * Builds the parser, configuration and message fixture before each test.
     *
     * <p>Opens the Mockito annotations, registers the proto key and message classes in
     * {@link #configuration}, creates a real {@link HttpSinkConfig}, resolves a
     * {@link ProtoMessageParser} through {@code MessageParserFactory} and constructs a booking-log
     * {@link Message} from a key and message payload.</p>
     *
     * @throws IOException if the parser cannot be created
     */
    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestBookingLogMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "com.gotocompany.depot.TestBookingLogKey");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        messageParser = (ProtoMessageParser) MessageParserFactory.getParser(sinkConfig, statsDReporter);

        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        message = new Message(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());
    }

    /**
     * Verifies that the container parses the log key consistently with the parser.
     *
     * <p>Given a {@link MessageContainer} wrapping the fixture message and parser, when
     * {@link MessageContainer#getParsedLogKey(String)} is called with the configured key class, then
     * its raw payload equals that of parsing the message directly in
     * {@link SinkConnectorSchemaMessageMode#LOG_KEY} mode.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldReturnParsedLogKey() throws IOException {
        ParsedMessage expectedParsedLogKey = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, sinkConfig.getSinkConnectorSchemaProtoKeyClass());
        MessageContainer messageContainer = new MessageContainer(message, messageParser);
        ParsedMessage parsedLogKey = messageContainer.getParsedLogKey(sinkConfig.getSinkConnectorSchemaProtoKeyClass());
        Assert.assertEquals(expectedParsedLogKey.getRaw(), parsedLogKey.getRaw());
    }

    /**
     * Verifies that the container parses the log message consistently with the parser.
     *
     * <p>Given a {@link MessageContainer} wrapping the fixture message and parser, when
     * {@link MessageContainer#getParsedLogMessage(String)} is called with the configured message
     * class, then its raw payload equals that of parsing the message directly in
     * {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE} mode.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldReturnParsedLogMessage() throws IOException {
        ParsedMessage expectedParsedLogMessage = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        MessageContainer messageContainer = new MessageContainer(message, messageParser);
        ParsedMessage parsedLogMessage = messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        Assert.assertEquals(expectedParsedLogMessage.getRaw(), parsedLogMessage.getRaw());
    }
}
