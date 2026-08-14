package com.gotocompany.depot.http.request.body;

import com.google.protobuf.Timestamp;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestTypesMessage;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TemplatizedJsonBody}, the {@link RequestBody} implementation that renders a
 * configurable JSON template whose {@code %s,field} placeholders are substituted with values extracted
 * from the parsed message.
 *
 * <p>A real {@link ProtoMessageParser} obtained via {@link MessageParserFactory} parses a
 * {@link com.gotocompany.depot.TestTypesMessage}, while the {@link MessageContainer} and
 * {@link StatsDReporter} are Mockito mocks. The tests cover successful parameterised rendering as well
 * as the validation failures raised for an empty template, a syntactically invalid template
 * ({@link ConfigurationException}) and a placeholder that names a non-existent field
 * ({@link IllegalArgumentException}).</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class TemplatizedJsonBodyTest {
    /**
     * Mocked container supplying the parsed log message to the body builder.
     */
    @Mock
    private MessageContainer messageContainer;
    /**
     * Mocked metrics reporter required when constructing the message parser.
     */
    @Mock
    private StatsDReporter statsDReporter;
    /**
     * Timestamp instant embedded in the message and referenced by the rendering assertion.
     */
    private Instant time;
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
     * <p>Configures the proto key/message classes, the {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE}
     * mode and disables default field values, then builds a {@link com.gotocompany.depot.TestTypesMessage}
     * (including a timestamp), parses it with a real {@link ProtoMessageParser}, and stubs the container
     * to return the parsed log message.</p>
     *
     * @throws IOException if parser construction or message parsing fails
     */
    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        time = Instant.ofEpochSecond(1669160207, 600000000);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestTypesMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "com.gotocompany.depot.TestMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        configuration.put("SINK_HTTPV2_DEFAULT_FIELD_VALUE_ENABLE", "false");
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
        Message message = new Message(testMessage.toByteArray(), testTypesMessage.toByteArray());
        ParsedMessage parsedLogMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        when(messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass())).thenReturn(parsedLogMessage);
    }

    /**
     * Verifies that template placeholders are substituted with values from the parsed message across
     * multiple field types.
     *
     * <p>Given a JSON template referencing float, string, repeated, message, nested-message and
     * timestamp fields (including a nested {@code timestamp_value.seconds} access), when
     * {@link TemplatizedJsonBody#build(MessageContainer)} is invoked, then the rendered JSON equals the
     * expected document with each placeholder resolved (and the timestamp rendered from
     * {@link #time}).</p>
     *
     * @throws IOException never in practice; declared because building the body is a checked operation
     */
    @Test
    public void shouldReturnJsonBodyWithParameterizedValue() throws IOException {
        configuration.put("SINK_HTTPV2_JSON_BODY_TEMPLATE",
                "{"
                        + "\"test_float\":\"%s,float_value\", "
                        + "\"%s,string_value\" : "
                        + "{"
                        + "\"xxx\" : \"constant\", "
                        + "\"yyy\" : \"Test-%s-%s,string_value,float_value\" "
                        + "}, "
                        + "\"test_repeated\" : \"%s,list_values\", "
                        + "\"test_repeated_messages\" : \"%s,list_message_values\", "
                        + "\"test_message\" : \"%s,message_value\", "
                        + "\"test_timestamp\" : \"%s,timestamp_value\","
                        + "\"test_seconds\" : \"%s,timestamp_value.seconds\""
                        + "}"
        );
        configuration.put("SINK_HTTPV2_DEFAULT_FIELD_VALUE_ENABLE", "false");

        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        RequestBody body = new TemplatizedJsonBody(sinkConfig);
        String stringBody = body.build(messageContainer);
        String expected = "{\"test_float\":10.0,\"test-string\":{\"xxx\":\"constant\",\"yyy\":\"Test-test-string-10.0\"},\"test_repeated\":[\"test-list-1\",\"test-list-2\",\"test-list-3\"],\"test_repeated_messages\":[{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"},{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"}],\"test_message\":{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"},\"test_timestamp\":\"" + time.toString() + "\",\"test_seconds\":1669160207}";
        assertEquals(expected, stringBody);
    }

    /**
     * Verifies that an empty template is rejected at construction time.
     *
     * <p>Given an empty {@code SINK_HTTPV2_JSON_BODY_TEMPLATE}, when a {@link TemplatizedJsonBody} is
     * constructed, then a {@link ConfigurationException} is thrown with the message
     * {@code "Json body template cannot be empty"}.</p>
     */
    @Test
    public void shouldThrowExceptionIfJsonTemplateBodyIsEmpty() {
        configuration.put("SINK_HTTPV2_JSON_BODY_TEMPLATE", "");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        ConfigurationException thrown = assertThrows(ConfigurationException.class, () -> new TemplatizedJsonBody(sinkConfig));
        assertEquals("Json body template cannot be empty", thrown.getMessage());
    }

    /**
     * Verifies that a syntactically invalid template is rejected at construction time.
     *
     * <p>Given a malformed template ({@code {"a"="b"}}), when a {@link TemplatizedJsonBody} is
     * constructed, then a {@link ConfigurationException} is thrown whose message reports the underlying
     * JSON parse error.</p>
     */
    @Test
    public void shouldThrowExceptionIfJsonTemplateBodyIsNotValid() {
        configuration.put("SINK_HTTPV2_JSON_BODY_TEMPLATE", "{\"a\"=\"b\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        ConfigurationException thrown = assertThrows(ConfigurationException.class, () -> new TemplatizedJsonBody(sinkConfig));
        assertEquals("Json body template is not a valid json. Unexpected character ('=' (code 61)): was expecting a colon to separate field name and value\n"
                + " at [Source: (String)\"{\"a\"=\"b\"}\"; line: 1, column: 6]", thrown.getMessage());
    }


    /**
     * Verifies that referencing a field absent from the schema fails when the body is built.
     *
     * <p>Given a template placeholder naming {@code unknown_field}, when
     * {@link TemplatizedJsonBody#build(MessageContainer)} is invoked, then an
     * {@link IllegalArgumentException} is thrown with the message
     * {@code "Invalid field config : unknown_field"}.</p>
     */
    @Test
    public void shouldThrowExceptionForUnknownFieldInTemplate() {
        configuration.put("SINK_HTTPV2_JSON_BODY_TEMPLATE", "{\"test_string\":\"%s,unknown_field\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        RequestBody body = new TemplatizedJsonBody(sinkConfig);
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> body.build(messageContainer));
        assertEquals("Invalid field config : unknown_field", thrown.getMessage());
    }
}
