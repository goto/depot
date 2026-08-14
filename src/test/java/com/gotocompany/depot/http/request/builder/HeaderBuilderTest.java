package com.gotocompany.depot.http.request.builder;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.TestBookingLogKey;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestServiceType;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link HeaderBuilder}, which assembles HTTP request headers from a static base header
 * configuration and an optional header template whose {@code %s,field} placeholders are resolved from
 * the message or key.
 *
 * <p>A real {@link ProtoMessageParser} obtained via {@link MessageParserFactory} parses
 * {@link com.gotocompany.depot.TestBookingLogKey} and {@link com.gotocompany.depot.TestBookingLogMessage}
 * payloads, while the {@link MessageContainer} and {@link StatsDReporter} are Mockito mocks. The tests
 * cover base header parsing, multiple headers, empty and malformed configurations, templated headers
 * sourced from the message and from the key, constant template entries, and the validation error raised
 * for an unknown field.</p>
 */
public class HeaderBuilderTest {

    /**
     * HTTP sink configuration rebuilt per scenario from {@link #configuration}.
     */
    private HttpSinkConfig sinkConfig;
    /**
     * Mocked metrics reporter required when constructing the message parser.
     */
    @Mock
    private StatsDReporter statsDReporter;
    /**
     * Mocked container supplying the parsed log key and log message used for templated headers.
     */
    @Mock
    private MessageContainer messageContainer;
    /**
     * Mutable configuration map seeded in {@link #setup()} and overridden per test before the config
     * is rebuilt.
     */
    private final Map<String, String> configuration = new HashMap<>();

    /**
     * Parses a representative booking-log key and message and stubs the container before each test.
     *
     * <p>Configures the proto key/message classes, {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE}
     * mode and a {@code MESSAGE} header parameter source, then parses both the key and message with a
     * real {@link ProtoMessageParser} and stubs the container to return the parsed key and parsed
     * message.</p>
     *
     * @throws IOException if parser construction or message parsing fails
     */
    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestBookingLogMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "com.gotocompany.depot.TestBookingLogKey");
        configuration.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        configuration.put("SINK_HTTPV2_HEADERS_PARAMETER_SOURCE", "MESSAGE");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        ProtoMessageParser protoMessageParser = (ProtoMessageParser) MessageParserFactory.getParser(sinkConfig, statsDReporter);
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        Message message = new Message(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());
        ParsedMessage parsedMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        ParsedMessage parsedLogKey = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, sinkConfig.getSinkConnectorSchemaProtoKeyClass());

        when(messageContainer.getParsedLogKey(sinkConfig.getSinkConnectorSchemaProtoKeyClass())).thenReturn(parsedLogKey);
        when(messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass())).thenReturn(parsedMessage);
    }

    /**
     * Verifies that a single base header entry is parsed into the header map.
     *
     * <p>Given a base header configuration of {@code content-type:json}, when
     * {@link HeaderBuilder#build()} is invoked, then the returned map maps {@code content-type} to
     * {@code json}.</p>
     */
    @Test
    public void shouldGenerateBaseHeader() {
        configuration.put("SINK_HTTPV2_HEADERS", "content-type:json");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);

        assertEquals("json", headerBuilder.build().get("content-type"));
    }

    /**
     * Verifies that multiple comma-separated base headers are each parsed into the header map.
     *
     * <p>Given a base header configuration of {@code Authorization:auth_token,Accept:text/plain}, when
     * {@link HeaderBuilder#build()} is invoked, then the returned map contains both the
     * {@code Authorization} and {@code Accept} entries with their values.</p>
     */
    @Test
    public void shouldHandleMultipleBaseHeaders() {
        configuration.put("SINK_HTTPV2_HEADERS", "Authorization:auth_token,Accept:text/plain");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> header = headerBuilder.build();

        assertEquals("auth_token", header.get("Authorization"));
        assertEquals("text/plain", header.get("Accept"));
    }

    /**
     * Verifies that an empty base header configuration is handled gracefully.
     *
     * <p>Given an empty header configuration, when {@link HeaderBuilder#build()} is invoked, then it
     * completes without throwing a {@code NullPointerException}.</p>
     */
    @Test
    public void shouldNotThrowNullPointerExceptionWhenHeaderConfigEmpty() {
        configuration.put("SINK_HTTPV2_HEADERS", "");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        headerBuilder.build();
    }

    /**
     * Verifies that a malformed base header configuration fails fast.
     *
     * <p>Given a header configuration containing entries that are not well-formed {@code key:value}
     * pairs, when {@link HeaderBuilder#build()} is invoked, then an
     * {@link ArrayIndexOutOfBoundsException} is thrown.</p>
     */
    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void shouldThrowErrorIfHeaderConfigIsInvalid() {
        configuration.put("SINK_HTTPV2_HEADERS", "content-type:json,header_key;header_value,key:,:value");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        headerBuilder.build();
    }

    /**
     * Verifies that a header template is resolved from message fields and merged with base headers.
     *
     * <p>Given a base header {@code content-type:json} and a template
     * {@code {"H-%s,order_number":"V-%s,service_type"}} with a {@code MESSAGE} parameter source, when
     * {@link HeaderBuilder#build(MessageContainer)} is invoked, then the result has two entries:
     * {@code content-type=json} and {@code H-ON#1=V-GO_SEND}.</p>
     *
     * @throws IOException never in practice; declared because building templated headers is checked
     */
    @Test
    public void shouldGenerateParameterisedHeaderFromTemplate() throws IOException {
        configuration.put("SINK_HTTPV2_HEADERS", "content-type:json");
        configuration.put("SINK_HTTPV2_HEADERS_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,service_type\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(2, headers.size());
        assertEquals("json", headers.get("content-type"));
        assertEquals("V-GO_SEND", headers.get("H-ON#1"));
    }

    /**
     * Verifies that a header template can be resolved from key fields when the parameter source is the
     * key.
     *
     * <p>Given a template {@code {"H-%s,order_url":"V-%s,order_number"}} with a {@code KEY} parameter
     * source, when {@link HeaderBuilder#build(MessageContainer)} is invoked, then the result has two
     * entries: {@code content-type=json} and {@code H-OURL#1=V-ON#1} sourced from the parsed key.</p>
     *
     * @throws IOException never in practice; declared because building templated headers is checked
     */
    @Test
    public void shouldGenerateParameterisedHeaderFromTemplateWhenHeaderParamSourceIsKey() throws IOException {
        configuration.put("SINK_HTTPV2_HEADERS", "content-type:json");
        configuration.put("SINK_HTTPV2_HEADERS_TEMPLATE", "{\"H-%s,order_url\":\"V-%s,order_number\"}");
        configuration.put("SINK_HTTPV2_HEADERS_PARAMETER_SOURCE", "KEY");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(2, headers.size());
        assertEquals("json", headers.get("content-type"));
        assertEquals("V-ON#1", headers.get("H-OURL#1"));
    }

    /**
     * Verifies that templated headers are produced even when no base headers are configured.
     *
     * <p>Given only a header template {@code {"H-%s,order_number":"V-%s,service_type"}} and no base
     * headers, when {@link HeaderBuilder#build(MessageContainer)} is invoked, then the result has a
     * single entry {@code H-ON#1=V-GO_SEND}.</p>
     *
     * @throws IOException never in practice; declared because building templated headers is checked
     */
    @Test
    public void shouldGenerateParameterisedHeaderFromTemplateWhenBaseHeadersAreNotProvided() throws IOException {
        configuration.put("SINK_HTTPV2_HEADERS_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,service_type\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(1, headers.size());
        assertEquals("V-GO_SEND", headers.get("H-ON#1"));
    }

    /**
     * Verifies that constant template entries coexist with parameterised entries and base headers.
     *
     * <p>Given a base header and a template mixing a parameterised entry with a constant
     * {@code "H-const":"V-const"}, when {@link HeaderBuilder#build(MessageContainer)} is invoked, then
     * the result has three entries: {@code H-ON#1=V-GO_SEND}, {@code H-const=V-const} and
     * {@code content-type=json}.</p>
     *
     * @throws IOException never in practice; declared because building templated headers is checked
     */
    @Test
    public void shouldHandleConstantHeaderStringsProvidedInTemplateAlongWithAnyFormattedString() throws IOException {
        configuration.put("SINK_HTTPV2_HEADERS", "content-type:json");
        configuration.put("SINK_HTTPV2_HEADERS_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,service_type\", \"H-const\":\"V-const\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(3, headers.size());
        assertEquals("V-GO_SEND", headers.get("H-ON#1"));
        assertEquals("V-const", headers.get("H-const"));
        assertEquals("json", headers.get("content-type"));
    }

    /**
     * Verifies that an empty JSON object template yields only the base headers.
     *
     * <p>Given a base header and a template of {@code {}}, when
     * {@link HeaderBuilder#build(MessageContainer)} is invoked, then the result has a single entry
     * {@code content-type=json}.</p>
     *
     * @throws IOException never in practice; declared because building templated headers is checked
     */
    @Test
    public void shouldReturnBaseHeadersIfHeadersTemplateIsEmpty() throws IOException {
        configuration.put("SINK_HTTPV2_HEADERS", "content-type:json");
        configuration.put("SINK_HTTPV2_HEADERS_TEMPLATE", "{}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(1, headers.size());
        assertEquals("json", headers.get("content-type"));
    }

    /**
     * Verifies that an empty-string template yields only the base headers.
     *
     * <p>Given a base header and an empty template string, when
     * {@link HeaderBuilder#build(MessageContainer)} is invoked, then the result has a single entry
     * {@code content-type=json}.</p>
     *
     * @throws IOException never in practice; declared because building templated headers is checked
     */
    @Test
    public void shouldReturnBaseHeadersIfHeadersTemplateIsEmptyString() throws IOException {
        configuration.put("SINK_HTTPV2_HEADERS", "content-type:json");
        configuration.put("SINK_HTTPV2_HEADERS_TEMPLATE", "");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(1, headers.size());
        assertEquals("json", headers.get("content-type"));
    }

    /**
     * Verifies that omitting the template entirely yields only the base headers.
     *
     * <p>Given a base header and no configured template, when
     * {@link HeaderBuilder#build(MessageContainer)} is invoked, then the result has a single entry
     * {@code content-type=json}.</p>
     *
     * @throws IOException never in practice; declared because building templated headers is checked
     */
    @Test
    public void shouldReturnBaseHeadersIfHeadersTemplateIsNotProvided() throws IOException {
        configuration.put("SINK_HTTPV2_HEADERS", "content-type:json");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);
        Map<String, String> headers = headerBuilder.build(messageContainer);

        assertEquals(1, headers.size());
        assertEquals("json", headers.get("content-type"));
    }

    /**
     * Verifies that a template referencing a field absent from the schema is rejected.
     *
     * <p>Given a template placeholder naming {@code RANDOM_FIELD}, when
     * {@link HeaderBuilder#build(MessageContainer)} is invoked, then an
     * {@link IllegalArgumentException} with the message {@code "Invalid field config : RANDOM_FIELD"}
     * is thrown; the assertion is performed inside a {@code catch} block.</p>
     */
    @Test
    public void shouldThrowIllegalArgumentExceptionIfAnyFieldNameProvidedDoesNotExistInSchema() {
        configuration.put("SINK_HTTPV2_HEADERS", "content-type:json");
        configuration.put("SINK_HTTPV2_HEADERS_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,RANDOM_FIELD\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(sinkConfig);

        try {
            headerBuilder.build(messageContainer);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("Invalid field config : RANDOM_FIELD", e.getMessage());
        }
    }
}
