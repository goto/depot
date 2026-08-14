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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link QueryParamBuilder}, which builds the request's query parameters from a query
 * template containing constant entries and {@code %s,field} placeholders resolved from the message or
 * the key.
 *
 * <p>A real {@link ProtoMessageParser} obtained via {@link MessageParserFactory} parses
 * {@link com.gotocompany.depot.TestBookingLogKey} and {@link com.gotocompany.depot.TestBookingLogMessage}
 * payloads, while the {@link MessageContainer} and {@link StatsDReporter} are Mockito mocks. The tests
 * cover constant parameters, parameters templated from the message and from the key, mixed
 * constant/parameterised templates, empty/missing templates and the validation error raised for an
 * unknown field.</p>
 */
public class QueryParamBuilderTest {

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
     * Mocked container supplying the parsed log key and log message used for templated parameters.
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
     * mode and a {@code MESSAGE} query parameter source, then parses both the key and message with a
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
        configuration.put("SINK_HTTPV2_QUERY_PARAMETER_SOURCE", "MESSAGE");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        ProtoMessageParser messageParser = (ProtoMessageParser) MessageParserFactory.getParser(sinkConfig, statsDReporter);
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        Message message = new Message(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());
        ParsedMessage parsedMessage = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        ParsedMessage parsedLogKey = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, sinkConfig.getSinkConnectorSchemaProtoKeyClass());

        when(messageContainer.getParsedLogKey(sinkConfig.getSinkConnectorSchemaProtoKeyClass())).thenReturn(parsedLogKey);
        when(messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass())).thenReturn(parsedMessage);
    }

    /**
     * Verifies that a template with only constant values produces those query parameters verbatim.
     *
     * <p>Given a template {@code {"order_number":"V-1234", "order_details":"test-details"}}, when
     * {@link QueryParamBuilder#build()} is invoked, then the result maps {@code order_number} to
     * {@code V-1234} and {@code order_details} to {@code test-details}.</p>
     */
    @Test
    public void shouldGenerateConstantQueryParameter() {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "{\"order_number\":\"V-1234\", \"order_details\":\"test-details\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build();

        assertEquals("V-1234", queryParam.get("order_number"));
        assertEquals("test-details", queryParam.get("order_details"));
    }

    /**
     * Verifies that a query template is resolved from message fields.
     *
     * <p>Given a template {@code {"H-%s,order_number":"V-%s,service_type"}} with a {@code MESSAGE}
     * parameter source, when {@link QueryParamBuilder#build(MessageContainer)} is invoked, then the
     * result has a single entry {@code H-ON#1=V-GO_SEND}.</p>
     *
     * @throws IOException never in practice; declared because building templated parameters is checked
     */
    @Test
    public void shouldGenerateQueryParameterFromTemplate() throws IOException {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,service_type\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer);

        assertEquals(1, queryParam.size());
        assertEquals("V-GO_SEND", queryParam.get("H-ON#1"));
    }

    /**
     * Verifies that a query template can be resolved from key fields when the parameter source is the
     * key.
     *
     * <p>Given a template {@code {"H-%s,order_url":"V-%s,order_number"}} with a {@code KEY} parameter
     * source, when {@link QueryParamBuilder#build(MessageContainer)} is invoked, then the result has a
     * single entry {@code H-OURL#1=V-ON#1} sourced from the parsed key.</p>
     *
     * @throws IOException never in practice; declared because building templated parameters is checked
     */
    @Test
    public void shouldGenerateQueryParameterFromTemplateWhenQueryParamSourceIsKey() throws IOException {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "{\"H-%s,order_url\":\"V-%s,order_number\"}");
        configuration.put("SINK_HTTPV2_QUERY_PARAMETER_SOURCE", "KEY");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer);

        assertEquals(1, queryParam.size());
        assertEquals("V-ON#1", queryParam.get("H-OURL#1"));
    }

    /**
     * Verifies that constant template entries coexist with parameterised entries.
     *
     * <p>Given a template mixing a parameterised entry with a constant {@code "H-const":"V-const"},
     * when {@link QueryParamBuilder#build(MessageContainer)} is invoked, then the result has two
     * entries: {@code H-ON#1=V-GO_SEND} and {@code H-const=V-const}.</p>
     *
     * @throws IOException never in practice; declared because building templated parameters is checked
     */
    @Test
    public void shouldHandleConstantStringInTemplateAlongWithParameterizedQuery() throws IOException {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,service_type\", \"H-const\":\"V-const\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer);

        assertEquals(2, queryParam.size());
        assertEquals("V-GO_SEND", queryParam.get("H-ON#1"));
        assertEquals("V-const", queryParam.get("H-const"));
    }

    /**
     * Verifies that an empty JSON object template produces no query parameters.
     *
     * <p>Given a template of {@code {}}, when {@link QueryParamBuilder#build(MessageContainer)} is
     * invoked, then the result is an empty map.</p>
     *
     * @throws IOException never in practice; declared because building templated parameters is checked
     */
    @Test
    public void shouldReturnEmptyCollectionIfQueryTemplateIsEmpty() throws IOException {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "{}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer);

        assertEquals(0, queryParam.size());
        assertEquals(Collections.emptyMap(), queryParam);
    }

    /**
     * Verifies that an empty-string template produces no query parameters.
     *
     * <p>Given an empty template string, when {@link QueryParamBuilder#build(MessageContainer)} is
     * invoked, then the result is an empty map.</p>
     *
     * @throws IOException never in practice; declared because building templated parameters is checked
     */
    @Test
    public void shouldReturnEmptyMapIfQueryTemplateIsEmptyString() throws IOException {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer);

        assertEquals(0, queryParam.size());
        assertEquals(Collections.emptyMap(), queryParam);
    }

    /**
     * Verifies that omitting the template entirely produces no query parameters.
     *
     * <p>Given no configured query template, when {@link QueryParamBuilder#build(MessageContainer)} is
     * invoked, then the result is an empty map.</p>
     *
     * @throws IOException never in practice; declared because building templated parameters is checked
     */
    @Test
    public void shouldReturnEmptyMapIfQueryTemplateIsNotProvided() throws IOException {
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);
        Map<String, String> queryParam = queryParamBuilder.build(messageContainer);

        assertEquals(0, queryParam.size());
        assertEquals(Collections.emptyMap(), queryParam);
    }

    /**
     * Verifies that a template referencing a field absent from the schema is rejected.
     *
     * <p>Given a template placeholder naming {@code RANDOM_FIELD}, when
     * {@link QueryParamBuilder#build(MessageContainer)} is invoked, then an
     * {@link IllegalArgumentException} with the message {@code "Invalid field config : RANDOM_FIELD"}
     * is thrown; the assertion is performed inside a {@code catch} block.</p>
     */
    @Test
    public void shouldThrowIllegalArgumentExceptionIfAnyFieldNameProvidedDoesNotExistInSchema() {
        configuration.put("SINK_HTTPV2_QUERY_TEMPLATE", "{\"H-%s,order_number\":\"V-%s,RANDOM_FIELD\"}");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(sinkConfig);

        try {
            queryParamBuilder.build(messageContainer);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("Invalid field config : RANDOM_FIELD", e.getMessage());
        }
    }
}
