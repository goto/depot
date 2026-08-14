package com.gotocompany.depot.http.request.builder;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.TestBookingLogKey;
import com.gotocompany.depot.TestBookingLogMessage;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link UriBuilder}, which constructs the request {@link URI} from the configured
 * service URL (optionally containing a {@code %s,field} template resolved from the message) and a map
 * of query parameters.
 *
 * <p>A real {@link ProtoMessageParser} obtained via {@link MessageParserFactory} parses a
 * {@link com.gotocompany.depot.TestBookingLogMessage}, while the {@link MessageContainer} and
 * {@link StatsDReporter} are Mockito mocks. The suite verifies URL trimming, query parameter
 * appending and template resolution, and uses a JUnit {@link ExpectedException} rule to assert the
 * {@link InvalidTemplateException} and {@link ConfigurationException} failures for empty, missing or
 * invalid URLs.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class UriBuilderTest {

    /**
     * JUnit rule used by the failure tests to declare the expected exception type and message.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
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
     * Mocked container supplying the parsed log message used when resolving URI templates.
     */
    @Mock
    private MessageContainer messageContainer;
    /**
     * Query parameters appended to the URI; populated per test.
     */
    private final Map<String, String> queryParam = new HashMap<>();
    /**
     * Mutable configuration map seeded in {@link #setUp()} and overridden per test before the config
     * is rebuilt.
     */
    private final Map<String, String> configuration = new HashMap<>();

    /**
     * Parses a representative booking-log message and stubs the container before each test.
     *
     * <p>Configures the proto key/message classes, builds a {@link Message} from a
     * {@link com.gotocompany.depot.TestBookingLogKey} and {@link com.gotocompany.depot.TestBookingLogMessage}
     * (the message carrying {@code order_url = test-url}), parses the log message with a real
     * {@link ProtoMessageParser}, and stubs the container to return it.</p>
     *
     * @throws IOException if parser construction or message parsing fails
     */
    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestBookingLogMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "com.gotocompany.depot.TestBookingLogKey");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("test-order").setOrderUrl("test-url").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderUrl("test-url").build();
        Message message = new Message(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());
        ProtoMessageParser parser = (ProtoMessageParser) MessageParserFactory.getParser(sinkConfig, statsDReporter);
        ParsedMessage parsedMessage = parser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        when(messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass())).thenReturn(parsedMessage);
    }

    /**
     * Verifies that the base service URL is trimmed and turned into a {@link URI}.
     *
     * <p>Given a service URL of {@code "http://dummy.com   "} with trailing spaces and no query
     * parameters, when {@link UriBuilder#build(Map)} is invoked, then the result equals the
     * {@link URI} {@code http://dummy.com}.</p>
     *
     * @throws URISyntaxException never in practice; declared for the expected-URI construction
     * @throws InvalidTemplateException never in practice; declared because URL template parsing is
     *     checked
     */
    @Test
    public void shouldReturnURIInstanceBasedOnBaseUrl() throws URISyntaxException, InvalidTemplateException {
        configuration.put("SINK_HTTPV2_SERVICE_URL", "http://dummy.com   ");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        assertEquals(new URI("http://dummy.com"), uriBuilder.build(queryParam));
    }

    /**
     * Verifies that an empty service URL is rejected.
     *
     * <p>Given an empty service URL, when {@link UriBuilder#build(Map)} is invoked, then an
     * {@link InvalidTemplateException} with the message {@code "Template cannot be empty"} is thrown
     * (declared via the {@link ExpectedException} rule).</p>
     *
     * @throws InvalidTemplateException always, as the expected outcome of this test
     */
    @Test
    public void shouldFailWhenUrlConfigIsEmpty() throws InvalidTemplateException {
        expectedException.expect(InvalidTemplateException.class);
        expectedException.expectMessage("Template cannot be empty");
        configuration.put("SINK_HTTPV2_SERVICE_URL", "");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        uriBuilder.build(queryParam);
    }

    /**
     * Verifies that a missing service URL is rejected.
     *
     * <p>Given no configured service URL, when {@link UriBuilder#build(Map)} is invoked, then an
     * {@link InvalidTemplateException} with the message {@code "Template cannot be empty"} is thrown
     * (declared via the {@link ExpectedException} rule).</p>
     *
     * @throws InvalidTemplateException always, as the expected outcome of this test
     */
    @Test
    public void shouldFailWhenUrlConfigIsNotGiven() throws InvalidTemplateException {
        expectedException.expect(InvalidTemplateException.class);
        expectedException.expectMessage("Template cannot be empty");
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        uriBuilder.build(queryParam);
    }

    /**
     * Verifies that a syntactically invalid service URL is rejected.
     *
     * <p>Given a service URL containing an illegal character ({@code http://dummy.com?s=^IXIC}), when
     * {@link UriBuilder#build(Map)} is invoked, then a {@link ConfigurationException} reporting the
     * invalid service URL is thrown (declared via the {@link ExpectedException} rule).</p>
     *
     * @throws InvalidTemplateException never in practice; declared because URL template parsing is
     *     checked
     */
    @Test
    public void shouldFailWhenUrlConfigIsInvalid() throws InvalidTemplateException {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("Service URL 'http://dummy.com?s=^IXIC' is invalid");
        configuration.put("SINK_HTTPV2_SERVICE_URL", "http://dummy.com?s=^IXIC");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        uriBuilder.build(queryParam);
    }

    /**
     * Verifies that a single query parameter is appended to the URI.
     *
     * <p>Given a service URL {@code http://dummy.com} and a query parameter
     * {@code test-key=test-value}, when {@link UriBuilder#build(Map)} is invoked, then the result
     * equals the {@link URI} {@code http://dummy.com?test-key=test-value}.</p>
     *
     * @throws URISyntaxException never in practice; declared for the expected-URI construction
     * @throws InvalidTemplateException never in practice; declared because URL template parsing is
     *     checked
     */
    @Test
    public void shouldAddParameter() throws URISyntaxException, InvalidTemplateException {
        queryParam.put("test-key", "test-value");
        configuration.put("SINK_HTTPV2_SERVICE_URL", "http://dummy.com");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        assertEquals(new URI("http://dummy.com?test-key=test-value"), uriBuilder.build(queryParam));
    }

    /**
     * Verifies that a query parameter is appended to the URI.
     *
     * <p>Given a service URL {@code http://dummy.com} and a single query parameter
     * {@code test-key=test-value}, when {@link UriBuilder#build(Map)} is invoked, then the result
     * equals the {@link URI} {@code http://dummy.com?test-key=test-value}.</p>
     *
     * @throws URISyntaxException never in practice; declared for the expected-URI construction
     * @throws InvalidTemplateException never in practice; declared because URL template parsing is
     *     checked
     */
    @Test
    public void shouldAddMultipleParameter() throws URISyntaxException, InvalidTemplateException {
        queryParam.put("test-key", "test-value");
        configuration.put("SINK_HTTPV2_SERVICE_URL", "http://dummy.com");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        assertEquals(new URI("http://dummy.com?test-key=test-value"), uriBuilder.build(queryParam));
    }

    /**
     * Verifies that a {@code %s,field} placeholder in the service URL is resolved from the message.
     *
     * <p>Given a service URL template {@code http://dummy.com/%s,order_url}, when
     * {@link UriBuilder#build(MessageContainer, Map)} is invoked, then the placeholder is replaced with
     * the message's {@code order_url} and the result equals the {@link URI}
     * {@code http://dummy.com/test-url}.</p>
     *
     * @throws URISyntaxException never in practice; declared for the expected-URI construction
     * @throws InvalidTemplateException never in practice; declared because URL template parsing is
     *     checked
     * @throws IOException never in practice; declared because resolving the template reads the message
     */
    @Test
    public void shouldParseUriTemplate() throws URISyntaxException, InvalidTemplateException, IOException {
        configuration.put("SINK_HTTPV2_SERVICE_URL", "http://dummy.com/%s,order_url");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        assertEquals(new URI("http://dummy.com/test-url"), uriBuilder.build(messageContainer, queryParam));
    }

    /**
     * Verifies that a resolved URL template is combined with appended query parameters.
     *
     * <p>Given a service URL template {@code http://dummy.com/%s,order_url} and two query parameters,
     * when {@link UriBuilder#build(MessageContainer, Map)} is invoked, then the placeholder is resolved
     * and both parameters are appended, yielding
     * {@code http://dummy.com/test-url?test-key-1=test-value-1&test-key-2=test-value-2}.</p>
     *
     * @throws URISyntaxException never in practice; declared for the expected-URI construction
     * @throws InvalidTemplateException never in practice; declared because URL template parsing is
     *     checked
     * @throws IOException never in practice; declared because resolving the template reads the message
     */
    @Test
    public void shouldReturnParsedUriTemplateWithQueryParam() throws URISyntaxException, InvalidTemplateException, IOException {
        queryParam.put("test-key-1", "test-value-1");
        queryParam.put("test-key-2", "test-value-2");
        configuration.put("SINK_HTTPV2_SERVICE_URL", "http://dummy.com/%s,order_url");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        assertEquals(new URI("http://dummy.com/test-url?test-key-1=test-value-1&test-key-2=test-value-2"), uriBuilder.build(messageContainer, queryParam));
    }
}
