package com.gotocompany.depot.http.request;

import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.http.enums.HttpRequestBodyType;
import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.http.request.builder.HeaderBuilder;
import com.gotocompany.depot.http.request.builder.QueryParamBuilder;
import com.gotocompany.depot.http.request.builder.UriBuilder;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SingleRequest}, the {@link Request} strategy that produces one HTTP request
 * per message rather than batching them.
 *
 * <p>The builders ({@link HeaderBuilder}, {@link QueryParamBuilder}, {@link UriBuilder}), the
 * {@link MessageParser} and the {@link HttpSinkConfig} are Mockito mocks; a real
 * {@link com.gotocompany.depot.TestMessage} protobuf provides the payload. The tests assert how each
 * message becomes its own valid or invalid {@link HttpRequestRecord} via
 * {@link HttpRequestRecord#isValid()}, how failures raised by the header builder mark records invalid,
 * and how {@code DELETE} requests are rendered with or without a per-message body.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class SingleRequestTest {

    /**
     * Shared, mutable list of input messages populated per test before building the request.
     */
    private final List<Message> messages = new ArrayList<>();
    /**
     * Mocked builder for HTTP headers; several tests stub it to return an empty map or to throw.
     */
    @Mock
    private HeaderBuilder headerBuilder;
    /**
     * Mocked builder for query parameters.
     */
    @Mock
    private QueryParamBuilder queryParamBuilder;
    /**
     * Mocked builder for the request URI.
     */
    @Mock
    private UriBuilder uriBuilder;
    /**
     * Mocked message parser supplied to the single request.
     */
    @Mock
    private MessageParser parser;
    /**
     * Mocked HTTP sink configuration controlling body type, method and delete-body behaviour.
     */
    @Mock
    private HttpSinkConfig config;
    /**
     * Reusable protobuf payload used to build the input messages.
     */
    private TestMessage testMessage;

    /**
     * Builds the shared {@link com.gotocompany.depot.TestMessage} fixture and stubs the configuration
     * common to every test.
     *
     * <p>Configures a {@link HttpRequestBodyType#RAW} body type and a {@link HttpRequestMethodType#PUT}
     * request method so that each request defaults to a raw, per-message PUT unless a test overrides
     * it.</p>
     */
    @Before
    public void setup() {
        testMessage = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        when(config.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        when(config.getSinkHttpRequestMethod()).thenReturn(HttpRequestMethodType.PUT);
    }

    /**
     * Verifies that each valid message becomes its own valid record.
     *
     * <p>Given the header builder returns an empty header map and two valid messages are supplied,
     * when {@link SingleRequest#createRecords(List)} is invoked, then two records are produced, both
     * valid and none invalid.</p>
     *
     * @throws IOException never in practice; declared because the header builder's build method is
     *     checked
     */
    @Test
    public void shouldGetValidRequestRecords() throws IOException {
        when(headerBuilder.build(any(MessageContainer.class))).thenReturn(new HashMap<>());
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new SingleRequest(headerBuilder, queryParamBuilder, uriBuilder, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(2, validRecords.size());
        assertEquals(0, invalidRecords.size());
    }

    /**
     * Verifies that messages that cannot be serialised each become an invalid record.
     *
     * <p>Given the header builder returns an empty header map and two messages whose key/value types
     * are not byte arrays, when {@link SingleRequest#createRecords(List)} is invoked, then two records
     * are produced, none valid and both invalid.</p>
     *
     * @throws IOException never in practice; declared because the header builder's build method is
     *     checked
     */
    @Test
    public void shouldGetInvalidRequestRecords() throws IOException {
        when(headerBuilder.build(any(MessageContainer.class))).thenReturn(new HashMap<>());
        messages.add(new Message("", 123));
        messages.add(new Message(123, "test"));
        Request requestParser = new SingleRequest(headerBuilder, queryParamBuilder, uriBuilder, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(0, validRecords.size());
        assertEquals(2, invalidRecords.size());
    }

    /**
     * Verifies that an {@code IOException} raised while building headers turns the affected messages
     * into invalid records instead of propagating.
     *
     * <p>Given the header builder is stubbed to throw an {@link IOException} and two otherwise valid
     * messages are supplied, when {@link SingleRequest#createRecords(List)} is invoked, then two
     * records are produced, none valid and both invalid.</p>
     *
     * @throws IOException never in practice; declared because the header builder's build method is
     *     checked
     */
    @Test
    public void shouldGetInvalidRequestRecordsWhenHeaderBuilderThrowsIOException() throws IOException {
        when(headerBuilder.build(any(MessageContainer.class))).thenThrow(IOException.class);
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new SingleRequest(headerBuilder, queryParamBuilder, uriBuilder, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(0, validRecords.size());
        assertEquals(2, invalidRecords.size());
    }

    /**
     * Verifies that an exception raised while building headers turns the affected messages into
     * invalid records.
     *
     * <p>Although the method name references an {@code IllegalArgumentException}, the header builder is
     * actually stubbed to throw an {@link IOException}. Given two otherwise valid messages, when
     * {@link SingleRequest#createRecords(List)} is invoked, then two records are produced, none valid
     * and both invalid.</p>
     *
     * @throws IOException never in practice; declared because the header builder's build method is
     *     checked
     */
    @Test
    public void shouldGetInvalidRequestRecordsWhenHeaderBuilderThrowsIllegalArgumentException() throws IOException {
        when(headerBuilder.build(any(MessageContainer.class))).thenThrow(IOException.class);
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new SingleRequest(headerBuilder, queryParamBuilder, uriBuilder, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(0, validRecords.size());
        assertEquals(2, invalidRecords.size());
    }

    /**
     * Verifies that a mix of one serialisable and one non-serialisable message yields one valid and
     * one invalid record.
     *
     * <p>Given one message with a non-byte-array value and one valid message, when
     * {@link SingleRequest#createRecords(List)} is invoked, then two records are produced, one valid
     * and one invalid.</p>
     */
    @Test
    public void shouldGetValidAndInvalidRequestRecords() {
        messages.add(new Message(null, ""));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new SingleRequest(headerBuilder, queryParamBuilder, uriBuilder, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(1, invalidRecords.size());
    }

    /**
     * Verifies that per-message {@code DELETE} requests include their body when delete bodies are
     * enabled.
     *
     * <p>Given a {@link HttpRequestMethodType#DELETE} method with delete-body support enabled, an
     * empty header map and a real {@link UriBuilder}, when two valid messages are converted to
     * records, then two valid records are produced and each record's request string reports the
     * {@code DELETE} method, the configured URL, empty headers and that message's own JSON body.</p>
     *
     * @throws IOException never in practice; declared because building the request string is checked
     * @throws InvalidTemplateException never in practice; declared because the real URI builder may
     *     reject an invalid template
     */
    @Test
    public void shouldCreateDeleteRequestWithBody() throws IOException, InvalidTemplateException {
        when(config.getSinkHttpRequestMethod()).thenReturn(HttpRequestMethodType.DELETE);
        when(config.isSinkHttpDeleteBodyEnable()).thenReturn(true);
        when(config.getSinkHttpServiceUrl()).thenReturn("localhost:8080/value123");
        when(headerBuilder.build(any(MessageContainer.class))).thenReturn(new HashMap<>());
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        UriBuilder uriBuilder1 = new UriBuilder(config);
        Request requestParser = new SingleRequest(headerBuilder, queryParamBuilder, uriBuilder1, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(2, validRecords.size());
        assertEquals(0, invalidRecords.size());
        assertEquals("\n"
                        + "Request Method: DELETE\n"
                        + "Request Url: localhost:8080/value123\n"
                        + "Request Headers: []\n"
                        + "Request Body: {\"log_key\":\"\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}",
                parsedRecords.get(0).getRequestString());
        assertEquals("\n"
                        + "Request Method: DELETE\n"
                        + "Request Url: localhost:8080/value123\n"
                        + "Request Headers: []\n"
                        + "Request Body: {\"log_key\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}",
                parsedRecords.get(1).getRequestString());
    }

    /**
     * Verifies that per-message {@code DELETE} requests omit their body when delete bodies are
     * disabled.
     *
     * <p>Given a {@link HttpRequestMethodType#DELETE} method with delete-body support disabled, an
     * empty header map and a real {@link UriBuilder}, when two valid messages are converted to
     * records, then each record's request string reports the {@code DELETE} method, the configured URL
     * and empty headers, with no request body line.</p>
     *
     * @throws IOException never in practice; declared because building the request string is checked
     * @throws InvalidTemplateException never in practice; declared because the real URI builder may
     *     reject an invalid template
     */
    @Test
    public void shouldCreateDeleteRequestWithoutBody() throws IOException, InvalidTemplateException {
        when(config.getSinkHttpRequestMethod()).thenReturn(HttpRequestMethodType.DELETE);
        when(config.isSinkHttpDeleteBodyEnable()).thenReturn(false);
        when(config.getSinkHttpServiceUrl()).thenReturn("localhost:8080/value123");
        when(headerBuilder.build(any(MessageContainer.class))).thenReturn(new HashMap<>());
        UriBuilder uriBuilder1 = new UriBuilder(config);
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new SingleRequest(headerBuilder, queryParamBuilder, uriBuilder1, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        assertEquals("\n"
                        + "Request Method: DELETE\n"
                        + "Request Url: localhost:8080/value123\n"
                        + "Request Headers: []",
                parsedRecords.get(0).getRequestString());
        assertEquals("\n"
                        + "Request Method: DELETE\n"
                        + "Request Url: localhost:8080/value123\n"
                        + "Request Headers: []",
                parsedRecords.get(1).getRequestString());
    }
}
