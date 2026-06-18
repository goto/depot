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
import com.gotocompany.depot.message.MessageParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link BatchRequest}, the {@link Request} strategy that collapses an entire batch of
 * messages into a single HTTP request whose body is a JSON array of the per-message payloads.
 *
 * <p>The builders ({@link HeaderBuilder}, {@link QueryParamBuilder}, {@link UriBuilder}), the
 * {@link MessageParser} and the {@link HttpSinkConfig} are Mockito mocks; a real
 * {@link com.gotocompany.depot.TestMessage} protobuf is used as the message payload. The tests assert
 * how messages are partitioned into valid and invalid {@link HttpRequestRecord} instances via
 * {@link HttpRequestRecord#isValid()}, that valid messages are merged into a single batched record,
 * and how {@code DELETE} requests are rendered with or without a body depending on configuration.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class BatchRequestTest {

    /**
     * Shared, mutable list of input messages populated per test before building the request.
     */
    private final List<Message> messages = new ArrayList<>();
    /**
     * Mocked builder for HTTP headers.
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
     * Mocked message parser supplied to the batch request.
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
     * request method so that the request defaults to a raw, batched PUT unless a test overrides it.</p>
     */
    @Before
    public void setup() {
        testMessage = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        when(config.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        when(config.getSinkHttpRequestMethod()).thenReturn(HttpRequestMethodType.PUT);
    }

    /**
     * Verifies that an entire batch of valid messages is wrapped into a single request whose body is
     * a JSON array of per-message payloads.
     *
     * <p>Given two valid messages (one with a {@code null} key), when
     * {@link BatchRequest#createRecords(List)} is invoked, then exactly one valid record is produced
     * with no invalid records, and its request body is the JSON array containing both messages'
     * base64-encoded {@code log_key}/{@code log_message} entries.</p>
     *
     * @throws IOException never in practice; declared because reading the request body is checked
     */
    @Test
    public void shouldWrapMessagesToSingleRequestBody() throws IOException {
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(1, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(0, invalidRecords.size());
        assertEquals("[{\"log_key\":\"\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}, {\"log_key\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}]", parsedRecords.get(0).getRequestBody());
    }

    /**
     * Verifies that a batch of two valid messages yields a single valid batched record.
     *
     * <p>Given two valid messages, when {@link BatchRequest#createRecords(List)} is invoked and the
     * results are partitioned by validity, then there is one record in total, one valid record and no
     * invalid records.</p>
     */
    @Test
    public void shouldGetValidRequestRecords() {
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(1, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(0, invalidRecords.size());
    }

    /**
     * Verifies that messages which cannot be serialised become individual invalid records rather than
     * being batched.
     *
     * <p>Given two messages whose key/value types are not byte arrays, when
     * {@link BatchRequest#createRecords(List)} is invoked, then two records are produced, none valid
     * and both invalid.</p>
     */
    @Test
    public void shouldGetInvalidRequestRecords() {
        messages.add(new Message("", 1));
        messages.add(new Message(1, "test"));
        Request requestParser = new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(0, validRecords.size());
        assertEquals(2, invalidRecords.size());
    }

    /**
     * Verifies that a mixed batch yields one batched valid record plus a separate invalid record.
     *
     * <p>Given one message with a non-serialisable payload and one valid message, when
     * {@link BatchRequest#createRecords(List)} is invoked, then two records are produced, one valid
     * (the batched valid message) and one invalid.</p>
     */
    @Test
    public void shouldGetValidAndInvalidRequestRecords() {
        messages.add(new Message("", 1));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(1, invalidRecords.size());
    }

    /**
     * Verifies that a batched {@code DELETE} request includes the JSON array body when delete bodies
     * are enabled.
     *
     * <p>Given a {@link HttpRequestMethodType#DELETE} method with delete-body support enabled and a
     * real {@link UriBuilder}, when the batch of two valid messages is converted to records, then a
     * single valid record is produced whose request string reports the {@code DELETE} method, the
     * configured URL, empty headers and the batched JSON array body.</p>
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
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        UriBuilder uriBuilder1 = new UriBuilder(config);
        Request requestParser = new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder1, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(1, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(0, invalidRecords.size());
        assertEquals("\n"
                + "Request Method: DELETE\n"
                + "Request Url: localhost:8080/value123\n"
                + "Request Headers: []\n"
                + "Request Body: [{\"log_key\":\"\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}, {\"log_key\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}]",
                parsedRecords.get(0).getRequestString());
    }

    /**
     * Verifies that a batched {@code DELETE} request omits the body when delete bodies are disabled.
     *
     * <p>Given a {@link HttpRequestMethodType#DELETE} method with delete-body support disabled and a
     * real {@link UriBuilder}, when the batch of two valid messages is converted to records, then a
     * single valid record is produced whose request string reports the {@code DELETE} method, the
     * configured URL and empty headers, with no request body line.</p>
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
        UriBuilder uriBuilder1 = new UriBuilder(config);
        messages.add(new Message(null, testMessage.toByteArray()));
        messages.add(new Message(testMessage.toByteArray(), testMessage.toByteArray()));
        Request requestParser = new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder1, config, parser);
        List<HttpRequestRecord> parsedRecords = requestParser.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(1, parsedRecords.size());
        assertEquals(1, validRecords.size());
        assertEquals(0, invalidRecords.size());
        assertEquals("\n"
                + "Request Method: DELETE\n"
                + "Request Url: localhost:8080/value123\n"
                + "Request Headers: []", parsedRecords.get(0).getRequestString());
    }
}
