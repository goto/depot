package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.TestMessage;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link RawBody}, the {@link RequestBody} implementation that emits the raw,
 * base64-encoded message bytes (and any configured metadata columns) as a JSON object keyed by
 * {@code log_key} and {@code log_message}.
 *
 * <p>A real {@link MessageContainer} wraps the input {@link Message} with a mocked
 * {@link MessageParser}, and the {@link HttpSinkConfig} is either a Mockito mock or, for the metadata
 * scenario, a real configuration built via {@code ConfigFactory}. Assertions use
 * {@link org.json.JSONObject#similar(Object)} so that key ordering does not affect equality.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RawBodyTest {

    /**
     * Input message under test; rebuilt within individual tests to vary key, value and metadata.
     */
    private Message message;
    /**
     * Real container wrapping {@link #message} with the mocked parser, supplied to the body builder.
     */
    private MessageContainer messageContainer;
    /**
     * Mocked message parser supplied to the {@link MessageContainer}.
     */
    @Mock
    private MessageParser parser;
    /**
     * Mocked HTTP sink configuration; replaced with a real configuration in the metadata test.
     */
    @Mock
    private HttpSinkConfig config;

    /**
     * Builds the base message fixture and its container before each test.
     *
     * <p>Creates a {@link com.gotocompany.depot.TestMessage}, wraps it into a {@link Message} whose key
     * and value are the same serialised bytes, and constructs the {@link MessageContainer} used by most
     * tests.</p>
     */
    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        message = new Message(testMessage.toByteArray(), testMessage.toByteArray());
        messageContainer = new MessageContainer(message, parser);
    }

    /**
     * Verifies that raw protobuf bytes are base64-encoded into the {@code log_key} and
     * {@code log_message} fields.
     *
     * <p>Given a message whose key and value are identical serialised bytes, when
     * {@link RawBody#build(MessageContainer)} is invoked, then the JSON body has both
     * {@code log_key} and {@code log_message} set to the same base64-encoded value.</p>
     *
     * @throws IOException never in practice; declared because building the body is a checked operation
     */
    @Test
    public void shouldWrapProtoByteInsideJson() throws IOException {
        RequestBody body = new RawBody(config);
        String rawBody = body.build(messageContainer);
        assertTrue(new JSONObject("{\"log_key\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}").similar(new JSONObject(rawBody)));
    }

    /**
     * Verifies that a missing key is rendered as an empty string rather than being omitted.
     *
     * <p>Given a message with a {@code null} key and a valid value, when
     * {@link RawBody#build(MessageContainer)} is invoked, then the JSON body has an empty
     * {@code log_key} and the base64-encoded value in {@code log_message}.</p>
     *
     * @throws IOException never in practice; declared because building the body is a checked operation
     */
    @Test
    public void shouldPutEmptyStringIfKeyIsNull() throws IOException {
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        message = new Message(null, testMessage.toByteArray());
        messageContainer = new MessageContainer(message, parser);
        RequestBody body = new RawBody(config);
        String rawBody = body.build(messageContainer);
        assertTrue(new JSONObject("{\"log_key\":\"\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}").similar(new JSONObject(rawBody)));
    }

    /**
     * Verifies that configured metadata columns are merged into the raw JSON body.
     *
     * <p>Given a message carrying {@code message_topic} and {@code message_partition} metadata and a
     * configuration that enables metadata with those typed columns, when
     * {@link RawBody#build(MessageContainer)} is invoked, then the JSON body contains the
     * base64-encoded key and message alongside {@code message_partition} ({@code 1}) and
     * {@code message_topic} ({@code "sample-topic"}).</p>
     *
     * @throws IOException never in practice; declared because building the body is a checked operation
     */
    @Test
    public void shouldAddMetadataToRawBody() throws IOException {
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        message = new Message(
                testMessage.toByteArray(),
                testMessage.toByteArray(),
                new Tuple<>("message_topic", "sample-topic"),
                new Tuple<>("message_partition", 1));
        messageContainer = new MessageContainer(message, parser);
        Map<String, String> configuration = new HashMap<>();
        configuration.put("SINK_ADD_METADATA_ENABLED", "true");
        configuration.put("SINK_METADATA_COLUMNS_TYPES", "message_partition=integer,message_topic=string");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);

        RequestBody body = new RawBody(config);
        String rawBody = body.build(messageContainer);
        assertTrue(new JSONObject("{\"message_partition\":1,\"log_key\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"message_topic\":\"sample-topic\"}").similar(new JSONObject(rawBody)));
    }

    /**
     * Verifies that a non-byte-array payload causes the body builder to fail.
     *
     * <p>Given a message whose key and value are plain strings rather than byte arrays, when
     * {@link RawBody#build(MessageContainer)} is invoked, then an {@link IOException} is thrown.</p>
     *
     * @throws IOException always, as the expected outcome of this test
     */
    @Test(expected = IOException.class)
    public void shouldThrowExceptionIfMessageIsNotBytes() throws IOException {
        message = new Message("", "test-string");
        messageContainer = new MessageContainer(message, parser);
        RequestBody body = new RawBody(config);
        body.build(messageContainer);
    }
}
