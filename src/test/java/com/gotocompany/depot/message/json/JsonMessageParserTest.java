package com.gotocompany.depot.message.json;

import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.JsonParserMetrics;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.of;
import static com.gotocompany.depot.message.SinkConnectorSchemaMessageMode.LOG_KEY;
import static com.gotocompany.depot.message.SinkConnectorSchemaMessageMode.LOG_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 * Unit tests for {@link JsonMessageParser}, which parses raw JSON payloads into a {@link ParsedMessage}
 * and records parse metrics.
 *
 * <p>The shared fixtures are a default {@link SinkConfig}, a mocked {@link Instrumentation} and a real
 * {@link JsonParserMetrics}. Tests parse log-message and log-key payloads in
 * {@link com.gotocompany.depot.message.SinkConnectorSchemaMessageMode#LOG_MESSAGE} and
 * {@link com.gotocompany.depot.message.SinkConnectorSchemaMessageMode#LOG_KEY} modes, assert the
 * resulting {@code org.json} object via {@code JSONObject.similar}, verify the parse-duration metric,
 * and cover the optional string-casting behaviour together with the error paths for nested, malformed,
 * empty and mode-less input.</p>
 */
public class JsonMessageParserTest {

    /** Default sink configuration used by most parser instances. */
    private final SinkConfig defaultConfig = ConfigFactory.create(SinkConfig.class, Collections.emptyMap());
    /** Mocked instrumentation used to verify metric capture. */
    private final Instrumentation instrumentation = mock(Instrumentation.class);
    /** Real JSON parser metrics bound to the default config. */
    private final JsonParserMetrics jsonParserMetrics = new JsonParserMetrics(defaultConfig);

    /*
            JSONObject.equals does reference check, so cant use assertEquals instead we use expectedJson.similar(actualJson)
            reference https://github.com/stleary/JSON-java/blob/master/src/test/java/org/json/junit/JSONObjectTest.java#L132
         */
    /**
     * Verifies that a valid JSON log message is parsed into an equivalent JSON object.
     *
     * <p>Given the payload {@code {"first_name":"john"}}, when it is parsed in {@code LOG_MESSAGE}
     * mode, then the raw parsed {@link JSONObject} is structurally similar to the original JSON.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldParseJsonLogMessage() throws IOException {
        JsonMessageParser jsonMessageParser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String validJsonStr = "{\"first_name\":\"john\"}";
        Message jsonMessage = new Message(null, validJsonStr.getBytes());

        ParsedMessage parsedMessage = jsonMessageParser.parse(jsonMessage, LOG_MESSAGE, null);
        JSONObject actualJson = (JSONObject) parsedMessage.getRaw();
        JSONObject expectedJsonObject = new JSONObject(validJsonStr);
        assertTrue(expectedJsonObject.similar(actualJson));
    }

    /**
     * Verifies that parsing publishes the JSON parse-duration metric.
     *
     * <p>Given a valid JSON payload, when it is parsed in {@code LOG_MESSAGE} mode, then the parsed
     * object matches the input and the {@link Instrumentation} captures the
     * {@code application_sink_json_parse_operation_milliseconds} duration exactly once.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldPublishTimeTakenToCastJsonValuesToString() throws IOException {
        JsonMessageParser jsonMessageParser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String validJsonStr = "{\"first_name\":\"john\"}";
        Message jsonMessage = new Message(null, validJsonStr.getBytes());

        ParsedMessage parsedMessage = jsonMessageParser.parse(jsonMessage, LOG_MESSAGE, null);
        JSONObject actualJson = (JSONObject) parsedMessage.getRaw();
        JSONObject expectedJsonObject = new JSONObject(validJsonStr);
        assertTrue(expectedJsonObject.similar(actualJson));
        verify(instrumentation, times(1)).captureDurationSince(
                eq("application_sink_json_parse_operation_milliseconds"), any(Instant.class));
    }

    /**
     * Verifies that all JSON scalar values are cast to strings when the option is enabled.
     *
     * <p>Given {@code SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE} set to {@code true} and a payload
     * mixing string, boolean, float and integer values plus a {@code null}, when it is parsed in
     * {@code LOG_MESSAGE} mode, then booleans, floats and integers are rendered as strings, the
     * {@code null} entry is dropped and plain strings are left unchanged.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldCastTheJSONValuesToString() throws IOException {
        Map<String, String> configMap = of("SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "true");
        SinkConfig config = ConfigFactory.create(SinkConfig.class, configMap);
        JsonMessageParser jsonMessageParser = new JsonMessageParser(config, instrumentation, jsonParserMetrics);
        String validJsonStr = "{\n"
                + "  \"idfv\": \"FE533F4A-F776-4BEF-98B7-6BD1DFC2972C\",\n"
                + "  \"is_lat\": true,\n"
                + "  \"contributor_2_af_prt\": null,\n"
                + "  \"sdk_version\": 6.310932397154218,\n"
                + "  \"whole_number\": 2\n"
                + "}";
        Message jsonMessage = new Message(null, validJsonStr.getBytes());

        ParsedMessage parsedMessage = jsonMessageParser.parse(jsonMessage, LOG_MESSAGE, null);
        JSONObject actualJson = (JSONObject) parsedMessage.getRaw();
        String stringifiedJsonStr = "{\n"
                //normal string should remain as is
                + "  \"idfv\": \"FE533F4A-F776-4BEF-98B7-6BD1DFC2972C\",\n"

                //boolean should be converted to string
                + "  \"is_lat\": \"true\",\n"
                //null will not be there entirely
                //"  \"contributor_2_af_prt\": null,\n"

                //float should be converted to string
                + "  \"sdk_version\": \"6.310932397154218\",\n"

                //integer should be converted to string
                + "  \"whole_number\": \"2\"\n"
                + "}";
        JSONObject expectedJsonObject = new JSONObject(stringifiedJsonStr);
        assertTrue(expectedJsonObject.similar(actualJson));
    }

    /**
     * Verifies that nested JSON objects are rejected as unsupported.
     *
     * <p>Given a payload containing a nested object under {@code event_value}, when it is parsed in
     * {@code LOG_MESSAGE} mode, then an {@link UnsupportedOperationException} with the message
     * {@code "nested json structure not supported yet"} is thrown.</p>
     */
    @Test
    public void shouldThrowExceptionForNestedJsonNotSupported() {
        JsonMessageParser jsonMessageParser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String nestedJsonStr = "{\n"
                + "  \"event_value\": {\n"
                + "    \"CustomerLatitude\": \"-6.166895595817224\",\n"
                + "    \"fb_content_type\": \"product\"\n"
                + "  },\n"
                + "  \"ip\": \"210.210.175.250\",\n"
                + "  \"oaid\": null,\n"
                + "  \"event_time\": \"2022-05-06 08:03:43.561\",\n"
                + "  \"is_receipt_validated\": null,\n"
                + "  \"contributor_1_campaign\": null\n"
                + "}";
        Message jsonMessage = new Message(null, nestedJsonStr.getBytes());

        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> jsonMessageParser.parse(jsonMessage, LOG_MESSAGE, null));
        assertEquals("nested json structure not supported yet", exception.getMessage());

    }


    /**
     * Verifies that a malformed log-message payload fails with a wrapped JSON error.
     *
     * <p>Given a truncated, unterminated JSON payload, when it is parsed in {@code LOG_MESSAGE} mode,
     * then an {@link IOException} with the message {@code "invalid json error"} is thrown, caused by a
     * {@link JSONException}.</p>
     */
    @Test
    public void shouldThrowErrorForInvalidLogMessage() {
        JsonMessageParser jsonMessageParser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String invalidJsonStr = "{\"first_";
        Message jsonMessage = new Message(null, invalidJsonStr.getBytes());
        IOException ioException = assertThrows(IOException.class,
                () -> jsonMessageParser.parse(jsonMessage, LOG_MESSAGE, null));
        assertEquals("invalid json error", ioException.getMessage());
        assertTrue(ioException.getCause() instanceof JSONException);
    }

    /**
     * Verifies that a null log-message payload raises an empty-message error.
     *
     * <p>Given a {@link Message} with a {@code null} value, when it is parsed in {@code LOG_MESSAGE}
     * mode, then an {@link EmptyMessageException} with the message {@code "log message is empty"} is
     * thrown.</p>
     */
    @Test
    public void shouldThrowEmptyMessageException() {
        JsonMessageParser jsonMessageParser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        Message jsonMessage = new Message(null, null);
        EmptyMessageException emptyMessageException = assertThrows(EmptyMessageException.class,
                () -> jsonMessageParser.parse(jsonMessage, LOG_MESSAGE, null));
        assertEquals("log message is empty", emptyMessageException.getMessage());
    }

    /**
     * Verifies that a valid JSON log key is parsed into an equivalent JSON object.
     *
     * <p>Given a valid JSON payload supplied as the message key, when it is parsed in {@code LOG_KEY}
     * mode, then the raw parsed {@link JSONObject} is structurally similar to the original JSON.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldParseJsonKeyMessage() throws IOException {
        JsonMessageParser jsonMessageParser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String validJsonStr = "{\"first_name\":\"john\"}";
        Message jsonMessage = new Message(validJsonStr.getBytes(), null);

        ParsedMessage parsedMessage = jsonMessageParser.parse(jsonMessage, LOG_KEY, null);
        JSONObject actualJson = (JSONObject) parsedMessage.getRaw();
        JSONObject expectedJsonObject = new JSONObject(validJsonStr);
        assertTrue(expectedJsonObject.similar(actualJson));
    }

    /**
     * Verifies that a malformed log-key payload fails with a wrapped JSON error.
     *
     * <p>Given a truncated, unterminated JSON payload supplied as the message key, when it is parsed
     * in {@code LOG_KEY} mode, then an {@link IOException} with the message {@code "invalid json error"}
     * is thrown, caused by a {@link JSONException}.</p>
     */
    @Test
    public void shouldThrowErrorForInvalidKeyMessage() {
        JsonMessageParser jsonMessageParser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String invalidJsonStr = "{\"first_";
        Message jsonMessage = new Message(invalidJsonStr.getBytes(), null);
        IOException ioException = assertThrows(IOException.class,
                () -> jsonMessageParser.parse(jsonMessage, LOG_KEY, null));
        assertEquals("invalid json error", ioException.getMessage());
        assertTrue(ioException.getCause() instanceof JSONException);
    }

    /**
     * Verifies that parsing without a message mode is rejected.
     *
     * <p>Given any payload and a {@code null} message mode, when it is parsed, then an
     * {@link IOException} with the message {@code "message mode not defined"} is thrown.</p>
     */
    @Test
    public void shouldThrowErrorWhenModeNotDefined() {
        JsonMessageParser jsonMessageParser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        String invalidJsonStr = "{\"first_";
        Message jsonMessage = new Message(invalidJsonStr.getBytes(), null);
        IOException ioException = assertThrows(IOException.class,
                () -> jsonMessageParser.parse(jsonMessage, null, null));
        assertEquals("message mode not defined", ioException.getMessage());
    }
}
