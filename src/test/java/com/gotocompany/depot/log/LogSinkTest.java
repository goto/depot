package com.gotocompany.depot.log;

import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.json.JsonMessageParser;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.JsonParserMetrics;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link LogSink}, the sink that logs parsed messages rather than writing them to an
 * external system.
 *
 * <p>Each test builds a {@link LogSink} from a mocked {@link SinkConfig}, a {@link MessageParser}
 * (either a Mockito mock or a real {@link JsonMessageParser}) and a mocked {@link Instrumentation}.
 * The batch pipeline is exercised through {@link LogSink#pushToSink(java.util.List)} and the returned
 * {@link SinkResponse}, along with the interactions captured on the {@link Instrumentation} mock, is
 * asserted. The shared {@link #template} mirrors the layout the sink uses to render the data and
 * metadata sections of each log line.</p>
 */
public class LogSinkTest {
    /** Log format string shared with the sink: a data section followed by a metadata section. */
    private final String template = "\n================= DATA =======================\n{}\n================= METADATA =======================\n{}\n";
    /** Mocked sink configuration supplied to the sink and to the metrics under test. */
    private SinkConfig config;
    /** Message parser used by the sink; a Mockito mock or a real {@link JsonMessageParser} per test. */
    private MessageParser messageParser;
    /** Mocked instrumentation used to assert logging and metric interactions. */
    private Instrumentation instrumentation;
    /** JSON parser metrics bound to the mocked config and passed to the real JSON parser. */
    private JsonParserMetrics jsonParserMetrics;

    /**
     * Initialises the shared fixtures before each test.
     *
     * <p>Creates Mockito mocks for the {@link SinkConfig}, {@link MessageParser} and
     * {@link Instrumentation}, and a real {@link JsonParserMetrics} bound to the mocked config.</p>
     *
     * @throws Exception if fixture initialisation fails
     */
    @Before
    public void setUp() throws Exception {
        config = mock(SinkConfig.class);
        messageParser = mock(MessageParser.class);
        instrumentation = mock(Instrumentation.class);
        jsonParserMetrics = new JsonParserMetrics(config);

    }

    /**
     * Verifies that pushing an empty batch is a no-op that reports no errors.
     *
     * <p>Given a {@link LogSink} and an empty message list, when
     * {@link LogSink#pushToSink(java.util.List)} is invoked, then the returned {@link SinkResponse}
     * carries an empty error map and neither the message parser nor the instrumentation is invoked.</p>
     *
     * @throws IOException if the sink push fails
     */
    @Test
    public void shouldProcessEmptyMessageWithNoError() throws IOException {
        LogSink logSink = new LogSink(config, messageParser, instrumentation);
        ArrayList<Message> messages = new ArrayList<>();
        SinkResponse sinkResponse = logSink.pushToSink(messages);
        Map<Long, ErrorInfo> errors = sinkResponse.getErrors();

        assertEquals(Collections.emptyMap(), errors);
        verify(messageParser, never()).parse(any(), any(), any());
        verify(instrumentation, never()).logInfo(any(), any(), any());
    }

    /**
     * Verifies that valid JSON messages are parsed and logged without errors.
     *
     * <p>Given a sink configured in {@code log_message} mode with a real {@link JsonMessageParser},
     * when two valid JSON payloads are pushed, then the response contains no errors and the
     * {@link Instrumentation} logs both payloads exactly twice using the shared {@link #template} and
     * an empty metadata map. The captured log arguments are asserted to contain both payloads in any
     * order.</p>
     *
     * @throws SinkException if the sink push fails
     */
    @Test
    public void shouldLogJsonMessages() throws SinkException {
        HashMap<String, String> configMap = new HashMap<String, String>() {{
            put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "log_message");
        }};
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);
        messageParser = new JsonMessageParser(sinkConfig, instrumentation, jsonParserMetrics);
        LogSink logSink = new LogSink(sinkConfig, messageParser, instrumentation);
        ArrayList<Message> messages = new ArrayList<>();
        String validJsonFirstName = "{\"first_name\":\"john\"}";
        byte[] logMessage1 = validJsonFirstName.getBytes();
        String validJsonLastName = "{\"last_name\":\"doe\"}";
        byte[] logMessage2 = validJsonLastName.getBytes();
        messages.add(new Message(null, logMessage1));
        messages.add(new Message(null, logMessage2));
        SinkResponse sinkResponse = logSink.pushToSink(messages);

        //assert no error
        Map<Long, ErrorInfo> errors = sinkResponse.getErrors();
        assertEquals(Collections.emptyMap(), errors);

        //assert processed message
        ArgumentCaptor<String> jsonStrCaptor = ArgumentCaptor.forClass(String.class);
        verify(instrumentation, times(2)).logInfo(eq(template), jsonStrCaptor.capture(), eq(Collections.emptyMap().toString()));
        assertThat(jsonStrCaptor.getAllValues(), containsInAnyOrder(validJsonFirstName, validJsonLastName));
    }

    /**
     * Verifies that an invalid message is reported as an error while valid messages are still logged.
     *
     * <p>Given a sink in {@code log_message} mode fed one valid and one malformed JSON payload, when
     * the batch is pushed, then the response records an {@link ErrorType#DESERIALIZATION_ERROR} for
     * the malformed entry at index {@code 1} and the single valid payload is logged exactly once with
     * the shared {@link #template}.</p>
     *
     * @throws SinkException if the sink push fails
     */
    @Test
    public void shouldReturnErrorResponseAndProcessValidMessage() throws SinkException {
        HashMap<String, String> configMap = new HashMap<String, String>() {{
            put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "log_message");
        }};
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);

        messageParser = new JsonMessageParser(sinkConfig, instrumentation, jsonParserMetrics);
        LogSink logSink = new LogSink(sinkConfig, messageParser, instrumentation);
        ArrayList<Message> messages = new ArrayList<>();
        String validJsonFirstName = "{\"first_name\":\"john\"}";
        byte[] logMessage1 = validJsonFirstName.getBytes();
        String invalidJson = "{\"last_name";
        byte[] invalidLogMessage = invalidJson.getBytes();
        messages.add(new Message(null, logMessage1));
        messages.add(new Message(null, invalidLogMessage));
        SinkResponse sinkResponse = logSink.pushToSink(messages);

        //assert error
        ErrorInfo error = sinkResponse.getErrorsFor(1L);
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, error.getErrorType());

        //assert valid message processed
        ArgumentCaptor<String> jsonStrCaptor = ArgumentCaptor.forClass(String.class);
        verify(instrumentation, times(1)).logInfo(eq(template), jsonStrCaptor.capture(), eq(Collections.emptyMap().toString()));
        assertEquals(validJsonFirstName, jsonStrCaptor.getValue().toString());
    }
}
