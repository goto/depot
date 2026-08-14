package com.gotocompany.depot.metrics;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link Instrumentation}, the facade that pairs SLF4J logging with StatsD metric
 * and error-event emission.
 *
 * <p>The suite runs with {@link MockitoJUnitRunner}, which initializes the {@link Mock}-annotated
 * {@link StatsDReporter} and {@link Logger} collaborators. {@link #setUp()} constructs a real
 * {@link Instrumentation} over those mocks, so each test can verify exactly which delegate calls the
 * instrumentation performs without producing real log output or StatsD network traffic.
 *
 * <p>The logging tests assert that the SLF4J template and its arguments are forwarded verbatim at
 * the expected level; the telemetry tests assert that the matching {@link StatsDReporter} method is
 * invoked; and the error-capture tests additionally assert that an error event tagged with the
 * originating exception class and severity is recorded.
 */
@RunWith(MockitoJUnitRunner.class)
public class InstrumentationTest {
    /**
     * Mocked reporter used to verify metric and error-event emission without real StatsD traffic.
     */
    @Mock
    private StatsDReporter statsDReporter;
    /**
     * Mocked SLF4J logger used to verify logging delegation without producing real log output.
     */
    @Mock
    private Logger logger;

    /**
     * Instrumentation under test, constructed in {@link #setUp()} over the mocked reporter and logger.
     */
    private Instrumentation instrumentation;
    /**
     * Plain log-message fixture ({@code "test"}) reused by the logging and error-capture scenarios.
     */
    private String testMessage;
    /**
     * SLF4J parameterized template fixture ({@code "test: {},{},{}"}) reused across the scenarios.
     */
    private String testTemplate;
    /**
     * Exception fixture whose class name and message drive the error-event tag assertions.
     */
    private Exception e;

    /**
     * Initializes the shared fixtures before each test.
     *
     * <p>Constructs the {@link Instrumentation} under test over the mocked {@link StatsDReporter} and
     * {@link Logger}, and seeds the reusable {@link #testMessage}, {@link #testTemplate} and
     * {@link #e} exception fixtures.
     */
    @Before
    public void setUp() {
        instrumentation = new Instrumentation(statsDReporter, logger);
        testMessage = "test";
        testTemplate = "test: {},{},{}";
        e = new Exception();
    }

    /**
     * Verifies that {@link Instrumentation#logInfo(String, Object...)} forwards a plain message to
     * the backing logger.
     *
     * <p>Given the {@link #testMessage} with no template arguments, when {@code logInfo} is invoked,
     * then {@link Logger#info(String, Object...)} is called exactly once with that message and an
     * empty argument array.
     */
    @Test
    public void shouldLogString() {
        instrumentation.logInfo(testMessage);
        verify(logger, times(1)).info(testMessage, new Object[]{});
    }

    /**
     * Verifies that {@link Instrumentation#logInfo(String, Object...)} forwards a parameterized
     * template together with its substitution arguments.
     *
     * <p>Given the {@link #testTemplate} and the arguments {@code 1, 2, 3}, when {@code logInfo} is
     * invoked, then {@link Logger#info(String, Object...)} is called exactly once with the same
     * template and arguments.
     */
    @Test
    public void shouldLogStringTemplate() {
        instrumentation.logInfo(testTemplate, 1, 2, 3);
        verify(logger, times(1)).info(testTemplate, 1, 2, 3);
    }

    /**
     * Verifies that {@link Instrumentation#logWarn(String, Object...)} forwards a parameterized
     * template and its arguments at {@code WARN} level.
     *
     * <p>Given the {@link #testTemplate} and the arguments {@code 1, 2, 3}, when {@code logWarn} is
     * invoked, then {@link Logger#warn(String, Object...)} is called exactly once with the same
     * template and arguments.
     */
    @Test
    public void shouldLogWarnStringTemplate() {
        instrumentation.logWarn(testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(testTemplate, 1, 2, 3);
    }

    /**
     * Verifies that {@link Instrumentation#logDebug(String, Object...)} forwards a parameterized
     * template and its arguments at {@code DEBUG} level.
     *
     * <p>Given the {@link #testTemplate} and the arguments {@code 1, 2, 3}, when {@code logDebug} is
     * invoked, then {@link Logger#debug(String, Object...)} is called exactly once with the same
     * template and arguments.
     */
    @Test
    public void shouldLogDebugStringTemplate() {
        instrumentation.logDebug(testTemplate, 1, 2, 3);
        verify(logger, times(1)).debug(testTemplate, 1, 2, 3);
    }

    /**
     * Verifies that {@link Instrumentation#logError(String, Object...)} forwards a parameterized
     * template and its arguments at {@code ERROR} level.
     *
     * <p>Given the {@link #testTemplate} and the arguments {@code 1, 2, 3}, when {@code logError} is
     * invoked, then {@link Logger#error(String, Object...)} is called exactly once with the same
     * template and arguments.
     */
    @Test
    public void shouldLogErrorStringTemplate() {
        instrumentation.logError(testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(testTemplate, 1, 2, 3);
    }

    /**
     * Verifies that
     * {@link Instrumentation#captureNonFatalError(String, Throwable, String, Object...)} both logs
     * the failure and records a non-fatal error event when given a plain message.
     *
     * <p>Given a metric name, the {@link #e} exception and the plain {@link #testMessage}, when the
     * non-fatal error is captured, then the message is logged once at {@code WARN} level, the
     * exception's own message and stack trace are logged once at {@code WARN} level, and a single
     * event carrying {@link SinkMetrics#NON_FATAL_ERROR} is recorded with a tag built from the
     * exception's class name and the non-fatal severity.
     */
    @Test
    public void shouldCaptureNonFatalErrorWithStringMessage() {
        instrumentation.captureNonFatalError("test_metric", e, testMessage);
        verify(logger, times(1)).warn(testMessage, new Object[]{});
        verify(logger, times(1)).warn(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent("test_metric", SinkMetrics.NON_FATAL_ERROR, SinkMetrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + SinkMetrics.NON_FATAL_ERROR);
    }

    /**
     * Verifies that
     * {@link Instrumentation#captureNonFatalError(String, Throwable, String, Object...)} logs a
     * parameterized template and records a non-fatal error event.
     *
     * <p>Given a metric name, the {@link #e} exception, the {@link #testTemplate} and the arguments
     * {@code 1, 2, 3}, when the non-fatal error is captured, then the template and arguments are
     * logged once at {@code WARN} level, the exception's message and stack trace are logged once at
     * {@code WARN} level, and a single event carrying {@link SinkMetrics#NON_FATAL_ERROR} is recorded
     * with a tag built from the exception's class name and the non-fatal severity.
     */
    @Test
    public void shouldCaptureNonFatalErrorWithStringTemplate() {
        instrumentation.captureNonFatalError("test_metric", e, testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent("test_metric", SinkMetrics.NON_FATAL_ERROR, SinkMetrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + SinkMetrics.NON_FATAL_ERROR);
    }

    /**
     * Verifies that {@link Instrumentation#captureFatalError(String, Throwable, String, Object...)}
     * both logs the failure and records a fatal error event when given a plain message.
     *
     * <p>Given a metric name, the {@link #e} exception and the plain {@link #testMessage}, when the
     * fatal error is captured, then the message is logged once at {@code ERROR} level, the
     * exception's own message and stack trace are logged once at {@code ERROR} level, and a single
     * event carrying {@link SinkMetrics#FATAL_ERROR} is recorded with a tag built from the
     * exception's class name and the fatal severity.
     */
    @Test
    public void shouldCaptureFatalErrorWithStringMessage() {
        instrumentation.captureFatalError("test_metric", e, testMessage);
        verify(logger, times(1)).error(testMessage, new Object[]{});
        verify(logger, times(1)).error(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent("test_metric", SinkMetrics.FATAL_ERROR, SinkMetrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + SinkMetrics.FATAL_ERROR);
    }

    /**
     * Verifies that {@link Instrumentation#captureFatalError(String, Throwable, String, Object...)}
     * logs a parameterized template and records a fatal error event.
     *
     * <p>Given the {@code "test"} metric name, the {@link #e} exception, the {@link #testTemplate}
     * and the arguments {@code 1, 2, 3}, when the fatal error is captured, then the template and
     * arguments are logged once at {@code ERROR} level, the exception's message and stack trace are
     * logged once at {@code ERROR} level, and a single event carrying {@link SinkMetrics#FATAL_ERROR}
     * is recorded with a tag built from the exception's class name and the fatal severity.
     */
    @Test
    public void shouldCaptureFatalErrorWithStringTemplate() {
        instrumentation.captureFatalError("test", e, testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent("test", SinkMetrics.FATAL_ERROR, SinkMetrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + SinkMetrics.FATAL_ERROR);
    }

    /**
     * Verifies that {@link Instrumentation#captureCount(String, Long, String...)} forwards the
     * metric, count and tags to the reporter unchanged.
     *
     * <p>Given a metric name, a count of {@code 1} and the HTTP status-code and URL tags, when
     * {@code captureCount} is invoked, then
     * {@link StatsDReporter#captureCount(String, Long, String...)} is called exactly once with the
     * same metric, count and tags.
     */
    @Test
    public void shouldCaptureCountWithTags() {
        String metric = "test_metric";
        String urlTag = "url=test";
        String httpCodeTag = "status_code=200";
        instrumentation.captureCount(metric, 1L, httpCodeTag, urlTag);
        verify(statsDReporter, times(1)).captureCount(metric, 1L, httpCodeTag, urlTag);
    }

    /**
     * Verifies that {@link Instrumentation#incrementCounter(String, String...)} delegates to the
     * reporter's increment with the supplied tag.
     *
     * <p>Given a metric name and a status-code tag, when {@code incrementCounter} is invoked, then
     * {@link StatsDReporter#increment(String, String...)} is called exactly once with the same metric
     * and tag.
     */
    @Test
    public void shouldIncrementCounterWithTags() {
        String metric = "test_metric";
        String httpCodeTag = "status_code=200";
        instrumentation.incrementCounter(metric, httpCodeTag);
        verify(statsDReporter, times(1)).increment(metric, httpCodeTag);
    }

    /**
     * Verifies that {@link Instrumentation#incrementCounter(String, String...)} delegates to the
     * reporter's increment when no tags are supplied.
     *
     * <p>Given only a metric name, when {@code incrementCounter} is invoked, then
     * {@link StatsDReporter#increment(String, String...)} is called exactly once with that metric and
     * an empty tag array.
     */
    @Test
    public void shouldIncrementCounter() {
        String metric = "test_metric";
        instrumentation.incrementCounter(metric);
        verify(statsDReporter, times(1)).increment(metric, new String[]{});
    }

    /**
     * Verifies that {@link Instrumentation#close()} closes the underlying reporter.
     *
     * <p>When the instrumentation is closed, then {@link StatsDReporter#close()} is invoked exactly
     * once.
     *
     * @throws IOException if closing the instrumentation fails; declared because
     *                     {@link Instrumentation#close()} declares it
     */
    @Test
    public void shouldClose() throws IOException {
        instrumentation.close();
        verify(statsDReporter, times(1)).close();
    }
}
