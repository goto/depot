package com.gotocompany.depot.utils;

import com.gotocompany.depot.exception.NonRetryableException;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link RetryUtils#executeWithRetry(RetryUtils.RunnableWithException, int, long,
 * java.util.function.Predicate)}, covering the retry, exhaustion and non-retryable paths.
 *
 * <p>Each test drives a Mockito spy over a {@link RunnableMock} that fails a fixed number of times
 * before succeeding, and verifies the observable outcome and, where reachable, the number of
 * execution attempts. A {@link NonRetryableException} is expected when the supplied predicate rejects
 * the thrown exception.
 */
public class RetryUtilsTest {

    /**
     * Verifies that the operation is retried until it succeeds.
     *
     * <p>Wraps a {@link RunnableMock} that fails twice and succeeds on its third attempt, runs it with
     * a retry limit of five and a predicate that retries any {@link Exception}, and verifies
     * {@code execute} is invoked exactly three times.
     *
     * @throws Exception if the retried operation propagates an unexpected exception
     */
    @Test
    public void shouldRetryUntilSuccess() throws Exception {
        int repeatCountBeforeSuccess = 3;
        RunnableMock runnableMock = Mockito.spy(new RunnableMock(repeatCountBeforeSuccess));

        RetryUtils.executeWithRetry(runnableMock::execute, 5, 0, e -> e instanceof Exception);

        Mockito.verify(runnableMock, Mockito.times(repeatCountBeforeSuccess)).execute();
    }

    /**
     * Verifies that a failure the predicate does not accept surfaces a {@link NonRetryableException}.
     *
     * <p>Runs a {@link RunnableMock} that would need five attempts to succeed with a retry limit of
     * three. Because the supplied predicate only retries {@link RuntimeException} while the mock throws
     * a checked {@link Exception}, the first failure is classified as non-retryable and a
     * {@link NonRetryableException} is thrown at once, satisfying the {@code expected} declaration
     * before the trailing verification is reached.
     *
     * @throws Exception if the operation propagates an exception other than the expected one
     */
    @Test(expected = NonRetryableException.class)
    public void shouldThrowNonRetryableExceptionAfterRetryIsExhausted() throws Exception {
        int repeatCountBeforeSuccess = 5;
        RunnableMock runnableMock = Mockito.spy(new RunnableMock(repeatCountBeforeSuccess));

        RetryUtils.executeWithRetry(runnableMock::execute, 3, 0, e -> e instanceof RuntimeException);

        Mockito.verify(runnableMock, Mockito.times(3)).execute();
    }

    /**
     * Verifies that an exception not matched by the predicate aborts immediately as non-retryable.
     *
     * <p>Runs a {@link RunnableMock} with a predicate that retries only
     * {@link IllegalArgumentException}; since the mock throws a checked {@link Exception}, the first
     * failure is rejected by the predicate and a {@link NonRetryableException} is thrown without
     * further attempts.
     *
     * @throws Exception if the operation propagates an exception other than the expected one
     */
    @Test(expected = NonRetryableException.class)
    public void shouldThrowNonRetryableExceptionWhenNonMatchingExceptionIsThrown() throws Exception {
        RunnableMock runnableMock = Mockito.spy(new RunnableMock(3));

        RetryUtils.executeWithRetry(runnableMock::execute, 3, 0, e -> e instanceof IllegalArgumentException);

        Mockito.verify(runnableMock, Mockito.times(1)).execute();
    }

    /**
     * Test double that throws a fixed number of times before completing successfully.
     *
     * <p>Used as a Mockito spy so the number of {@link #execute()} invocations can be verified. The
     * instance throws a checked {@link Exception} on every call until the configured success attempt
     * is reached.
     */
    private static class RunnableMock {
        /**
         * The attempt number on which {@link #execute()} first completes without throwing.
         */
        private final int repeatCountBeforeSuccess;
        /**
         * The number of times {@link #execute()} has been invoked so far.
         */
        private int repeatCount;

        /**
         * Creates a mock that succeeds only once the given attempt number is reached.
         *
         * @param repeatCountBeforeSuccess the attempt on which execution first succeeds
         */
        RunnableMock(int repeatCountBeforeSuccess) {
            this.repeatCountBeforeSuccess = repeatCountBeforeSuccess;
            this.repeatCount = 0;
        }

        /**
         * Records an invocation and throws until the success threshold is reached.
         *
         * @throws Exception on every invocation before the configured success attempt
         */
        void execute() throws Exception {
            repeatCount++;
            if (repeatCount < repeatCountBeforeSuccess) {
                throw new Exception("Mock exception");
            }
        }
    }

}
