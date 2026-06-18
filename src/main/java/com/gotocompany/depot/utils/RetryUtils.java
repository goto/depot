package com.gotocompany.depot.utils;

import com.gotocompany.depot.exception.NonRetryableException;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Predicate;

/**
 * Utility that retries a fallible operation with a fixed back-off and a caller-supplied retry policy.
 *
 * <p>{@code RetryUtils} runs a {@link RunnableWithException} and, when it throws, consults a
 * {@link java.util.function.Predicate} to decide whether the failure is worth retrying. Retryable
 * failures are re-attempted, sleeping for a fixed number of milliseconds between attempts, until
 * either the operation succeeds or the configured maximum number of attempts is reached. A
 * non-retryable failure aborts immediately. In both terminal cases — a rejected exception or
 * exhausted retries — the failure is rethrown wrapped in a
 * {@link com.gotocompany.depot.exception.NonRetryableException}.</p>
 *
 * <p>Note that an {@link InterruptedException} raised while sleeping between attempts is logged but
 * otherwise swallowed, and the interrupt status is not re-asserted on the thread.</p>
 */
@Slf4j
public class RetryUtils {

    /**
     * Executes an operation, retrying it on retryable failures with a fixed back-off between attempts.
     *
     * <p>The operation is invoked up to {@code maxRetries} times. On success the method returns
     * immediately. When an attempt throws, {@code retryPredicate} classifies the exception: if it is
     * retryable the attempt is counted, the exception is remembered, and after sleeping for
     * {@code backoffMillis} milliseconds the operation is tried again; if it is not retryable the
     * method aborts at once by throwing a
     * {@link com.gotocompany.depot.exception.NonRetryableException} that wraps it. If every allowed
     * attempt fails with retryable exceptions, the method finally throws a
     * {@link com.gotocompany.depot.exception.NonRetryableException} carrying the last observed exception
     * as its cause.</p>
     *
     * @param runnableWithException the operation to execute, which may throw a checked exception
     * @param maxRetries the maximum number of attempts before giving up
     * @param backoffMillis the number of milliseconds to sleep between successive attempts
     * @param retryPredicate predicate returning {@code true} for exceptions that should be retried and
     *     {@code false} for exceptions that must abort the operation immediately
     * @throws com.gotocompany.depot.exception.NonRetryableException if the operation throws an exception
     *     the predicate rejects, or if all {@code maxRetries} attempts fail with retryable exceptions
     */
    public static void executeWithRetry(RunnableWithException runnableWithException,
                                        int maxRetries,
                                        long backoffMillis,
                                        Predicate<Exception> retryPredicate) {
        int retryCount = 0;
        Exception lastException = null;
        while (retryCount < maxRetries) {
            try {
                runnableWithException.run();
                break;
            } catch (Exception e) {
                if (retryPredicate.test(e)) {
                    retryCount++;
                    lastException = e;
                    log.info("Retrying operation, retry count: {}", retryCount);
                } else {
                    log.error("Non-retryable exception occurred, aborting operation", e);
                    throw new NonRetryableException(e.getMessage(), e);
                }
            }
            try {
                Thread.sleep(backoffMillis);
            } catch (InterruptedException e) {
                log.error("Thread interrupted while sleeping", e);
            }
        }
        if (retryCount == maxRetries) {
            log.error("Max retries reached, aborting operation");
            throw new NonRetryableException("Max retries reached, aborting operation", lastException);
        }
    }

    /**
     * Functional interface for an operation that may throw a checked exception.
     *
     * <p>Used by
     * {@link RetryUtils#executeWithRetry(RunnableWithException, int, long, java.util.function.Predicate)}
     * to represent the unit of work to attempt. Unlike {@link Runnable}, its single method is permitted
     * to throw any {@link Exception}, allowing callers to pass operations that perform I/O or other
     * checked-exception-throwing work.</p>
     */
    @FunctionalInterface
    public interface RunnableWithException {
        /**
         * Runs the operation.
         *
         * @throws Exception if the operation fails
         */
        void run() throws Exception;
    }

}
