package com.gotocompany.depot.exception;

/**
 * Signals that a configured string template is missing or malformed.
 *
 * <p>Several Depot sinks build dynamic values, such as Redis keys, Bigtable row keys, and HTTP URLs,
 * headers, and query parameters, from {@link com.gotocompany.depot.common.Template} patterns. A
 * template couples a format pattern with the message field names that supply its placeholder values.
 * This exception is thrown while constructing such a template when the pattern is empty, or when the
 * number of {@code %s} placeholders does not match the number of declared field arguments.
 *
 * <p>Unlike most exceptions in this package, this is a checked exception (it extends
 * {@link Exception}), so callers must handle or declare it. Template-based converters typically catch
 * it and rethrow it as an {@link IllegalArgumentException} so that invalid configuration fails fast
 * during startup.
 */
public class InvalidTemplateException extends Exception {
    /**
     * Creates an {@code InvalidTemplateException} with the supplied detail message.
     *
     * @param message human-readable description of why the template was rejected, typically naming
     *                the offending pattern or the placeholder/argument count mismatch
     */
    public InvalidTemplateException(String message) {
        super(message);
    }
}
