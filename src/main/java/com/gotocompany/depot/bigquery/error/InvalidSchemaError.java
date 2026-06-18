package com.gotocompany.depot.bigquery.error;

import lombok.AllArgsConstructor;

/**
 * {@link ErrorDescriptor} that identifies BigQuery "invalid input" failures caused by a
 * row not matching the destination table schema.
 *
 * <p>This error is reported when BigQuery rejects a row for reasons other than an
 * invalid query, for example referencing a column that does not exist in the table
 * (a "no such field" message), missing required fields, or otherwise violating the
 * table schema. The {@code reason} and {@code message} strings are taken from the
 * BigQuery error of a failed insertion.</p>
 *
 * <p>Lombok's {@link AllArgsConstructor} generates the all-arguments constructor that
 * accepts the {@code reason} and {@code message} fields.</p>
 *
 * @see <a href="https://cloud.google.com/bigquery/docs/error-messages">BigQuery error messages</a>
 * @see ErrorParser
 */
@AllArgsConstructor
/**
 * This error returns when there is any kind of invalid input
 * other than an invalid query, such as missing required fields
 * or an invalid table schema.
 *
 * https://cloud.google.com/bigquery/docs/error-messages
 * */
public class InvalidSchemaError implements ErrorDescriptor {

    /** The {@code reason} code reported by BigQuery for the failed row. */
    private final String reason;
    /** The descriptive {@code message} reported by BigQuery for the failed row. */
    private final String message;

    /**
     * Returns whether the wrapped BigQuery error denotes an invalid schema failure.
     *
     * <p>Matches when the {@code reason} equals {@code "invalid"} and the
     * {@code message} contains the phrase {@code "no such field"}.</p>
     *
     * @return {@code true} if the error matches the invalid-schema signature,
     *         {@code false} otherwise
     */
    @Override
    public boolean matches() {
        return reason.equals("invalid") && message.contains("no such field");
    }

    /**
     * Returns a human readable representation of this error.
     *
     * @return the string {@code "InvalidSchemaError: "} followed by the original
     *         BigQuery message
     */
    @Override
    public String toString() {
        return String.format("InvalidSchemaError: %s", message);
    }
}
