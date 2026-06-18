package com.gotocompany.depot.bigquery.error;

import lombok.AllArgsConstructor;

/**
 * {@link ErrorDescriptor} that identifies out-of-bounds partitioning failures.
 *
 * <p>BigQuery rejects a row when the value of its partitioning column falls outside the
 * table's allowed partition window. In practice this happens when the partitioned
 * column holds a date that is too far in the past (historically more than around five
 * years) or too far in the future (more than one year), which BigQuery reports either
 * as the value being outside the allowed bounds (mentioning a number of days in the
 * past and in the future) or as being "out of range".</p>
 *
 * <p>The classification is derived from the BigQuery {@code reason} and {@code message}
 * strings of a failed insertion. Lombok's {@link AllArgsConstructor} generates the
 * all-arguments constructor that accepts the {@code reason} and {@code message}
 * fields.</p>
 *
 * @see ErrorParser
 */
@AllArgsConstructor
/**
 * Out of bounds are caused when the partitioned column has a date value less than
 * 5 years and more than 1 year in future
 * */
public class OOBError implements ErrorDescriptor {

    /** The {@code reason} code reported by BigQuery for the failed row. */
    private final String reason;
    /** The descriptive {@code message} reported by BigQuery for the failed row. */
    private final String message;

    /**
     * Returns whether the wrapped BigQuery error denotes an out-of-bounds partition value.
     *
     * <p>Matches when the {@code reason} equals {@code "invalid"} and the
     * {@code message} either states that the value is outside the allowed bounds
     * (containing both the {@code "days in the past and"} and
     * {@code "days in the future"} phrases) or contains the {@code "out of range"}
     * phrase.</p>
     *
     * @return {@code true} if the error matches the out-of-bounds signature,
     *         {@code false} otherwise
     */
    @Override
    public boolean matches() {
        return reason.equals("invalid")
               && ((message.contains("is outside the allowed bounds") && message.contains("days in the past and") && message.contains("days in the future"))
                   || message.contains("out of range"));
    }

    /**
     * Returns a human readable representation of this error.
     *
     * @return the string {@code "OOBError: "} followed by the original BigQuery message
     */
    @Override
    public String toString() {
        return String.format("OOBError: %s", message);
    }
}
