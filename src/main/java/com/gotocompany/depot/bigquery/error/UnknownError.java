package com.gotocompany.depot.bigquery.error;

import lombok.AllArgsConstructor;

/**
 * {@link ErrorDescriptor} used as the fallback when no known descriptor matches.
 *
 * <p>{@link ErrorParser} returns an instance of this class when a BigQuery error does
 * not correspond to any of the recognised categories. Because it represents an
 * unclassified error, its {@link #matches()} method always returns {@code false}; it
 * exists to carry the original {@code reason} and {@code message} for reporting.</p>
 *
 * <p>Lombok's {@link AllArgsConstructor} generates the all-arguments constructor that
 * accepts the {@code reason} and {@code message} fields.</p>
 *
 * @see ErrorParser
 */
@AllArgsConstructor
/**
 * UnknownError is used when error factory failed to match any possible
 * known errors
 * */
public class UnknownError implements ErrorDescriptor {

    /** The {@code reason} code reported by BigQuery for the failed row, if any. */
    private String reason;
    /** The descriptive {@code message} reported by BigQuery for the failed row, if any. */
    private String message;

    /**
     * Always reports that this fallback descriptor does not match.
     *
     * <p>An {@code UnknownError} is only produced once every known descriptor has
     * already been rejected, so it never claims a match itself.</p>
     *
     * @return {@code false} always
     */
    @Override
    public boolean matches() {
        return false;
    }

    /**
     * Returns a human readable representation of this error.
     *
     * <p>Uses the original {@code reason} as the label when it is non-empty, otherwise
     * falls back to the literal {@code "UnknownError"}; the message portion is the
     * original message, or an empty string when the message is {@code null}.</p>
     *
     * @return the formatted {@code "reason: message"} string
     */
    @Override
    public String toString() {
        return String.format("%s: %s", !reason.equals("") ? reason : "UnknownError", message != null ? message : "");
    }
}
