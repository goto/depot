package com.gotocompany.depot.bigquery.error;

import lombok.AllArgsConstructor;

/**
 * {@link ErrorDescriptor} that identifies rows BigQuery marked as "stopped".
 *
 * <p>BigQuery returns the {@code stopped} status (HTTP 200) for the rows of an insert
 * job that was cancelled because other rows in the same batch failed. The offending
 * rows carry their own specific error, while the remaining rows are flagged as
 * {@code stopped}; those rows were not inserted but can be retried as-is.</p>
 *
 * <p>Lombok's {@link AllArgsConstructor} generates the all-arguments constructor that
 * accepts the {@code reason} field.</p>
 *
 * @see <a href="https://cloud.google.com/bigquery/docs/error-messages">BigQuery error messages</a>
 * @see ErrorParser
 */
@AllArgsConstructor
/**
 * stopped 200 This status code returns when a job is canceled.
 * This will be returned if a batch of insertion has some bad records
 * which caused the job to be cancelled. Bad records will have some *other* error
 * but rest of records will be marked as "stopped" and can be sent as is
 *
 * https://cloud.google.com/bigquery/docs/error-messages
 * */
public class StoppedError implements ErrorDescriptor {

    /** The {@code reason} code reported by BigQuery for the failed row. */
    private final String reason;

    /**
     * Returns whether the wrapped BigQuery error denotes a stopped row.
     *
     * @return {@code true} if the {@code reason} equals {@code "stopped"},
     *         {@code false} otherwise
     */
    @Override
    public boolean matches() {
        return reason.equals("stopped");
    }

    /**
     * Returns a fixed, retry-oriented description of this error.
     *
     * @return a message explaining that BigQuery encountered an error on individual
     *         rows so none of the rows were inserted, and that the request can be
     *         retried
     */
    @Override
    public String toString() {
        return "StoppedError: BigQuery encountered an error on individual rows in the request, none of the rows are inserted. This error can be retried";
    }
}
