package com.gotocompany.depot.bigquery.exception;

/**
 * Unchecked exception thrown when the BigQuery table's partition key is missing.
 *
 * <p>Raised while building the table definition when partitioning is enabled but no
 * partition key is configured, or when the configured partition key is not present in
 * the table schema.</p>
 */
public class BQPartitionKeyNotSpecified extends RuntimeException {
    /**
     * Creates the exception with a message describing the partition-key problem.
     *
     * @param message the detail message explaining why the partition key is missing or
     *                invalid
     */
    public BQPartitionKeyNotSpecified(String message) {
        super(message);
    }
}
