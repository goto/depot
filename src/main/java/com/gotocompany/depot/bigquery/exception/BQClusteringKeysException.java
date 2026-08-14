package com.gotocompany.depot.bigquery.exception;

/**
 * Unchecked exception thrown when a BigQuery table's clustering keys are invalid.
 *
 * <p>Raised while building the table definition when no clustering key is configured,
 * when the number of clustering columns exceeds BigQuery's maximum, or when one or more
 * of the configured clustering columns do not exist in the schema (or refer to a nested
 * type, which is not supported for clustering).</p>
 */
public class BQClusteringKeysException extends RuntimeException {
    /**
     * Creates the exception with a message describing the clustering-key problem.
     *
     * @param message the detail message explaining why the clustering keys are invalid
     */
    public BQClusteringKeysException(String message) {
        super(message);
    }
}
