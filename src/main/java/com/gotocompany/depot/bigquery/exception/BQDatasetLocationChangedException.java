package com.gotocompany.depot.bigquery.exception;

/**
 * Unchecked exception thrown when an existing BigQuery dataset's location would change.
 *
 * <p>BigQuery does not allow a dataset's geographic location to be modified after
 * creation. This exception is raised when the configured dataset location differs from
 * the location of the already-existing dataset, signalling that the change is not
 * permitted.</p>
 */
public class BQDatasetLocationChangedException extends RuntimeException {
    /**
     * Creates the exception with a message describing the attempted location change.
     *
     * @param message the detail message describing the original and requested locations
     */
    public BQDatasetLocationChangedException(String message) {
        super(message);
    }
}

