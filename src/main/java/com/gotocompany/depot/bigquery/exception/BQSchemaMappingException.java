package com.gotocompany.depot.bigquery.exception;

/**
 * Unchecked exception thrown when a protobuf field cannot be mapped to a BigQuery schema.
 *
 * <p>Raised when no BigQuery type mapping exists for a protobuf field while generating
 * the schema, and when a configured metadata field collides with a field that is
 * already present in the generated schema.</p>
 */
public class BQSchemaMappingException extends RuntimeException {
    /**
     * Creates the exception with a message describing the schema-mapping problem.
     *
     * @param message the detail message explaining the unmapped field or naming conflict
     */
    public BQSchemaMappingException(String message) {
        super(message);
    }
}
