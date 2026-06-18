package com.gotocompany.depot.bigtable.exception;

/**
 * Signals that a Bigtable destination does not match the schema the sink requires.
 *
 * <p>Raised while validating the Bigtable target before writing — specifically when the configured
 * table cannot be found, or when one or more column families declared in the column-family mapping do
 * not exist on the table. It is an unchecked exception because it reflects a configuration or
 * provisioning problem with the destination that cannot be resolved by retrying; the table or its
 * column families must be created, or the mapping corrected.</p>
 *
 * @see com.gotocompany.depot.bigtable.client.BigTableClient#validateBigTableSchema()
 */
public class BigTableInvalidSchemaException extends RuntimeException {
    /**
     * Creates the exception with a detail message and an underlying cause.
     *
     * @param message human-readable description of the schema problem
     * @param cause the underlying throwable that surfaced the problem
     */
    public BigTableInvalidSchemaException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates the exception with a detail message and no cause.
     *
     * @param messsage human-readable description of the schema problem
     */
    public BigTableInvalidSchemaException(String messsage) {
        super(messsage);
    }
}
