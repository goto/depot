package com.gotocompany.depot.maxcompute.exception;

/**
 * Unchecked exception that signals a failure while creating, updating, or otherwise operating on a
 * MaxCompute (Alibaba ODPS) table.
 *
 * <p>The MaxCompute sink interacts with the ODPS backend through checked APIs that may throw
 * {@code com.aliyun.odps.OdpsException}. To keep the sink's processing pipeline free of checked
 * exception handling, those low-level failures are translated into this runtime exception, which
 * carries a descriptive message and, when available, the originating throwable as its cause.</p>
 *
 * <p>A typical trigger is a schema upsert failure encountered while reconciling the destination
 * table with an evolving Protobuf schema. Because it extends {@link RuntimeException}, it is
 * intended to propagate to the sink's centralized error handling rather than be caught at each
 * call site.</p>
 *
 * @see com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache
 */
public class MaxComputeTableOperationException extends RuntimeException {

    /**
     * Constructs a new exception with the supplied detail message and underlying cause.
     *
     * <p>Use this constructor when the failure originates from another exception (most commonly a
     * {@code com.aliyun.odps.OdpsException}) that should be preserved as the cause for diagnostics
     * and to retain the full stack trace.</p>
     *
     * @param message the human-readable detail message describing the failed table operation
     * @param e the underlying exception that caused this failure; retained as the exception cause
     */
    public MaxComputeTableOperationException(String message, Exception e) {
        super(message, e);
    }

    /**
     * Constructs a new exception with the supplied detail message and no underlying cause.
     *
     * <p>Use this constructor when there is no lower-level exception to wrap and only a descriptive
     * message is required to explain the failure.</p>
     *
     * @param message the human-readable detail message describing the failed table operation
     */
    public MaxComputeTableOperationException(String message) {
        super(message);
    }

}
