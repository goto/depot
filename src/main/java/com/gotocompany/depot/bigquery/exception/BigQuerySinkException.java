package com.gotocompany.depot.bigquery.exception;

import lombok.EqualsAndHashCode;

/**
 * Generic unchecked exception that represents a failure of Depot's BigQuery sink.
 *
 * <p>It is used as a lightweight, message-less stand-in that is attached to an
 * {@link com.gotocompany.depot.error.ErrorInfo} when the BigQuery response parser
 * records that a row failed at the sink (for example unknown, invalid-schema,
 * out-of-bounds or stopped row errors). Extending {@link RuntimeException} lets it
 * propagate without being declared on the sink API.</p>
 *
 * <p>Lombok's {@link EqualsAndHashCode} is applied with {@code callSuper = false} so
 * that equality ignores the {@link RuntimeException} super-state (such as the captured
 * stack trace); two instances are therefore considered equal, which is convenient when
 * comparing expected and actual errors.</p>
 */
@EqualsAndHashCode(callSuper = false)
public class BigQuerySinkException extends RuntimeException {
}
