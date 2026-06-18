package com.gotocompany.depot.bigquery.handler;

import lombok.extern.slf4j.Slf4j;

/**
 * {@link ErrorHandler} implementation that intentionally does nothing.
 *
 * <p>Used by the BigQuery sink when no error handling is required (for example for
 * protobuf-based schemas, where the table schema is managed up front). It inherits the
 * no-op {@link ErrorHandler#handle(java.util.Map, java.util.List)} default, so insertion
 * errors are simply ignored by this handler.</p>
 *
 * @see ErrorHandlerFactory
 */
@Slf4j
public class NoopErrorHandler implements ErrorHandler {
}
