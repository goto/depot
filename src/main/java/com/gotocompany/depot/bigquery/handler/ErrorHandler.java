package com.gotocompany.depot.bigquery.handler;

import com.google.cloud.bigquery.BigQueryError;
import com.gotocompany.depot.bigquery.models.Record;

import java.util.List;
import java.util.Map;

/**
 * Hook invoked by the BigQuery sink to react to per-row insertion errors.
 *
 * <p>After a batch insert returns errors, the sink passes the BigQuery errors (keyed by
 * the index of the failed row within the batch) together with the records that were
 * sent, so that an implementation can take corrective action. The most notable
 * implementation, {@link JsonErrorHandler}, repairs the destination table schema when it
 * is inferred from incoming JSON data; {@link NoopErrorHandler} does nothing.</p>
 *
 * @see ErrorHandlerFactory
 */
public interface ErrorHandler {
    /**
     * Handles the errors produced by a batch insert into BigQuery.
     *
     * <p>The default implementation is a no-op, so implementations only need to override
     * this method when they have to react to insertion failures.</p>
     *
     * @param errorInfoMap a map from the index of a failed row within the inserted batch
     *                     to the list of {@link BigQueryError} instances reported for that
     *                     row
     * @param records      the records that were submitted in the batch, used to correlate
     *                     each error back to the originating {@link Record}
     */
    default void handle(Map<Long, List<BigQueryError>> errorInfoMap, List<Record> records) {
    }
}
