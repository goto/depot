package com.gotocompany.depot.bigquery.client;

import com.google.cloud.bigquery.InsertAllRequest;
import com.gotocompany.depot.bigquery.models.Record;

import java.util.Map;
import java.util.function.Function;

/**
 * {@link BigQueryRow} implementation that attaches a generated insert ID to each row.
 *
 * <p>Providing an insert ID enables BigQuery's best-effort de-duplication for streaming inserts, so
 * retried or duplicated records carrying the same ID are less likely to be stored twice. The ID is
 * computed from the record's metadata by a caller-supplied function, allowing the de-duplication key
 * to be derived from message metadata (for example a partition/offset combination).</p>
 *
 * @see BigQueryRowWithoutInsertId
 */
public class BigQueryRowWithInsertId implements BigQueryRow {
    /** Function that derives the BigQuery insert ID from a record's metadata map. */
    private final Function<Map<String, Object>, String> rowIDCreator;

    /**
     * Creates a row builder that derives each row's insert ID via the supplied function.
     *
     * @param rowIDCreator function mapping a record's metadata to the insert ID string used for
     *                     de-duplication
     */
    public BigQueryRowWithInsertId(Function<Map<String, Object>, String> rowIDCreator) {
        this.rowIDCreator = rowIDCreator;
    }

    /**
     * Builds a {@code RowToInsert} from the record's columns, tagged with a generated insert ID.
     *
     * <p>The insert ID is produced by applying the configured function to the record's metadata.</p>
     *
     * @param record the source record providing the metadata (for the insert ID) and the column values
     * @return a {@code RowToInsert} carrying the generated insert ID and the record's columns
     */
    @Override
    public InsertAllRequest.RowToInsert of(Record record) {
        return InsertAllRequest.RowToInsert.of(rowIDCreator.apply(record.getMetadata()), record.getColumns());
    }
}
