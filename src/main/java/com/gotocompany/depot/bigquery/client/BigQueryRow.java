package com.gotocompany.depot.bigquery.client;


import com.google.cloud.bigquery.InsertAllRequest;
import com.gotocompany.depot.bigquery.models.Record;

/**
 * Fetches BQ insertable row from the base record {@link Record}. The implementations can differ if unique rows need to be inserted or not.
 *
 * <p>When insert IDs are used, BigQuery performs best-effort de-duplication on streaming inserts,
 * whereas without an insert ID rows are always inserted.</p>
 *
 * <p>Known implementations:</p>
 * <ul>
 *     <li>{@link BigQueryRowWithInsertId} attaches a generated insert ID for de-duplication.</li>
 *     <li>{@link BigQueryRowWithoutInsertId} inserts rows without an insert ID.</li>
 * </ul>
 *
 * @see com.google.cloud.bigquery.InsertAllRequest.RowToInsert
 */
public interface BigQueryRow {

    /**
     * Builds the BigQuery insertable row representation of the given record.
     *
     * @param record the source record whose columns (and possibly metadata) form the row
     * @return the {@code RowToInsert} to be added to an {@code InsertAllRequest}
     */
    InsertAllRequest.RowToInsert of(Record record);
}

