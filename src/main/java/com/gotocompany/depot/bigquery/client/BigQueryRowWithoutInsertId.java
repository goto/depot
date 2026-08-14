package com.gotocompany.depot.bigquery.client;

import com.google.cloud.bigquery.InsertAllRequest;
import com.gotocompany.depot.bigquery.models.Record;

/**
 * {@link BigQueryRow} implementation that inserts rows without an insert ID.
 *
 * <p>Because no insert ID is supplied, BigQuery does not attempt de-duplication for these streaming
 * inserts; every call results in a row being inserted. This is appropriate when de-duplication is not
 * required or is handled elsewhere.</p>
 *
 * @see BigQueryRowWithInsertId
 */
public class BigQueryRowWithoutInsertId implements BigQueryRow {

    /**
     * Builds a {@code RowToInsert} from the record's columns, without an insert ID.
     *
     * @param record the source record providing the column values
     * @return a {@code RowToInsert} containing only the record's columns
     */
    @Override
    public InsertAllRequest.RowToInsert of(Record record) {
        return InsertAllRequest.RowToInsert.of(record.getColumns());
    }
}
