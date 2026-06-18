package com.gotocompany.depot.bigquery.client;

import com.google.cloud.bigquery.InsertAllRequest;
import com.gotocompany.depot.bigquery.models.Record;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertNull;

/**
 * Unit tests for {@link BigQueryRowWithoutInsertId}, the {@link BigQueryRow} strategy that emits
 * BigQuery rows without an insert id.
 *
 * <p>The test asserts that the produced {@link InsertAllRequest.RowToInsert} has a {@code null} id,
 * which configures BigQuery streaming inserts to skip best-effort de-duplication.</p>
 */
public class BigQueryRowWithoutInsertIdTest {

    /**
     * Verifies that wrapping a record produces a row without an insert id.
     *
     * <p>Given a {@link BigQueryRowWithoutInsertId}, when an empty {@link Record} is wrapped via
     * {@code of}, then the resulting {@link InsertAllRequest.RowToInsert} has a {@code null} id.</p>
     */
    @Test
    public void shouldCreateRowWithoutInsertID() {
        Record record = new Record(new HashMap<>(), new HashMap<>(), 0, null);

        BigQueryRowWithoutInsertId withoutInsertId = new BigQueryRowWithoutInsertId();
        InsertAllRequest.RowToInsert rowToInsert = withoutInsertId.of(record);
        String id = rowToInsert.getId();

        assertNull(id);
    }
}
