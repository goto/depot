package com.gotocompany.depot.bigquery.client;

import com.google.cloud.bigquery.InsertAllRequest;
import com.gotocompany.depot.bigquery.models.Record;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;


/**
 * Unit tests for {@link BigQueryRowWithInsertId}, the {@link BigQueryRow} strategy that attaches a
 * deterministic insert id to each BigQuery row.
 *
 * <p>The test supplies a stub id-creator function and asserts that the resulting
 * {@link InsertAllRequest.RowToInsert} exposes the computed insert id, which BigQuery uses for
 * best-effort de-duplication of streaming inserts.</p>
 */
public class BigQueryRowWithInsertIdTest {

    /**
     * Verifies that the configured id-creator function is applied when wrapping a record.
     *
     * <p>Given a {@link BigQueryRowWithInsertId} built with an id function that always returns
     * {@code "default_1_1"}, when an empty {@link Record} is wrapped via {@code of}, then the produced
     * {@link InsertAllRequest.RowToInsert} carries {@code "default_1_1"} as its insert id.</p>
     */
    @Test
    public void shouldCreateRowWithInsertID() {
        Record record = new Record(new HashMap<>(), new HashMap<>(), 0, null);

        BigQueryRowWithInsertId withInsertId = new BigQueryRowWithInsertId(metadata -> "default_1_1");
        InsertAllRequest.RowToInsert rowToInsert = withInsertId.of(record);
        String id = rowToInsert.getId();

        assertEquals("default_1_1", id);
    }
}
