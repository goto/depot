package com.gotocompany.depot.bigquery.storage.json;

import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.gotocompany.depot.bigquery.storage.BigQueryStream;

/**
 * Placeholder {@link BigQueryStream} for the JSON streaming path of the BigQuery Storage Write API.
 *
 * <p>It is the JSON counterpart to
 * {@link com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoStream} and is intended to wrap a
 * {@code JsonStreamWriter}. The JSON path is not yet implemented, so this stub currently holds no
 * writer.</p>
 */
public class BigQueryJsonStream implements BigQueryStream {

    /**
     * Returns the wrapped JSON stream writer.
     *
     * <p>The JSON streaming path is not yet implemented, so this stub always returns {@code null}.</p>
     *
     * @return always {@code null} in this stub implementation
     */
    public JsonStreamWriter getStreamWriter() {
        return null;
    }
}
