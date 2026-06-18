package com.gotocompany.depot.bigquery.storage.json;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryWriter;
import com.gotocompany.depot.config.BigQuerySinkConfig;

import java.util.concurrent.ExecutionException;

/**
 * Placeholder {@link BigQueryWriter} for the JSON streaming path of the BigQuery Storage Write API.
 *
 * <p>This implementation is currently a no-op stub: it is selected by
 * {@link com.gotocompany.depot.bigquery.storage.BigQueryWriterFactory} when the configured schema data
 * type is {@code JSON}, but none of its methods perform any work yet. It exists so the factory can
 * return a writer for the JSON case while full JSON support is pending; using it to stream rows will
 * not append any data.</p>
 *
 * @see com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoWriter
 */
public class BigQueryJsonWriter implements BigQueryWriter {

    /**
     * Creates a JSON writer for the supplied configuration.
     *
     * <p>The configuration is currently unused because the JSON path is not yet implemented.</p>
     *
     * @param config the BigQuery sink configuration
     */
    public BigQueryJsonWriter(BigQuerySinkConfig config) {

    }


    /**
     * No-op initialisation for the JSON writer.
     *
     * <p>The JSON streaming path is not implemented, so no connection or schema is set up.</p>
     */
    @Override
    public void init() {

    }

    /**
     * No-op append for the JSON writer.
     *
     * <p>The JSON streaming path is not implemented, so no rows are appended.</p>
     *
     * @param payload the payload that would be appended; ignored
     * @return always {@code null}
     * @throws ExecutionException   never thrown by this stub implementation
     * @throws InterruptedException never thrown by this stub implementation
     */
    @Override
    public AppendRowsResponse appendAndGet(BigQueryPayload payload) throws ExecutionException, InterruptedException {
        return null;
    }

    /**
     * No-op close for the JSON writer.
     *
     * <p>There are no resources to release in this stub implementation.</p>
     *
     * @throws Exception never thrown by this stub implementation
     */
    @Override
    public void close() throws Exception {

    }
}
