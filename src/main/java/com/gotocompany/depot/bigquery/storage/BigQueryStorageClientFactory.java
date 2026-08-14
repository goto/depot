package com.gotocompany.depot.bigquery.storage;

import com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoStorageClient;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.MessageParser;

/**
 * Factory for creating {@link BigQueryStorageClient} instances appropriate to the configured schema
 * data type.
 *
 * <p>This factory decouples callers from the concrete Storage Write API client implementation. The
 * selection is driven by {@code BigQuerySinkConfig#getSinkConnectorSchemaDataType()}; currently only
 * the Protobuf path is supported, which yields a
 * {@link com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoStorageClient}.</p>
 */
public class BigQueryStorageClientFactory {

    /**
     * Creates a {@link BigQueryStorageClient} for the supplied configuration, parser and writer.
     *
     * @param config         the BigQuery sink configuration, whose schema data type determines which
     *                       client implementation is created
     * @param parser         the message parser used by the client to deserialize incoming messages
     * @param bigQueryWriter the writer the client delegates to for appending rows to the stream
     * @return a storage client matching the configured schema data type
     * @throws IllegalArgumentException if the configured schema data type is not supported (any value
     *                                  other than {@code PROTOBUF})
     */
    public static BigQueryStorageClient createBigQueryStorageClient(
            BigQuerySinkConfig config,
            MessageParser parser,
            BigQueryWriter bigQueryWriter) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case PROTOBUF:
                return new BigQueryProtoStorageClient(bigQueryWriter, config, parser);
            default:
                throw new IllegalArgumentException("Invalid data type");
        }
    }
}
