package com.gotocompany.depot.bigquery.storage;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoWriter;
import com.gotocompany.depot.bigquery.storage.json.BigQueryJsonWriter;
import com.gotocompany.depot.common.Function3;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;

import java.util.function.Function;


/**
 * Factory for creating {@link BigQueryWriter} instances suited to the configured schema data type.
 *
 * <p>The writer wraps the BigQuery Storage Write API connection. Rather than constructing the
 * underlying clients directly, this factory accepts creator functions for the heavy collaborators
 * (the write client, credentials provider and stream). This indirection makes the resulting writer
 * easy to test by injecting fakes, and lets the writer recreate these collaborators on demand (for
 * example when refreshing a stale connection).</p>
 */
public class BigQueryWriterFactory {

    /**
     * Creates a {@link BigQueryWriter} for the supplied configuration and collaborator factories.
     *
     * @param config          the BigQuery sink configuration, whose schema data type selects the
     *                        writer implementation
     * @param bqWriterCreator factory that creates a {@code BigQueryWriteClient} from the configuration
     * @param credCreator     factory that creates a {@code CredentialsProvider} from the configuration
     * @param streamCreator   factory that creates a {@link BigQueryStream} given the configuration,
     *                        credentials provider and Protobuf schema
     * @param instrumentation the instrumentation used to log and emit metrics
     * @param metrics         the BigQuery metric name provider used when recording operations
     * @return a writer matching the configured schema data type: a
     *         {@link com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoWriter} for
     *         {@code PROTOBUF} or a
     *         {@link com.gotocompany.depot.bigquery.storage.json.BigQueryJsonWriter} for {@code JSON}
     * @throws IllegalArgumentException if the configured schema data type is not supported
     */
    public static BigQueryWriter createBigQueryWriter(
            BigQuerySinkConfig config,
            Function<BigQuerySinkConfig, BigQueryWriteClient> bqWriterCreator,
            Function<BigQuerySinkConfig, CredentialsProvider> credCreator,
            Function3<BigQuerySinkConfig, CredentialsProvider, ProtoSchema, BigQueryStream> streamCreator,
            Instrumentation instrumentation,
            BigQueryMetrics metrics) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case PROTOBUF:
                return new BigQueryProtoWriter(config, bqWriterCreator, credCreator, streamCreator, instrumentation, metrics);
            case JSON:
                return new BigQueryJsonWriter(config);
            default:
                throw new IllegalArgumentException("Couldn't initialise the BQ writer");
        }
    }
}
