package com.gotocompany.depot.bigquery.storage;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoStream;
import com.gotocompany.depot.config.BigQuerySinkConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Utility helpers for constructing the BigQuery Storage Write API collaborators used by the Protobuf
 * writer.
 *
 * <p>The static factory methods here are typically passed as method references to
 * {@link BigQueryWriterFactory} (and ultimately invoked by
 * {@link com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoWriter}) to create the write
 * client, credentials provider and stream writer from a {@link BigQuerySinkConfig}. Credentials are
 * always loaded from the service-account JSON file referenced by the configuration.</p>
 *
 * <p>All streams created here target the table's <em>default</em> stream, which provides
 * at-least-once delivery semantics through the Storage Write API.</p>
 */
public class BigQueryWriterUtils {
    /** Suffix appended to a table resource name to address its default Storage Write API stream. */
    private static final String DEFAULT_STREAM_SUFFIX = "/_default";

    /**
     * Creates a {@code BigQueryWriteClient} configured with credentials derived from the supplied
     * configuration.
     *
     * @param config the BigQuery sink configuration providing the credential path
     * @return a newly created Storage Write API client
     * @throws IllegalArgumentException if the client cannot be initialised, for example when the
     *                                  credentials cannot be read (wrapping the underlying
     *                                  {@link IOException})
     */
    public static BigQueryWriteClient getBigQueryWriterClient(BigQuerySinkConfig config) {
        try {

            BigQueryWriteSettings settings = BigQueryWriteSettings.newBuilder()
                    .setCredentialsProvider(getCredentialsProvider(config))
                    .build();
            return BigQueryWriteClient.create(settings);
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't initialise writer client", e);
        }
    }

    /**
     * Builds a fixed {@code CredentialsProvider} backed by the Google service-account credentials
     * read from the configured credential file path.
     *
     * @param config the BigQuery sink configuration providing the credential file path
     * @return a credentials provider wrapping the loaded Google credentials
     * @throws IllegalArgumentException if the credential file cannot be opened or parsed (wrapping the
     *                                  underlying {@link IOException})
     */
    public static CredentialsProvider getCredentialsProvider(BigQuerySinkConfig config) {
        try {
            return FixedCredentialsProvider.create(
                    GoogleCredentials.fromStream(Files.newInputStream(Paths.get(config.getBigQueryCredentialPath()))));
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't initialise credential provider", e);
        }
    }

    /**
     * Creates a Storage Write API stream writer targeting the table's default stream and wraps it in
     * a {@link BigQueryStream}.
     *
     * <p>The underlying {@code StreamWriter} is configured with the supplied credentials and Protobuf
     * writer schema, and has connection pooling disabled so that each writer owns a dedicated
     * connection.</p>
     *
     * @param config              the BigQuery sink configuration used to resolve the destination
     *                            stream name
     * @param credentialsProvider the credentials provider used to authenticate the stream
     * @param schema              the Protobuf schema describing the rows that will be appended
     * @return a {@link com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoStream} wrapping the
     *         created stream writer
     * @throws IllegalArgumentException if the stream writer cannot be built (wrapping the underlying
     *                                  {@link IOException})
     */
    public static BigQueryStream getStreamWriter(BigQuerySinkConfig config, CredentialsProvider credentialsProvider, ProtoSchema schema) {
        try {
            String streamName = getDefaultStreamName(config);
            StreamWriter.Builder builder = StreamWriter.newBuilder(streamName);
            builder.setCredentialsProvider(credentialsProvider);
            builder.setWriterSchema(schema);
            builder.setEnableConnectionPool(false);
            return new BigQueryProtoStream(builder.build());
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't initialise Stream Writer", e);
        }
    }

    /**
     * Computes the fully-qualified default stream name for the configured project, dataset and table.
     *
     * <p>The returned value has the form
     * {@code projects/PROJECT/datasets/DATASET/tables/TABLE/_default}, where the upper-cased segments
     * are replaced with the configured project, dataset and table names.</p>
     *
     * @param config the BigQuery sink configuration supplying the project, dataset and table names
     * @return the resource name of the table's default Storage Write API stream
     */
    public static String getDefaultStreamName(BigQuerySinkConfig config) {
        TableName parentTable = TableName.of(config.getGCloudProjectID(), config.getDatasetName(), config.getTableName());
        return parentTable.toString() + DEFAULT_STREAM_SUFFIX;
    }
}
