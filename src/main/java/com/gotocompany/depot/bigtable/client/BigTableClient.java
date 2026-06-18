package com.gotocompany.depot.bigtable.client;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import com.gotocompany.depot.bigtable.exception.BigTableInvalidSchemaException;
import com.gotocompany.depot.bigtable.model.BigTableRecord;
import com.gotocompany.depot.bigtable.model.BigTableSchema;
import com.gotocompany.depot.bigtable.response.BigTableResponse;
import com.gotocompany.depot.config.BigTableSinkConfig;
import com.gotocompany.depot.metrics.BigTableMetrics;
import com.gotocompany.depot.metrics.Instrumentation;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Thin wrapper around the Google Cloud Bigtable data and admin clients used by the Bigtable sink.
 *
 * <p>{@code BigTableClient} encapsulates two responsibilities: applying record mutations to a Bigtable
 * table and validating that the configured table and column families exist. Writes are performed as a
 * single {@link com.google.cloud.bigtable.data.v2.models.BulkMutation} built from the supplied
 * {@link BigTableRecord}s; on partial failure the resulting
 * {@link com.google.cloud.bigtable.data.v2.models.MutateRowsException} is captured and returned as a
 * {@link BigTableResponse} rather than propagated. Operation latency and throughput are recorded
 * through {@link BigTableMetrics} and {@link Instrumentation}.</p>
 *
 * @see com.gotocompany.depot.bigtable.BigTableSink
 * @see BigTableSchema
 * @see BigTableResponse
 */
public class BigTableClient {
    /** Admin client used to check that the table and its column families exist. */
    private final BigtableTableAdminClient bigtableTableAdminClient;
    /** Data client used to apply bulk row mutations. */
    private final BigtableDataClient bigtableDataClient;
    /** Bigtable sink configuration supplying project, instance and table identifiers. */
    private final BigTableSinkConfig sinkConfig;
    /** Expected column-family schema used during validation. */
    private final BigTableSchema bigtableSchema;
    /** Metric names and tags for Bigtable operations. */
    private final BigTableMetrics bigtableMetrics;
    /** Logging and metrics facade for this client. */
    private final Instrumentation instrumentation;

    /**
     * Creates a client that connects to Bigtable using data and admin clients derived from the
     * configuration.
     *
     * <p>Delegates to
     * {@link #BigTableClient(BigTableSinkConfig, BigtableDataClient, BigtableTableAdminClient, BigTableSchema, BigTableMetrics, Instrumentation)}
     * after building a {@link com.google.cloud.bigtable.data.v2.BigtableDataClient} and a
     * {@link com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient} from {@code sinkConfig}.</p>
     *
     * @param sinkConfig the Bigtable configuration (project, instance, credentials, table)
     * @param bigtableSchema the expected column-family schema used for validation
     * @param bigtableMetrics the Bigtable metric names and tags
     * @param instrumentation the logging and metrics facade for this client
     * @throws IOException if the Bigtable data or admin client cannot be created, for example because
     *     the credentials file cannot be read
     */
    public BigTableClient(BigTableSinkConfig sinkConfig, BigTableSchema bigtableSchema, BigTableMetrics bigtableMetrics, Instrumentation instrumentation) throws IOException {
        this(sinkConfig, getBigTableDataClient(sinkConfig), getBigTableAdminClient(sinkConfig), bigtableSchema, bigtableMetrics, instrumentation);
    }

    /**
     * Creates a client from explicitly provided Bigtable data and admin clients.
     *
     * <p>This constructor performs no I/O and is primarily intended for supplying preconfigured or
     * test-double clients.</p>
     *
     * @param sinkConfig the Bigtable configuration (project, instance, table)
     * @param bigtableDataClient the data client used to apply mutations
     * @param bigtableTableAdminClient the admin client used to validate table schema
     * @param bigtableSchema the expected column-family schema used for validation
     * @param bigtableMetrics the Bigtable metric names and tags
     * @param instrumentation the logging and metrics facade for this client
     */
    public BigTableClient(BigTableSinkConfig sinkConfig, BigtableDataClient bigtableDataClient, BigtableTableAdminClient bigtableTableAdminClient, BigTableSchema bigtableSchema, BigTableMetrics bigtableMetrics, Instrumentation instrumentation) {
        this.sinkConfig = sinkConfig;
        this.bigtableDataClient = bigtableDataClient;
        this.bigtableTableAdminClient = bigtableTableAdminClient;
        this.bigtableSchema = bigtableSchema;
        this.bigtableMetrics = bigtableMetrics;
        this.instrumentation = instrumentation;
    }

    /**
     * Builds a Bigtable data client from the sink configuration.
     *
     * <p>Configures the project and instance identifiers and loads service-account credentials from the
     * configured credentials file path.</p>
     *
     * @param sinkConfig the configuration supplying the project, instance and credentials path
     * @return a new {@link com.google.cloud.bigtable.data.v2.BigtableDataClient}
     * @throws IOException if the credentials file cannot be read or the client cannot be created
     */
    private static BigtableDataClient getBigTableDataClient(BigTableSinkConfig sinkConfig) throws IOException {
        BigtableDataSettings settings = BigtableDataSettings.newBuilder()
                .setProjectId(sinkConfig.getGCloudProjectID())
                .setInstanceId(sinkConfig.getInstanceId())
                .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(new FileInputStream(sinkConfig.getCredentialPath()))))
                .build();
        return BigtableDataClient.create(settings);
    }

    /**
     * Builds a Bigtable table admin client from the sink configuration.
     *
     * <p>Configures the project and instance identifiers and loads service-account credentials from the
     * configured credentials file path.</p>
     *
     * @param sinkConfig the configuration supplying the project, instance and credentials path
     * @return a new {@link com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient}
     * @throws IOException if the credentials file cannot be read or the client cannot be created
     */
    private static BigtableTableAdminClient getBigTableAdminClient(BigTableSinkConfig sinkConfig) throws IOException {
        BigtableTableAdminSettings settings = BigtableTableAdminSettings.newBuilder()
                .setProjectId(sinkConfig.getGCloudProjectID())
                .setInstanceId(sinkConfig.getInstanceId())
                .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(new FileInputStream(sinkConfig.getCredentialPath()))))
                .build();
        return BigtableTableAdminClient.create(settings);
    }

    /**
     * Applies a batch of record mutations to Bigtable as a single bulk mutation.
     *
     * <p>Each {@link BigTableRecord}'s
     * {@link com.google.cloud.bigtable.data.v2.models.RowMutationEntry} is added to a
     * {@link com.google.cloud.bigtable.data.v2.models.BulkMutation} for the configured table and
     * applied in one call. On full success the operation latency and the number of applied entries are
     * recorded and {@code null} is returned. If some entries fail, the thrown
     * {@link com.google.cloud.bigtable.data.v2.models.MutateRowsException} is caught, logged, and
     * wrapped in a {@link BigTableResponse} that is returned to the caller; the exception is not
     * propagated.</p>
     *
     * @param records the records whose mutations should be written
     * @return {@code null} when every mutation is applied successfully, or a {@link BigTableResponse}
     *     describing the failed mutations otherwise
     */
    public BigTableResponse send(List<BigTableRecord> records) {
        BigTableResponse bigTableResponse = null;
        BulkMutation batch = BulkMutation.create(sinkConfig.getTableId());
        records.forEach(record -> batch.add(record.getRowMutationEntry()));
        try {
            Instant startTime = Instant.now();
            bigtableDataClient.bulkMutateRows(batch);
            instrument(startTime, batch.getEntryCount());
        } catch (MutateRowsException e) {
            bigTableResponse = new BigTableResponse(e);
            instrumentation.logError("Some entries failed to be applied. {}", e.getCause());
        }
        return bigTableResponse;
    }

    /**
     * Records latency and throughput metrics for a completed Bigtable bulk write.
     *
     * <p>Emits the operation latency measured since {@code startTime} and a count of {@code entryCount}
     * applied entries, both tagged with the configured Bigtable instance and table.</p>
     *
     * @param startTime the instant the bulk write began, used to compute latency
     * @param entryCount the number of mutation entries that were applied
     */
    private void instrument(Instant startTime, long entryCount) {
        instrumentation.captureDurationSince(
                bigtableMetrics.getBigtableOperationLatencyMetric(),
                startTime,
                String.format(BigTableMetrics.BIGTABLE_INSTANCE_TAG, sinkConfig.getInstanceId()),
                String.format(BigTableMetrics.BIGTABLE_TABLE_TAG, sinkConfig.getTableId()));
        instrumentation.captureCount(
                bigtableMetrics.getBigtableOperationTotalMetric(),
                entryCount,
                String.format(BigTableMetrics.BIGTABLE_INSTANCE_TAG, sinkConfig.getInstanceId()),
                String.format(BigTableMetrics.BIGTABLE_TABLE_TAG, sinkConfig.getTableId()));
    }

    /**
     * Validates that the configured table and all required column families exist in Bigtable.
     *
     * <p>Checks that the configured table is present and then that every column family declared by the
     * {@link BigTableSchema} exists on it. Used by the factory to fail fast on a misconfigured
     * destination.</p>
     *
     * @throws BigTableInvalidSchemaException if the configured table does not exist or one or more
     *     required column families are missing
     */
    public void validateBigTableSchema() throws BigTableInvalidSchemaException {
        String tableId = sinkConfig.getTableId();
        instrumentation.logDebug(String.format("Validating schema for table: %s...", tableId));
        checkIfTableExists(tableId);
        checkIfColumnFamiliesExist(tableId);
        instrumentation.logDebug("Validation complete, Schema is valid.");
    }

    /**
     * Verifies that the named table exists in the configured Bigtable instance.
     *
     * @param tableId the identifier of the table to check
     * @throws BigTableInvalidSchemaException if no table with {@code tableId} exists
     */
    private void checkIfTableExists(String tableId) throws BigTableInvalidSchemaException {
        if (!bigtableTableAdminClient.exists(tableId)) {
                throw new BigTableInvalidSchemaException(String.format("Table not found on the path: projects/%s/instances/%s/tables/%s",
                        bigtableTableAdminClient.getProjectId(), bigtableTableAdminClient.getInstanceId(), tableId));
            }
    }

    /**
     * Verifies that every column family required by the schema exists on the named table.
     *
     * <p>Reads the table's existing column families and compares them against those declared by the
     * {@link BigTableSchema} via {@link BigTableSchema#getMissingColumnFamilies(java.util.Set)}.</p>
     *
     * @param tableId the identifier of the table to check
     * @throws BigTableInvalidSchemaException if one or more required column families are absent from the
     *     table
     */
    private void checkIfColumnFamiliesExist(String tableId) throws BigTableInvalidSchemaException {
        Set<String> existingColumnFamilies = bigtableTableAdminClient.getTable(tableId)
                .getColumnFamilies()
                .stream()
                .map(ColumnFamily::getId)
                .collect(Collectors.toSet());
        Set<String> missingColumnFamilies = bigtableSchema.getMissingColumnFamilies(existingColumnFamilies);
        if (missingColumnFamilies.size() > 0) {
            throw new BigTableInvalidSchemaException(
                    String.format("Column families %s do not exist in table %s!", missingColumnFamilies, tableId));
        }
    }
}
