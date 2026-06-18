package com.gotocompany.depot.maxcompute.client.ddl;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.task.SQLTask;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.exception.MaxComputeTableOperationException;
import com.gotocompany.depot.maxcompute.schema.SchemaDifferenceUtils;
import com.gotocompany.depot.maxcompute.schema.validator.TableValidator;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.utils.RetryUtils;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;

/**
 * DdlManager is responsible for creating and updating MaxCompute tables.
 *
 * <p>Given a desired {@link TableSchema}, {@link #createOrUpdateTable(TableSchema)} creates the table when it
 * is absent and otherwise applies the additive column differences produced by {@link SchemaDifferenceUtils}.
 * Both paths are executed through {@link RetryUtils} with the configured retry count and back-off, retrying
 * on {@link OdpsException}; the create path is additionally guarded by a {@link TableValidator}. Every
 * successful operation is timed and counted through {@link Instrumentation}.</p>
 *
 * <p>Schema evolution is intentionally restricted: a non-partitioned table cannot be converted into a
 * partitioned one and the partition column cannot be changed, as enforced by
 * {@link #checkPartitionPrecondition(TableSchema)}.</p>
 *
 * @see SchemaDifferenceUtils
 * @see TableValidator
 * @see RetryUtils
 */
@Slf4j
public class DdlManager {

    /**
     * ODPS entry point used to inspect, create, and alter the destination table.
     */
    private final Odps odps;
    /**
     * Sink configuration carrying the project, schema, table name, lifecycle, retry, and partition settings.
     */
    private final MaxComputeSinkConfig maxComputeSinkConfig;
    /**
     * Metric instrumentation used to time and count DDL operations.
     */
    private final Instrumentation instrumentation;
    /**
     * Holder of MaxCompute metric identifiers and tag templates.
     */
    private final MaxComputeMetrics maxComputeMetrics;
    /**
     * Validator that enforces table-level constraints (such as name and lifecycle) before a table is created.
     */
    private final TableValidator tableValidator;

    /**
     * Creates a DDL manager bound to the given ODPS client and configuration.
     *
     * <p>A {@link TableValidator} is constructed eagerly from the supplied configuration and reused across
     * table-creation calls.</p>
     *
     * @param odps                 the ODPS entry point used to run DDL against the destination table
     * @param maxComputeSinkConfig the sink configuration providing table coordinates and DDL settings
     * @param instrumentation      metric instrumentation used to time and count DDL operations
     * @param maxComputeMetrics    holder of MaxCompute metric identifiers and tag templates
     */
    public DdlManager(Odps odps, MaxComputeSinkConfig maxComputeSinkConfig, Instrumentation instrumentation, MaxComputeMetrics maxComputeMetrics) {
        this.odps = odps;
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.instrumentation = instrumentation;
        this.maxComputeMetrics = maxComputeMetrics;
        this.tableValidator = new TableValidator(maxComputeSinkConfig);
    }

    /**
     * Creates or updates the MaxCompute table based on the table schema.
     * Creates the table if it does not exist, otherwise updates the table schema.
     * DDL operations are retried based on the configuration.
     *
     * <p>The project, schema (dataset), and table name are resolved from the configuration. When the table does
     * not yet exist it is created through {@link #createTable(TableSchema, String, String, String)}; otherwise
     * the current schema is reconciled with the desired one through
     * {@link #updateTable(TableSchema, String, String, String)}. Both DDL paths are retried according to the
     * configured retry policy.</p>
     *
     * @param tableSchema the table schema to create or update
     * @throws OdpsException if the table creation or update fails
     */
    public void createOrUpdateTable(TableSchema tableSchema) throws OdpsException {
        String projectName = maxComputeSinkConfig.getMaxComputeProjectId();
        String datasetName = maxComputeSinkConfig.getMaxComputeSchema();
        String tableName = maxComputeSinkConfig.getMaxComputeTableName();
        if (!this.odps.tables().exists(tableName)) {
            createTable(tableSchema, projectName, datasetName, tableName);
            return;
        }
        updateTable(tableSchema, projectName, datasetName, tableName);
    }

    /**
     * Creates the destination table from the supplied schema.
     *
     * <p>The table is first checked by the {@link TableValidator}. The creation itself runs inside a
     * {@link RetryUtils} block that applies the configured lifecycle (in days) and table properties and retries
     * on {@link OdpsException}. A successful creation is logged and recorded as a
     * {@link MaxComputeMetrics.MaxComputeAPIType#TABLE_CREATE} metric.</p>
     *
     * @param tableSchema the schema of the table to create
     * @param projectName the MaxCompute project that will own the table
     * @param datasetName the MaxCompute schema (dataset) name applied to the table
     * @param tableName   the name of the table to create
     */
    private void createTable(TableSchema tableSchema, String projectName, String datasetName, String tableName) {
        log.info("Creating table: {} schema:{}", tableName, tableSchema);
        tableValidator.validate(tableName, maxComputeSinkConfig.getMaxComputeTableLifecycleDays(), tableSchema);
        RetryUtils.executeWithRetry(() -> {
                    Instant start = Instant.now();
                    this.odps.tables().newTableCreator(projectName, tableName, tableSchema)
                            .withSchemaName(datasetName)
                            .withLifeCycle(maxComputeSinkConfig.getMaxComputeTableLifecycleDays())
                            .withTblProperties(maxComputeSinkConfig.getTableProperties())
                            .create();
                    instrumentation.logInfo("Successfully created maxCompute table " + tableName);
                    instrument(start, MaxComputeMetrics.MaxComputeAPIType.TABLE_CREATE);
                }, maxComputeSinkConfig.getMaxDdlRetryCount(), maxComputeSinkConfig.getDdlRetryBackoffMillis(),
                e -> e instanceof OdpsException);
    }

    /**
     * Reconciles the existing table with the desired schema by applying additive column changes.
     *
     * <p>The current schema is read from the backend and validated against the partition precondition. The
     * ordered set of {@code ALTER TABLE} statements that bring the table up to date is computed by
     * {@link SchemaDifferenceUtils} and applied one by one inside a {@link RetryUtils} block (retried on
     * {@link OdpsException}). If a statement does not complete successfully a
     * {@link MaxComputeTableOperationException} is raised carrying the backend error message; because that
     * exception is not an {@link OdpsException}, the surrounding retry layer rethrows it wrapped in a
     * {@code NonRetryableException}. A successful update is logged and recorded as a
     * {@link MaxComputeMetrics.MaxComputeAPIType#TABLE_UPDATE} metric.</p>
     *
     * @param tableSchema the desired table schema to converge to
     * @param projectName the MaxCompute project that owns the table
     * @param datasetName the MaxCompute schema (dataset) name of the table
     * @param tableName   the name of the table to update
     * @throws MaxComputeTableOperationException if the partition precondition is violated
     */
    private void updateTable(TableSchema tableSchema, String projectName, String datasetName, String tableName) {
        log.info("Updating table: {} schema:{}", tableName, tableSchema);
        TableSchema oldSchema = this.odps.tables().get(projectName, datasetName, tableName)
                .getSchema();
        checkPartitionPrecondition(oldSchema);
        Deque<String> schemaDifferenceSql = new LinkedList<>(SchemaDifferenceUtils.getSchemaDifferenceSql(oldSchema, tableSchema, datasetName, tableName));
        RetryUtils.executeWithRetry(() -> {
                    Instant start = Instant.now();
                    while (!schemaDifferenceSql.isEmpty()) {
                        String sql = schemaDifferenceSql.peekFirst();
                        Instance instance = execute(sql);
                        if (!instance.isSuccessful()) {
                            instrumentation.logError("Failed to execute SQL: " + sql);
                            String errorMessage = instance.getRawTaskResults().get(0).getResult().getString();
                            throw new MaxComputeTableOperationException(String.format("Failed to update table schema with reason: %s", errorMessage));
                        }
                        schemaDifferenceSql.pollFirst();
                    }
                    instrumentation.logInfo("Successfully updated maxCompute table " + tableName);
                    instrument(start, MaxComputeMetrics.MaxComputeAPIType.TABLE_UPDATE);
                }, maxComputeSinkConfig.getMaxDdlRetryCount(), maxComputeSinkConfig.getDdlRetryBackoffMillis(),
                e -> e instanceof OdpsException);
    }

    /**
     * Validates that the requested partitioning is compatible with the existing table.
     *
     * <p>When partitioning is enabled the method rejects two unsupported transitions: converting an existing
     * non-partitioned table into a partitioned one, and changing the partition column away from the value
     * configured for the sink.</p>
     *
     * @param oldSchema the current schema of the table as read from the backend
     * @throws MaxComputeTableOperationException if partitioning is enabled and the existing table has no
     *                                           partition columns, or if the configured partition column differs
     *                                           from the table's current partition column
     */
    private void checkPartitionPrecondition(TableSchema oldSchema) {
        if (maxComputeSinkConfig.isTablePartitioningEnabled() && oldSchema.getPartitionColumns().isEmpty()) {
            throw new MaxComputeTableOperationException("Updating non-partitioned table to partitioned table is not supported");
        }
        if (maxComputeSinkConfig.isTablePartitioningEnabled()) {
            String currentPartitionColumnKey = oldSchema.getPartitionColumns()
                    .stream()
                    .findFirst()
                    .map(Column::getName)
                    .orElse(null);
            if (!Objects.equals(maxComputeSinkConfig.getTablePartitionColumnName(), currentPartitionColumnKey)) {
                throw new MaxComputeTableOperationException("Changing partition column is not supported");
            }
        }
    }

    /**
     * Runs a single SQL statement against MaxCompute and blocks until it completes.
     *
     * @param sql the SQL statement to execute
     * @return the completed {@link Instance} representing the executed statement
     * @throws OdpsException if submitting the statement or waiting for it to finish fails
     */
    private Instance execute(String sql) throws OdpsException {
        log.info("Executing SQL: {}", sql);
        Instance instance = SQLTask.run(odps, sql);
        instance.waitForSuccess();
        return instance;
    }

    /**
     * Emits the operation-count and latency metrics for a completed DDL operation.
     *
     * <p>Both metrics are tagged with the table, project, schema, and API type so that they can be sliced per
     * destination table and per operation type.</p>
     *
     * @param startTime the instant at which the operation started, used to compute its latency
     * @param type      the kind of DDL operation that completed (for example table create or table update)
     */
    private void instrument(Instant startTime, MaxComputeMetrics.MaxComputeAPIType type) {
        instrumentation.incrementCounter(
                maxComputeMetrics.getMaxComputeOperationTotalMetric(),
                String.format(MaxComputeMetrics.MAXCOMPUTE_TABLE_TAG, maxComputeSinkConfig.getMaxComputeTableName()),
                String.format(MaxComputeMetrics.MAXCOMPUTE_PROJECT_TAG, maxComputeSinkConfig.getMaxComputeProjectId()),
                String.format(MaxComputeMetrics.MAXCOMPUTE_SCHEMA_TAG, maxComputeSinkConfig.getMaxComputeSchema()),
                String.format(MaxComputeMetrics.MAXCOMPUTE_API_TAG, type)
        );
        instrumentation.captureDurationSince(
                maxComputeMetrics.getMaxComputeOperationLatencyMetric(),
                startTime,
                String.format(MaxComputeMetrics.MAXCOMPUTE_TABLE_TAG, maxComputeSinkConfig.getMaxComputeTableName()),
                String.format(MaxComputeMetrics.MAXCOMPUTE_PROJECT_TAG, maxComputeSinkConfig.getMaxComputeProjectId()),
                String.format(MaxComputeMetrics.MAXCOMPUTE_SCHEMA_TAG, maxComputeSinkConfig.getMaxComputeSchema()),
                String.format(MaxComputeMetrics.MAXCOMPUTE_API_TAG, type)
        );
    }

}
