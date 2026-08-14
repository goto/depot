package com.gotocompany.depot.maxcompute.client;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.ddl.DdlManager;
import com.gotocompany.depot.maxcompute.client.insert.InsertManager;
import com.gotocompany.depot.maxcompute.client.insert.InsertManagerFactory;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * MaxComputeClient is a client to interact with MaxCompute.
 * It provides methods to execute table creation and update and inserting records into MaxCompute.
 *
 * <p>It owns the configured {@link Odps} entry point and {@link TableTunnel}, and exposes the small set of
 * operations the sink requires:</p>
 * <ul>
 *     <li>reading the live table schema through {@link #getLatestTableSchema()};</li>
 *     <li>creating or evolving the destination table through {@link #createOrUpdateTable(TableSchema)},
 *     which is delegated to a {@link DdlManager};</li>
 *     <li>creating the {@link InsertManager} that streams records into the table through
 *     {@link #createInsertManager()}.</li>
 * </ul>
 *
 * <p>The Lombok-generated all-args and no-args constructors exist mainly to support construction in tests;
 * production code uses
 * {@link #MaxComputeClient(MaxComputeSinkConfig, StatsDReporter, MaxComputeMetrics)}, which derives the
 * ODPS client, tunnel, and DDL manager from configuration.</p>
 */
@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class MaxComputeClient {

    /**
     * Configured ODPS entry point used to access tables and run SQL.
     */
    private Odps odps;
    /**
     * MaxCompute sink configuration providing credentials, endpoints, and the target table coordinates.
     */
    private MaxComputeSinkConfig maxComputeSinkConfig;
    /**
     * Tunnel handle used to build the streaming upload sessions that carry records into MaxCompute.
     */
    private TableTunnel tableTunnel;
    /**
     * Manager that performs table creation and schema-evolution DDL.
     */
    private DdlManager ddlManager;
    /**
     * Holder of MaxCompute metric identifiers and tag templates.
     */
    private MaxComputeMetrics maxComputeMetrics;
    /**
     * Metric instrumentation used to time and count MaxCompute operations.
     */
    private Instrumentation instrumentation;

    /**
     * Builds a production-ready client from configuration.
     *
     * <p>Initializes the {@link Odps} entry point and its {@link TableTunnel}, overriding the tunnel endpoint
     * when a tunnel URL is configured, and creates the metric {@link Instrumentation} and the
     * {@link DdlManager} used for table operations.</p>
     *
     * @param maxComputeSinkConfig the MaxCompute sink configuration providing credentials, endpoints, and table coordinates
     * @param statsDReporter       reporter used to build the metric {@link Instrumentation}
     * @param maxComputeMetrics    holder of MaxCompute metric identifiers and tag templates
     */
    public MaxComputeClient(MaxComputeSinkConfig maxComputeSinkConfig,
                            StatsDReporter statsDReporter,
                            MaxComputeMetrics maxComputeMetrics) {
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.instrumentation = new Instrumentation(statsDReporter, this.getClass());
        this.odps = initializeOdps();
        this.tableTunnel = new TableTunnel(odps);
        if (StringUtils.isNotEmpty(maxComputeSinkConfig.getMaxComputeTunnelUrl())) {
            this.tableTunnel.setEndpoint(maxComputeSinkConfig.getMaxComputeTunnelUrl());
        }
        this.maxComputeMetrics = maxComputeMetrics;
        this.ddlManager = initializeDdlManager();
    }

    /**
     * Retrieves the latest table schema definition from Alibaba MaxCompute backend.
     *
     * <p>The project, schema (dataset), and table name are read from the sink configuration. The call reaches
     * the MaxCompute backend and therefore reflects any schema changes applied since the previous read.</p>
     *
     * @return the latest table schema
     */
    public TableSchema getLatestTableSchema() {
        return odps.tables()
                .get(maxComputeSinkConfig.getMaxComputeProjectId(),
                        maxComputeSinkConfig.getMaxComputeSchema(),
                        maxComputeSinkConfig.getMaxComputeTableName())
                .getSchema();
    }

    /**
     * Creates or updates the table schema in Alibaba MaxCompute.
     * Creates the table if it does not exist, updates the table if it exists.
     *
     * <p>Delegates to the {@link DdlManager}, which creates the table when it does not yet exist and otherwise
     * applies the additive schema differences. The underlying operation is retried according to the configured
     * DDL retry policy.</p>
     *
     * @param tableSchema the table schema to be created or updated
     * @throws OdpsException if the table creation or update fails
     */
    public void createOrUpdateTable(TableSchema tableSchema) throws OdpsException {
        ddlManager.createOrUpdateTable(tableSchema);
    }

    /**
     * Create new InsertManager instance.
     *
     * <p>The concrete implementation produced by {@link InsertManagerFactory} depends on whether table
     * partitioning is enabled in the configuration.</p>
     *
     * @return InsertManager instance
     */
    public InsertManager createInsertManager() {
        return InsertManagerFactory.createInsertManager(maxComputeSinkConfig, tableTunnel, instrumentation, maxComputeMetrics);
    }

    /**
     * Builds and configures the {@link Odps} client from the sink configuration.
     *
     * <p>Creates an {@link AliyunAccount} from the configured access id and key, and then sets the default
     * project, ODPS endpoint, current schema, and global settings on the client.</p>
     *
     * @return a fully configured {@link Odps} client
     */
    private Odps initializeOdps() {
        Account account = new AliyunAccount(maxComputeSinkConfig.getMaxComputeAccessId(), maxComputeSinkConfig.getMaxComputeAccessKey());
        Odps odpsClient = new Odps(account);
        odpsClient.setDefaultProject(maxComputeSinkConfig.getMaxComputeProjectId());
        odpsClient.setEndpoint(maxComputeSinkConfig.getMaxComputeOdpsUrl());
        odpsClient.setCurrentSchema(maxComputeSinkConfig.getMaxComputeSchema());
        odpsClient.setGlobalSettings(maxComputeSinkConfig.getOdpsGlobalSettings());
        return odpsClient;
    }

    /**
     * Creates the {@link DdlManager} bound to this client's {@link Odps} instance, configuration, and metrics.
     *
     * @return a new {@link DdlManager} used to create and evolve the destination table
     */
    private DdlManager initializeDdlManager() {
        return new DdlManager(odps, maxComputeSinkConfig, instrumentation, maxComputeMetrics);
    }

}
