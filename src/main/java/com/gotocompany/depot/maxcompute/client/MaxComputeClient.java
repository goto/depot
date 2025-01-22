package com.gotocompany.depot.maxcompute.client;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.type.TypeInfoFactory;
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

/**
 * MaxComputeClient is a client to interact with MaxCompute.
 * It provides methods to execute table creation and update and inserting records into MaxCompute.
 */
@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class MaxComputeClient {

    private Odps odps;
    private MaxComputeSinkConfig maxComputeSinkConfig;
    private TableTunnel tableTunnel;
    private DdlManager ddlManager;
    private MaxComputeMetrics maxComputeMetrics;
    private Instrumentation instrumentation;

    public MaxComputeClient(MaxComputeSinkConfig maxComputeSinkConfig,
                            StatsDReporter statsDReporter,
                            MaxComputeMetrics maxComputeMetrics) {
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.instrumentation = new Instrumentation(statsDReporter, this.getClass());
        this.odps = initializeOdps();
        this.tableTunnel = new TableTunnel(odps);
        this.tableTunnel.setEndpoint(maxComputeSinkConfig.getMaxComputeTunnelUrl());
        this.maxComputeMetrics = maxComputeMetrics;
        this.ddlManager = initializeDdlManager();
    }

    /**
     * Retrieves the latest table schema definition from Alibaba MaxCompute backend.
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
     * @param tableSchema the table schema to be created or updated
     * @throws OdpsException if the table creation or update fails
     */
    public void createOrUpdateTable(TableSchema tableSchema) throws OdpsException {
        ddlManager.createOrUpdateTable(tableSchema);
    }

    /**
     * Create new InsertManager instance.
     *
     * @return InsertManager instance
     */
    public InsertManager createInsertManager() {
        return InsertManagerFactory.createInsertManager(maxComputeSinkConfig, tableTunnel, instrumentation, maxComputeMetrics);
    }

    private Odps initializeOdps() {
        Account account = new AliyunAccount(maxComputeSinkConfig.getMaxComputeAccessId(), maxComputeSinkConfig.getMaxComputeAccessKey());
        Odps odpsClient = new Odps(account);
        odpsClient.setDefaultProject(maxComputeSinkConfig.getMaxComputeProjectId());
        odpsClient.setEndpoint(maxComputeSinkConfig.getMaxComputeOdpsUrl());
        odpsClient.setCurrentSchema(maxComputeSinkConfig.getMaxComputeSchema());
        odpsClient.setGlobalSettings(maxComputeSinkConfig.getOdpsGlobalSettings());
        return odpsClient;
    }

    private DdlManager initializeDdlManager() {
        return new DdlManager(odps, maxComputeSinkConfig, instrumentation, maxComputeMetrics);
    }

}
