package com.gotocompany.depot.maxcompute.client;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.insertManager.InsertManager;
import com.gotocompany.depot.maxcompute.client.insertManager.InsertManagerFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MaxComputeClient {
    private final Odps odps;
    private final MaxComputeSinkConfig maxComputeConfig;
    private final InsertManager insertManager;
    private final TableTunnel tableTunnel;

    public MaxComputeClient(MaxComputeSinkConfig maxComputeConfig) {
        this.maxComputeConfig = maxComputeConfig;
        this.odps = getOdpsClientInstance(maxComputeConfig);
        this.tableTunnel = new TableTunnel(odps);
        tableTunnel.setEndpoint(maxComputeConfig.getMaxComputeTunnelUrl());
        this.insertManager = initializeInsertManager();
    }

    public void upsertTable(TableSchema tableSchema) throws OdpsException {
        String tableName = maxComputeConfig.getMaxComputeTableName();
        if (!isTablePresent(tableName)) {
            createNewTable(tableName, tableSchema);
        }
    }

    private InsertManager initializeInsertManager() {
        return InsertManagerFactory.createInsertManager(maxComputeConfig, tableTunnel);
    }

    private static Odps getOdpsClientInstance(MaxComputeSinkConfig maxComputeConfig) {
        Odps odps = new Odps(new AliyunAccount(maxComputeConfig.getMaxComputeAccessId(), maxComputeConfig.getMaxComputeAccessKey()));
        odps.setEndpoint(maxComputeConfig.getMaxComputeOdpsUrl());
        odps.setDefaultProject(maxComputeConfig.getMaxComputeProjectId());
        odps.setCurrentSchema(maxComputeConfig.getMaxComputeSchema());
        return odps;
    }

    private boolean isTablePresent(String tableName) {
        try {
            return odps.tables().exists(tableName);
        } catch (OdpsException e) {
            log.error("Failed to verify table existence: {}", tableName, e);
            return false;
        }
    }

    private void createNewTable(String tableName, TableSchema tableSchema) throws OdpsException {
        odps.tables().create(
                odps.getDefaultProject(),
                tableName,
                tableSchema,
                "",
                false,
                maxComputeConfig.getMaxComputeTableLifecycleDays(),
                null,
                null
        );
    }
}
