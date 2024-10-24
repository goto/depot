package com.gotocompany.depot.maxcompute.client;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

import java.util.HashMap;
import java.util.Map;

public class MaxComputeClient {

    private final Odps odps;
    private final MaxComputeSinkConfig maxComputeSinkConfig;

    public MaxComputeClient(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.odps = initializeOdps();
    }

    public void upsertTable(TableSchema tableSchema) throws OdpsException {
        String tableName = maxComputeSinkConfig.getMaxComputeTableName();
        if (!this.odps.tables().exists(tableName)) {
            this.odps.tables().create(odps.getDefaultProject(), tableName, tableSchema, "",
                    false, maxComputeSinkConfig.getMaxComputeTableLifecycleDays(),
                    null, null);
            return;
        }
    }

    private Odps initializeOdps() {
        Account account = new AliyunAccount(maxComputeSinkConfig.getMaxComputeAccessId(), maxComputeSinkConfig.getMaxComputeAccessKey());
        Odps odpsClient = new Odps(account);
        odpsClient.setDefaultProject(maxComputeSinkConfig.getMaxComputeProjectId());
        odpsClient.setEndpoint(maxComputeSinkConfig.getMaxComputeOdpsUrl());
        odpsClient.setCurrentSchema(maxComputeSinkConfig.getMaxComputeSchema());
        odpsClient.setGlobalSettings(getGlobalSettings());
        return odpsClient;
    }

    private Map<String, String> getGlobalSettings() {
        Map<String, String> globalSettings = new HashMap<>();
        globalSettings.put("setproject odps.schema.evolution.enable", "true");
        globalSettings.put("odps.namespace.schema", "true");
        return globalSettings;
    }

}
