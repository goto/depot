package com.gotocompany.depot.maxcompute.client;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;

public class MaxComputeClient {
    private final String tunnelUrl;
    private final Account account;
    private final Odps odps;

    public MaxComputeClient(String accessId, String accessKey, String odpsUrl, String tunnelUrl, String project) {
        this.account = new AliyunAccount(accessId, accessKey);
        odps = initializeOdps(odpsUrl, project);
        this.tunnelUrl = tunnelUrl;
    }

    public void upsertTable(String tableName, TableSchema schema) throws OdpsException {
        if (!this.odps.tables().exists(tableName)) {
            this.odps.tables().create(tableName, schema);
        } else {
            updateTable(tableName, schema);
        }
    }

    private void updateTable(String tableName, TableSchema schema) {
        throw new RuntimeException("Update table operation is not supported yet");
    }

    private Odps initializeOdps(String odpsUrl, String project) {
        Odps odpsClient = new Odps(account);
        odpsClient.setDefaultProject(project);
        odpsClient.setEndpoint(odpsUrl);
        return odpsClient;
    }
}
