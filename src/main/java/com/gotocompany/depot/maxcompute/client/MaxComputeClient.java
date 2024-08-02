package com.gotocompany.depot.maxcompute.client;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.maxcompute.utils.ProtoToRecordUtils;

import java.io.IOException;

public class MaxComputeClient {
    private final String tunnelUrl;
    private final Account account;
    private final Odps odps;

    /**
     * Initializes a MaxCompute client.
     * @param accessId
     * @param accessKey
     * @param odpsUrl
     * @param tunnelUrl
     * @param project
     */
    public MaxComputeClient(String accessId, String accessKey, String odpsUrl, String tunnelUrl, String project) {
        this.account = new AliyunAccount(accessId, accessKey);
        odps = initializeOdps(odpsUrl, project);
        this.tunnelUrl = tunnelUrl;
    }

    /**
     * Creates a table if it does not exist, otherwise updates the table schema.
     * @param tableName
     * @param schema
     * @throws OdpsException
     */
    public void upsertTable(String tableName, TableSchema schema) throws OdpsException {
        if (!this.odps.tables().exists(tableName)) {
            this.odps.tables().create(tableName, schema);
        } else {
            updateTable(tableName, schema);
        }
    }

    /**
     * Inserts a dynamic message into a table.
     * @param tableName
     * @param dynamicMessage
     * @throws TunnelException
     * @throws IOException
     */
    public void insert(String tableName, DynamicMessage dynamicMessage) throws TunnelException, IOException {
        TableTunnel tableTunnel = new TableTunnel(odps);
        tableTunnel.setEndpoint(tunnelUrl);
        TableTunnel.StreamUploadSession streamUploadSession = tableTunnel.buildStreamUploadSession(odps.getDefaultProject(), tableName)
                .build();
        TableTunnel.StreamRecordPack streamRecordPack = streamUploadSession.newRecordPack();
        streamRecordPack.append(ProtoToRecordUtils.toRecord(streamUploadSession.getSchema(), dynamicMessage));
        streamRecordPack.flush();
    }

    private void updateTable(String tableName, TableSchema schema) {
        throw new RuntimeException("Update table operation is not supported yet");
    }

    /**
     * Initializes an Odps client.
     * @param odpsUrl
     * @param project
     * @return
     */
    private Odps initializeOdps(String odpsUrl, String project) {
        Odps odpsClient = new Odps(account);
        odpsClient.setDefaultProject(project);
        odpsClient.setEndpoint(odpsUrl);
        return odpsClient;
    }
}
