package com.gotocompany.depot.maxcompute.client.insertManager;

import com.aliyun.odps.tunnel.TableTunnel;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

public class InsertManagerFactory {

    public static InsertManager createInsertManager(MaxComputeSinkConfig maxComputeConfig, TableTunnel tableTunnel) {
        if (maxComputeConfig.isTablePartitioningEnabled()) {
            return new PartitionedInsertManager(maxComputeConfig, tableTunnel);
        } else {
            return new NonPartitionedInsertManager(maxComputeConfig, tableTunnel);
        }
    }
}