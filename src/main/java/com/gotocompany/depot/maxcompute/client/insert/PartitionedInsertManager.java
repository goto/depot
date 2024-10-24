package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class PartitionedInsertManager implements InsertManager {

    private final PartitioningStrategy<?> partitioningStrategy;
    private final TableTunnel tableTunnel;
    private final MaxComputeSinkConfig maxComputeSinkConfig;

    @Override
    public void insert(List<Record> records) {

    }

    private TableTunnel.StreamUploadSession getStreamUploadSession(String partitionKey) throws TunnelException {
        return tableTunnel.buildStreamUploadSession(maxComputeSinkConfig.getMaxComputeProjectId(),
                        maxComputeSinkConfig.getMaxComputeTableName())
                .setPartitionSpec(getPartitionSpec(partitionKey))
                .build();
    }

    private PartitionSpec getPartitionSpec(String partitionKey) {
        return new PartitionSpec(String.format("%s=%s", partitioningStrategy.getPartitionColumn(), partitionKey));
    }
}
