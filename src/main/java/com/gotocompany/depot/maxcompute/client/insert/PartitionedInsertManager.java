package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class PartitionedInsertManager implements InsertManager {

    private final PartitioningStrategy partitioningStrategy;
    private final TableTunnel tableTunnel;
    private final MaxComputeSinkConfig maxComputeSinkConfig;

    @Override
    public void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException {
        Map<PartitionSpec, List<RecordWrapper>> partitionSpecRecordWrapperMap = recordWrappers.stream()
                .collect(Collectors.groupingBy(partitioningStrategy::getPartitionSpec));
        for (Map.Entry<PartitionSpec, List<RecordWrapper>> entry : partitionSpecRecordWrapperMap.entrySet()) {
            TableTunnel.StreamUploadSession streamUploadSession = getStreamUploadSession(entry.getKey());
            TableTunnel.StreamRecordPack recordPack = streamUploadSession.newRecordPack();
            for (RecordWrapper recordWrapper : entry.getValue()) {
                recordPack.append(recordWrapper.getRecord());
            }
            TableTunnel.FlushResult flushResult = recordPack.flush(
                    new TableTunnel.FlushOption()
                            .timeout(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeout()));
            log.info("Flushed {} records to partition {}", flushResult.getRecordCount(), entry.getKey());
        }
    }

    private TableTunnel.StreamUploadSession getStreamUploadSession(PartitionSpec partitionSpec) throws TunnelException {
        return tableTunnel.buildStreamUploadSession(maxComputeSinkConfig.getMaxComputeProjectId(),
                        maxComputeSinkConfig.getMaxComputeTableName())
                .setPartitionSpec(partitionSpec)
                .build();
    }

}
