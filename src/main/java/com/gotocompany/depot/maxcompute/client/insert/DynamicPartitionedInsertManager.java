package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.impl.PartitionRecord;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.insert.session.StreamingSessionManager;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DynamicPartitionedInsertManager extends InsertManager {

    protected DynamicPartitionedInsertManager(MaxComputeSinkConfig maxComputeSinkConfig,
                                              Instrumentation instrumentation,
                                              MaxComputeMetrics maxComputeMetrics,
                                              StreamingSessionManager streamingSessionManager) {
        super(maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
    }

    @Override
    public void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException {
        Map<String, List<RecordWrapper>> partitionSpecRecordWrapperMap = recordWrappers.stream()
                .collect(Collectors.groupingBy(record -> record.getPartitionSpec().toString()));
        for (Map.Entry<String, List<RecordWrapper>> entry : partitionSpecRecordWrapperMap.entrySet()) {
            TableTunnel.StreamUploadSession streamUploadSession = super.getStreamingSessionManager().getSession(entry.getKey());
            TableTunnel.StreamRecordPack recordPack = newRecordPack(streamUploadSession);
            for (RecordWrapper recordWrapper : entry.getValue()) {
                ((PartitionRecord) recordWrapper.getRecord()).setPartition(recordWrapper.getPartitionSpec());
                super.appendRecord(recordPack, recordWrapper, recordWrapper.getPartitionSpec().toString());
            }
            super.flushRecordPack(recordPack);
        }
    }

}
