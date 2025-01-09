package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.insert.session.StreamingSessionManager;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * PartitionedInsertManager is responsible for inserting partitioned records into MaxCompute.
 */
@Slf4j
public class PartitionedInsertManager extends InsertManager {

    public PartitionedInsertManager(MaxComputeSinkConfig maxComputeSinkConfig,
                                    Instrumentation instrumentation,
                                    MaxComputeMetrics maxComputeMetrics,
                                    StreamingSessionManager streamingSessionManager) {
        super(maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
    }

    /**
     * Insert records into MaxCompute.
     * Each thread will have its own StreamUploadSession for each partitionSpec handled by the thread.
     *
     * @param recordWrappers list of records to insert
     * @throws TunnelException if there is an error with the tunnel service, typically due to network issues
     * @throws IOException     typically thrown when issues such as schema mismatch occur
     */
    @Override
    public void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException {
        Map<String, List<RecordWrapper>> partitionSpecRecordWrapperMap = recordWrappers.stream()
                .collect(Collectors.groupingBy(record -> record.getPartitionSpec().toString()));
        for (Map.Entry<String, List<RecordWrapper>> entry : partitionSpecRecordWrapperMap.entrySet()) {
            TableTunnel.StreamUploadSession streamUploadSession = streamingSessionManager.getSession(entry.getKey());
            TableTunnel.StreamRecordPack recordPack = newRecordPack(streamUploadSession);
            for (RecordWrapper recordWrapper : entry.getValue()) {
                appendRecord(recordPack, recordWrapper, recordWrapper.getPartitionSpec().toString());
            }
            flushRecordPack(recordPack);
        }
    }

}
