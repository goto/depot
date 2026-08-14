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
 *
 * <p>Records of a batch are grouped by their partition spec, and each group is written through the streaming
 * session associated with that partition spec. As a result a single {@link #insert(List)} call may use several
 * sessions, one per distinct partition spec present in the batch.</p>
 *
 * @see InsertManager
 * @see NonPartitionedInsertManager
 */
@Slf4j
public class PartitionedInsertManager extends InsertManager {

    /**
     * Creates a partitioned insert manager.
     *
     * @param maxComputeSinkConfig    the MaxCompute sink configuration
     * @param instrumentation         metric instrumentation used to time and count insert operations
     * @param maxComputeMetrics       holder of MaxCompute metric identifiers and tag templates
     * @param streamingSessionManager manager that supplies one streaming upload session per partition spec
     */
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
     * <p>The records are first grouped by the string form of their partition spec. For every group the method
     * obtains (or lazily creates) the session keyed by that partition spec, allocates a record pack, appends the
     * group's records, and flushes the pack. Each thread therefore holds one {@code StreamUploadSession} per
     * partition spec it handles.</p>
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
            TableTunnel.StreamUploadSession streamUploadSession = super.getStreamingSessionManager().getSession(entry.getKey());
            TableTunnel.StreamRecordPack recordPack = newRecordPack(streamUploadSession);
            for (RecordWrapper recordWrapper : entry.getValue()) {
                super.appendRecord(recordPack, recordWrapper, recordWrapper.getPartitionSpec().toString());
            }
            super.flushRecordPack(recordPack);
        }
    }

}
