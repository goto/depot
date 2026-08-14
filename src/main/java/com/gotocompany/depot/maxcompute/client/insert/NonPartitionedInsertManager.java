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

/**
 * NonPartitionedInsertManager is responsible for inserting non-partitioned records into MaxCompute.
 *
 * <p>All records of a batch are written through a single streaming upload session, keyed by the fixed
 * {@link #NON_PARTITIONED} cache key. Each thread that calls {@link #insert(List)} obtains its own session from
 * the {@link StreamingSessionManager}.</p>
 *
 * @see InsertManager
 * @see PartitionedInsertManager
 */
@Slf4j
public class NonPartitionedInsertManager extends InsertManager {

    /**
     * Fixed session-cache key identifying the single streaming session of a non-partitioned table.
     */
    private static final String NON_PARTITIONED = "non-partitioned";

    /**
     * Creates a non-partitioned insert manager.
     *
     * @param maxComputeSinkConfig    the MaxCompute sink configuration
     * @param instrumentation         metric instrumentation used to time and count insert operations
     * @param maxComputeMetrics       holder of MaxCompute metric identifiers and tag templates
     * @param streamingSessionManager manager that supplies the single streaming upload session
     */
    public NonPartitionedInsertManager(MaxComputeSinkConfig maxComputeSinkConfig,
                                       Instrumentation instrumentation,
                                       MaxComputeMetrics maxComputeMetrics,
                                       StreamingSessionManager streamingSessionManager) {
        super(maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
    }

    /**
     * Insert records into MaxCompute.
     * Each thread will have its own StreamUploadSession.
     *
     * <p>Obtains (or lazily creates) this thread's session under the {@link #NON_PARTITIONED} key, allocates a
     * record pack, appends every record, and flushes the pack once. Each thread therefore uses its own
     * {@code StreamUploadSession}.</p>
     *
     * @param recordWrappers list of records to insert
     * @throws TunnelException if there is an error with the tunnel service, typically due to network issues
     * @throws IOException typically thrown when issues such as schema mismatch occur
     */
    @Override
    public void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException {
        TableTunnel.StreamUploadSession streamUploadSession = super.getStreamingSessionManager().getSession(NON_PARTITIONED);
        TableTunnel.StreamRecordPack recordPack = newRecordPack(streamUploadSession);
        for (RecordWrapper recordWrapper : recordWrappers) {
            super.appendRecord(recordPack, recordWrapper, NON_PARTITIONED);
        }
        super.flushRecordPack(recordPack);
    }

}
