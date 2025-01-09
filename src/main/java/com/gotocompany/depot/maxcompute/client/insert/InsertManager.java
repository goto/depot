package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.exceptions.SchemaMismatchException;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.NonRetryableException;
import com.gotocompany.depot.maxcompute.client.insert.session.StreamingSessionManager;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * InsertManager is responsible for inserting records into MaxCompute.
 */
@Slf4j
public abstract class InsertManager {

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final Instrumentation instrumentation;
    private final MaxComputeMetrics maxComputeMetrics;
    private final TableTunnel.FlushOption flushOption;
    protected final StreamingSessionManager streamingSessionManager;

    protected InsertManager(MaxComputeSinkConfig maxComputeSinkConfig, Instrumentation instrumentation,
                            MaxComputeMetrics maxComputeMetrics,
                            StreamingSessionManager streamingSessionManager) {
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.instrumentation = instrumentation;
        this.maxComputeMetrics = maxComputeMetrics;
        this.streamingSessionManager = streamingSessionManager;
        this.flushOption = new TableTunnel.FlushOption()
                .timeout(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeoutMs());
    }

    /**
     * Insert records into MaxCompute.
     * @param recordWrappers list of records to insert
     * @throws TunnelException if there is an error with the tunnel service, typically due to network issues
     * @throws IOException typically thrown when issues such as schema mismatch occur
     */
    public abstract void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException;

    /**
     * Create a new record pack for streaming insert.
     * Record pack encloses the records to be inserted.
     *
     * @param streamUploadSession session for streaming insert
     * @return TableTunnel.StreamRecordPack
     * @throws IOException typically thrown when issues such as schema mismatch occur
     * @throws TunnelException if there is an error with the tunnel service, typically due to network issues
     */
    protected TableTunnel.StreamRecordPack newRecordPack(TableTunnel.StreamUploadSession streamUploadSession) throws IOException, TunnelException {
        if (!maxComputeSinkConfig.isStreamingInsertCompressEnabled()) {
            return streamUploadSession.newRecordPack();
        }
        return streamUploadSession.newRecordPack(new CompressOption(maxComputeSinkConfig.getMaxComputeCompressionAlgorithm(),
                maxComputeSinkConfig.getMaxComputeCompressionLevel(),
                maxComputeSinkConfig.getMaxComputeCompressionStrategy()));
    }

    /**
     * Instrument the insert operation.
     *
     * @param start start time of the operation
     * @param flushResult result of the flush operation
     */
    private void instrument(Instant start, TableTunnel.FlushResult flushResult) {
        instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeOperationTotalMetric(),
                String.format(MaxComputeMetrics.MAXCOMPUTE_API_TAG, MaxComputeMetrics.MaxComputeAPIType.TABLE_INSERT));
        instrumentation.captureDurationSince(maxComputeMetrics.getMaxComputeOperationLatencyMetric(), start,
                String.format(MaxComputeMetrics.MAXCOMPUTE_API_TAG, MaxComputeMetrics.MaxComputeAPIType.TABLE_INSERT));
        instrumentation.captureCount(maxComputeMetrics.getMaxComputeFlushRecordMetric(), flushResult.getRecordCount(),
                String.format(MaxComputeMetrics.MAXCOMPUTE_COMPRESSION_TAG, maxComputeSinkConfig.isStreamingInsertCompressEnabled(), maxComputeSinkConfig.getMaxComputeCompressionAlgorithm()));
        instrumentation.captureCount(maxComputeMetrics.getMaxComputeFlushSizeMetric(), flushResult.getFlushSize(),
                String.format(MaxComputeMetrics.MAXCOMPUTE_COMPRESSION_TAG, maxComputeSinkConfig.isStreamingInsertCompressEnabled(), maxComputeSinkConfig.getMaxComputeCompressionAlgorithm()));
    }

    /**
     * Append a record to the record pack.
     * When schema mismatch occurs, wrap the exception in a NonRetryableException. It is not possible to recover from schema mismatch.
     * When network partition occurs, refresh the schema and rethrow the exception.
     *
     * @param recordPack recordPack to append the record to
     * @param recordWrapper record to append
     * @param sessionKey key to identify the session, used for refreshing the schema
     * @throws IOException typically thrown when issues such as network partition occur
     */
    protected void appendRecord(TableTunnel.StreamRecordPack recordPack, RecordWrapper recordWrapper, String sessionKey) throws IOException {
        try {
            recordPack.append(recordWrapper.getRecord());
        } catch (SchemaMismatchException e) {
            log.error("Record pack schema Mismatch", e);
            throw new NonRetryableException("Record pack schema Mismatch", e);
        } catch (IOException e) {
            log.info("IOException occurs, refreshing the sessions", e);
            streamingSessionManager.refreshAllSessions();
            throw e;
        }
    }

    /**
     * Flush the record pack.
     * When schema mismatch occurs, wrap the exception in a NonRetryableException. It is not possible to recover from schema mismatch.
     * When network partition occurs, typically indicated by IOException being thrown, refresh the schema and rethrow the exception.
     *
     * @param recordPack recordPack to flush
     * @throws IOException typically thrown when issues such as network partition occur
     */
    protected void flushRecordPack(TableTunnel.StreamRecordPack recordPack) throws IOException {
        Instant start = Instant.now();
        try {
            TableTunnel.FlushResult flushResult = recordPack.flush(flushOption);
            instrument(start, flushResult);
        } catch (SchemaMismatchException e) {
            log.error("Record pack schema Mismatch", e);
            throw new NonRetryableException("Record pack schema Mismatch", e);
        } catch (IOException e) {
            log.info("TunnelException occurs, refreshing the sessions", e);
            streamingSessionManager.refreshAllSessions();
            throw e;
        }
    }

}
