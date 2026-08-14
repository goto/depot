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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * InsertManager is responsible for inserting records into MaxCompute.
 *
 * <p>It centralizes the mechanics shared by all insert strategies: obtaining record packs from a streaming
 * upload session (optionally compressed), appending records, flushing packs, and emitting the associated
 * metrics. Concrete subclasses implement {@link #insert(List)} to decide how records are grouped onto
 * sessions:</p>
 * <ul>
 *     <li>{@link NonPartitionedInsertManager} uses a single session per thread;</li>
 *     <li>{@link PartitionedInsertManager} uses one session per partition spec.</li>
 * </ul>
 *
 * <p>Schema-mismatch failures are treated as unrecoverable and surfaced as {@link NonRetryableException},
 * whereas I/O and tunnel failures are allowed to propagate so that the caller can retry. The Lombok
 * {@code @Getter} annotation generates accessors that expose the shared collaborators to subclasses and
 * tests.</p>
 *
 * @see StreamingSessionManager
 * @see NonPartitionedInsertManager
 * @see PartitionedInsertManager
 */
@Getter
@Slf4j
public abstract class InsertManager {

    /**
     * Sink configuration carrying compression, flush-timeout, and related streaming-insert settings.
     */
    private final MaxComputeSinkConfig maxComputeSinkConfig;
    /**
     * Metric instrumentation used to time and count insert operations.
     */
    private final Instrumentation instrumentation;
    /**
     * Holder of MaxCompute metric identifiers and tag templates.
     */
    private final MaxComputeMetrics maxComputeMetrics;
    /**
     * Supplies and caches the streaming upload sessions keyed by partition spec.
     */
    private final StreamingSessionManager streamingSessionManager;
    /**
     * Flush option built once from the configured record-pack flush timeout and reused for every flush.
     */
    private final TableTunnel.FlushOption flushOption;

    /**
     * Initializes the state shared by all insert strategies.
     *
     * <p>Builds the {@link TableTunnel.FlushOption} once from the configured record-pack flush timeout so that
     * it can be reused for every flush.</p>
     *
     * @param maxComputeSinkConfig    the sink configuration providing compression and flush-timeout settings
     * @param instrumentation         metric instrumentation used to time and count insert operations
     * @param maxComputeMetrics       holder of MaxCompute metric identifiers and tag templates
     * @param streamingSessionManager manager that supplies and caches the streaming upload sessions
     */
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
     *
     * <p>Implementations group the records onto one or more streaming upload sessions, append them to record
     * packs, and flush those packs. The concrete grouping strategy is defined by the subclass.</p>
     *
     * @param recordWrappers list of records to insert
     * @throws TunnelException if there is an error with the tunnel service, typically due to network issues
     * @throws IOException typically thrown when issues such as schema mismatch occur
     */
    public abstract void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException;

    /**
     * Create a new record pack for streaming insert.
     * Record pack encloses the records to be inserted.
     *
     * <p>A record pack accumulates the records that are flushed together. When streaming-insert compression is
     * enabled the pack is created with a {@link CompressOption} built from the configured algorithm, level, and
     * strategy; otherwise an uncompressed pack is created.</p>
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
     * <p>Emits the insert operation counter and latency, plus the flushed record count and flushed byte size;
     * the latter two are tagged with the compression settings in effect.</p>
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
     * <p>A {@link SchemaMismatchException} means the record no longer matches the table schema; this is
     * unrecoverable for the streaming insert, so it is logged and rethrown as a {@link NonRetryableException}.
     * Any other {@link IOException}, typically caused by a network partition, is allowed to propagate so that
     * the caller can refresh the schema and retry.</p>
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
        }
    }

    /**
     * Flush the record pack.
     * When schema mismatch occurs, wrap the exception in a NonRetryableException. It is not possible to recover from schema mismatch.
     * When network partition occurs, typically indicated by IOException being thrown, refresh the schema and rethrow the exception.
     *
     * <p>On success the flush metrics are recorded. A {@link SchemaMismatchException} is unrecoverable and is
     * logged and rethrown as a {@link NonRetryableException}; any other {@link IOException}, typically caused by
     * a network partition, propagates so that the caller can refresh the schema and retry.</p>
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
        }
    }

}
