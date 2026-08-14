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
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link NonPartitionedInsertManager}.
 *
 * <p>These tests exercise the streaming-insert flow for a non-partitioned MaxCompute table. The MaxCompute
 * Tunnel SDK is fully mocked: a {@link TableTunnel} yields a stubbed
 * {@link TableTunnel.StreamUploadSession.Builder} chain that produces a spied
 * {@link TableTunnel.StreamUploadSession}, which in turn hands back a mocked
 * {@link TableTunnel.StreamRecordPack}. A real {@link StreamingSessionManager} is built from these mocks so
 * that the production caching logic runs, while {@link MaxComputeSinkConfig} and {@link MaxComputeMetrics} are
 * stubbed to provide project, table, flush-timeout, compression, and metric-name values.</p>
 *
 * <p>The scenarios cover a plain successful flush, a successful flush with compression enabled (capturing the
 * {@link CompressOption} that is applied), and the translation of a {@link SchemaMismatchException} thrown
 * during record append or during flush into a {@link NonRetryableException}.</p>
 *
 * @see NonPartitionedInsertManager
 * @see StreamingSessionManager
 */
public class NonPartitionedInsertManagerTest {

    /**
     * Mocked metric instrumentation; stubbed to ignore counter captures so the tests can focus on the
     * insert flow rather than on metric side effects.
     */
    @Mock
    private Instrumentation instrumentation;

    /**
     * Initializes the Mockito-annotated fields and neutralizes counter instrumentation before each test.
     *
     * <p>Initializes the {@link Mock}-annotated {@link #instrumentation} field and stubs the three-argument
     * {@code captureCount} so that metric emission performed by the manager has no side effect during the
     * test.</p>
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.any(), Mockito.any(), Mockito.any());
    }

    /**
     * Verifies that a batch of records is appended and flushed exactly once for a non-partitioned table.
     *
     * <p>Given a fully stubbed tunnel whose record pack reports a flush result of two records, and a
     * {@link NonPartitionedInsertManager} built around a real non-partitioned {@link StreamingSessionManager},
     * when {@link NonPartitionedInsertManager#insert(List)} is called with a single {@link RecordWrapper}, then
     * the underlying {@link TableTunnel.StreamRecordPack} is flushed exactly once through
     * {@link TableTunnel.StreamRecordPack#flush(TableTunnel.FlushOption)}.</p>
     *
     * @throws IOException     never in this test; declared because the production insert path may throw it
     * @throws TunnelException never in this test; declared because the production insert path may throw it
     */
    @Test
    public void shouldFlushAllTheRecords() throws IOException, TunnelException {
        TableTunnel.FlushResult flushResult = Mockito.mock(TableTunnel.FlushResult.class);
        when(flushResult.getRecordCount())
                .thenReturn(2L);
        TableTunnel.StreamRecordPack streamRecordPack = Mockito.mock(TableTunnel.StreamRecordPack.class);
        TableTunnel.StreamUploadSession streamUploadSession = Mockito.spy(TableTunnel.StreamUploadSession.class);
        when(streamRecordPack.flush(Mockito.any(TableTunnel.FlushOption.class)))
                .thenReturn(flushResult);
        when(streamUploadSession.newRecordPack())
                .thenReturn(streamRecordPack);
        when(streamRecordPack.flush())
                .thenReturn("traceId");
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        when(builder.build())
                .thenReturn(streamUploadSession);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("table");
        when(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeoutMs())
                .thenReturn(1000L);
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        MaxComputeMetrics maxComputeMetrics = Mockito.mock(MaxComputeMetrics.class);
        when(maxComputeMetrics.getMaxComputeFlushRecordMetric())
                .thenReturn("flush_record");
        when(maxComputeMetrics.getMaxComputeFlushSizeMetric())
                .thenReturn("flush_size");
        StreamingSessionManager streamingSessionManager = StreamingSessionManager.createNonPartitioned(
                tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics
        );
        NonPartitionedInsertManager nonPartitionedInsertManager = new NonPartitionedInsertManager(maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
        List<RecordWrapper> recordWrappers = Collections.singletonList(
                Mockito.mock(RecordWrapper.class)
        );

        nonPartitionedInsertManager.insert(recordWrappers);

        verify(streamRecordPack, Mockito.times(1))
                .flush(Mockito.any(TableTunnel.FlushOption.class));
    }

    /**
     * Verifies that the configured compression settings are applied to the record pack when streaming-insert
     * compression is enabled.
     *
     * <p>Given a {@link MaxComputeSinkConfig} mock that enables compression with the
     * {@link CompressOption.CompressAlgorithm#ODPS_RAW} algorithm, level {@code 1}, and strategy {@code 1}, and
     * a session whose {@link TableTunnel.StreamUploadSession#newRecordPack(CompressOption)} argument is recorded
     * by an {@link ArgumentCaptor}, when {@link NonPartitionedInsertManager#insert(List)} is called with a
     * single record, then the pack is flushed exactly once and the captured {@link CompressOption} is asserted
     * to carry the configured algorithm, strategy, and level.</p>
     *
     * @throws IOException     never in this test; declared because the production insert path may throw it
     * @throws TunnelException never in this test; declared because the production insert path may throw it
     */
    @Test
    public void shouldFlushAllTheRecordsWithCompressOption() throws IOException, TunnelException {
        TableTunnel.FlushResult flushResult = Mockito.mock(TableTunnel.FlushResult.class);
        when(flushResult.getRecordCount())
                .thenReturn(2L);
        TableTunnel.StreamRecordPack streamRecordPack = Mockito.mock(TableTunnel.StreamRecordPack.class);
        TableTunnel.StreamUploadSession streamUploadSession = Mockito.spy(TableTunnel.StreamUploadSession.class);
        when(streamRecordPack.flush(Mockito.any(TableTunnel.FlushOption.class)))
                .thenReturn(flushResult);
        ArgumentCaptor<CompressOption> compressOptionArgumentCaptor = ArgumentCaptor.forClass(CompressOption.class);
        when(streamUploadSession.newRecordPack(compressOptionArgumentCaptor.capture()))
                .thenReturn(streamRecordPack);
        when(streamRecordPack.flush())
                .thenReturn("traceId");
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        when(builder.build())
                .thenReturn(streamUploadSession);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("table");
        when(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeoutMs())
                .thenReturn(1000L);
        when(maxComputeSinkConfig.isStreamingInsertCompressEnabled())
                .thenReturn(true);
        when(maxComputeSinkConfig.getMaxComputeCompressionAlgorithm())
                .thenReturn(CompressOption.CompressAlgorithm.ODPS_RAW);
        when(maxComputeSinkConfig.getMaxComputeCompressionLevel())
                .thenReturn(1);
        when(maxComputeSinkConfig.getMaxComputeCompressionStrategy())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        MaxComputeMetrics maxComputeMetrics = Mockito.mock(MaxComputeMetrics.class);
        when(maxComputeMetrics.getMaxComputeFlushRecordMetric())
                .thenReturn("flush_record");
        when(maxComputeMetrics.getMaxComputeFlushSizeMetric())
                .thenReturn("flush_size");
        StreamingSessionManager streamingSessionManager = StreamingSessionManager.createNonPartitioned(
                tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics
        );
        NonPartitionedInsertManager nonPartitionedInsertManager = new NonPartitionedInsertManager(maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
        List<RecordWrapper> recordWrappers = Collections.singletonList(
                Mockito.mock(RecordWrapper.class)
        );

        nonPartitionedInsertManager.insert(recordWrappers);

        verify(streamRecordPack, Mockito.times(1))
                .flush(Mockito.any(TableTunnel.FlushOption.class));
        assertEquals(compressOptionArgumentCaptor.getValue().algorithm, CompressOption.CompressAlgorithm.ODPS_RAW);
        assertEquals(compressOptionArgumentCaptor.getValue().strategy, 1);
        assertEquals(compressOptionArgumentCaptor.getValue().level, 1);
    }

    /**
     * Verifies that a schema mismatch encountered while appending a record is surfaced as a
     * {@link NonRetryableException}.
     *
     * <p>Given a record pack whose {@code append} method is stubbed to throw a {@link SchemaMismatchException},
     * when {@link NonPartitionedInsertManager#insert(List)} is called, then the manager treats the mismatch as
     * unrecoverable and rethrows it as a {@link NonRetryableException}, which the test asserts through the
     * {@code expected} attribute of the {@link Test} annotation.</p>
     *
     * @throws IOException     never in this test; declared because the production insert path may throw it
     * @throws TunnelException never in this test; declared because the production insert path may throw it
     */
    @Test(expected = NonRetryableException.class)
    public void shouldWrapToNonRetryableExceptionWhenSchemaMismatchHappenDuringAppend() throws IOException, TunnelException {
        TableTunnel.FlushResult flushResult = Mockito.mock(TableTunnel.FlushResult.class);
        when(flushResult.getRecordCount())
                .thenReturn(2L);
        TableTunnel.StreamRecordPack streamRecordPack = Mockito.mock(TableTunnel.StreamRecordPack.class);
        TableTunnel.StreamUploadSession streamUploadSession = Mockito.spy(TableTunnel.StreamUploadSession.class);
        when(streamRecordPack.flush(Mockito.any(TableTunnel.FlushOption.class)))
                .thenReturn(flushResult);
        ArgumentCaptor<CompressOption> compressOptionArgumentCaptor = ArgumentCaptor.forClass(CompressOption.class);
        when(streamUploadSession.newRecordPack(compressOptionArgumentCaptor.capture()))
                .thenReturn(streamRecordPack);
        Mockito.doThrow(SchemaMismatchException.class)
                .when(streamRecordPack)
                .append(Mockito.any());
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        when(builder.build())
                .thenReturn(streamUploadSession);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("table");
        when(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeoutMs())
                .thenReturn(1000L);
        when(maxComputeSinkConfig.isStreamingInsertCompressEnabled())
                .thenReturn(true);
        when(maxComputeSinkConfig.getMaxComputeCompressionAlgorithm())
                .thenReturn(CompressOption.CompressAlgorithm.ODPS_RAW);
        when(maxComputeSinkConfig.getMaxComputeCompressionLevel())
                .thenReturn(1);
        when(maxComputeSinkConfig.getMaxComputeCompressionStrategy())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        MaxComputeMetrics maxComputeMetrics = Mockito.mock(MaxComputeMetrics.class);
        when(maxComputeMetrics.getMaxComputeFlushRecordMetric())
                .thenReturn("flush_record");
        when(maxComputeMetrics.getMaxComputeFlushSizeMetric())
                .thenReturn("flush_size");
        StreamingSessionManager streamingSessionManager = Mockito.spy(StreamingSessionManager.createNonPartitioned(
                tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics
        ));
        NonPartitionedInsertManager nonPartitionedInsertManager = new NonPartitionedInsertManager(maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
        List<RecordWrapper> recordWrappers = Collections.singletonList(
                Mockito.mock(RecordWrapper.class)
        );

        nonPartitionedInsertManager.insert(recordWrappers);
    }

    /**
     * Verifies that a schema mismatch encountered while flushing a record pack is surfaced as a
     * {@link NonRetryableException}.
     *
     * <p>Given a record pack whose {@code append} succeeds but whose
     * {@link TableTunnel.StreamRecordPack#flush(TableTunnel.FlushOption)} is stubbed to throw a
     * {@link SchemaMismatchException}, when {@link NonPartitionedInsertManager#insert(List)} is called, then the
     * manager treats the mismatch as unrecoverable and rethrows it as a {@link NonRetryableException}, which the
     * test asserts through the {@code expected} attribute of the {@link Test} annotation.</p>
     *
     * @throws IOException     never in this test; declared because the production insert path may throw it
     * @throws TunnelException never in this test; declared because the production insert path may throw it
     */
    @Test(expected = NonRetryableException.class)
    public void shouldWrapToNonRetryableExceptionWhenSchemaMismatchHappenDuringFlush() throws IOException, TunnelException {
        TableTunnel.FlushResult flushResult = Mockito.mock(TableTunnel.FlushResult.class);
        when(flushResult.getRecordCount())
                .thenReturn(2L);
        TableTunnel.StreamRecordPack streamRecordPack = Mockito.mock(TableTunnel.StreamRecordPack.class);
        TableTunnel.StreamUploadSession streamUploadSession = Mockito.spy(TableTunnel.StreamUploadSession.class);
        doThrow(SchemaMismatchException.class)
                .when(streamRecordPack)
                .flush(Mockito.any(TableTunnel.FlushOption.class));
        ArgumentCaptor<CompressOption> compressOptionArgumentCaptor = ArgumentCaptor.forClass(CompressOption.class);
        when(streamUploadSession.newRecordPack(compressOptionArgumentCaptor.capture()))
                .thenReturn(streamRecordPack);
        Mockito.doNothing()
                .when(streamRecordPack)
                .append(Mockito.any());
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        when(builder.build())
                .thenReturn(streamUploadSession);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("table");
        when(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeoutMs())
                .thenReturn(1000L);
        when(maxComputeSinkConfig.isStreamingInsertCompressEnabled())
                .thenReturn(true);
        when(maxComputeSinkConfig.getMaxComputeCompressionAlgorithm())
                .thenReturn(CompressOption.CompressAlgorithm.ODPS_RAW);
        when(maxComputeSinkConfig.getMaxComputeCompressionLevel())
                .thenReturn(1);
        when(maxComputeSinkConfig.getMaxComputeCompressionStrategy())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        MaxComputeMetrics maxComputeMetrics = Mockito.mock(MaxComputeMetrics.class);
        when(maxComputeMetrics.getMaxComputeFlushRecordMetric())
                .thenReturn("flush_record");
        when(maxComputeMetrics.getMaxComputeFlushSizeMetric())
                .thenReturn("flush_size");
        StreamingSessionManager streamingSessionManager = Mockito.spy(StreamingSessionManager.createNonPartitioned(
                tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics
        ));
        NonPartitionedInsertManager nonPartitionedInsertManager = new NonPartitionedInsertManager(maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
        List<RecordWrapper> recordWrappers = Collections.singletonList(
                Mockito.mock(RecordWrapper.class)
        );

        nonPartitionedInsertManager.insert(recordWrappers);
    }
}
