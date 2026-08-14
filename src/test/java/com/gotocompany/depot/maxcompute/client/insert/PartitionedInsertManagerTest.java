package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.PartitionSpec;
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
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PartitionedInsertManager}.
 *
 * <p>These tests exercise the streaming-insert flow for a partitioned MaxCompute table. The MaxCompute Tunnel
 * SDK is fully mocked: a {@link TableTunnel} yields a stubbed
 * {@link TableTunnel.StreamUploadSession.Builder} chain (including the partition-specific
 * {@code setCreatePartition} and {@code setPartitionSpec} calls) that produces a spied
 * {@link TableTunnel.StreamUploadSession}, which in turn hands back a mocked
 * {@link TableTunnel.StreamRecordPack}. A real {@link StreamingSessionManager} is built from these mocks so
 * that the production per-partition caching logic runs, while {@link MaxComputeSinkConfig} and
 * {@link MaxComputeMetrics} are stubbed to provide project, table, flush-timeout, compression, and metric-name
 * values. Records are supplied as {@link RecordWrapper} mocks, each returning a distinct {@link PartitionSpec}
 * so that grouping by partition can be observed.</p>
 *
 * <p>The scenarios cover grouping records by partition spec and flushing each group, the same flow with
 * compression enabled (capturing the {@link CompressOption} that is applied), and the translation of a
 * {@link SchemaMismatchException} thrown during record append or during flush into a
 * {@link NonRetryableException}.</p>
 *
 * @see PartitionedInsertManager
 * @see StreamingSessionManager
 */
public class PartitionedInsertManagerTest {

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
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.any(), Mockito.any(), Mockito.any());
    }

    /**
     * Verifies that records are grouped by partition spec and each group is flushed through its own session.
     *
     * <p>Given two {@link RecordWrapper} mocks carrying the distinct partition specs {@code ds=1} and
     * {@code ds=2}, and a {@link PartitionedInsertManager} built around a real partitioned
     * {@link StreamingSessionManager}, when {@link PartitionedInsertManager#insert(List)} is called, then the
     * records are partitioned into two groups and {@link TableTunnel.StreamRecordPack#flush(TableTunnel.FlushOption)}
     * is invoked exactly twice, once per partition.</p>
     *
     * @throws IOException     never in this test; declared because the production insert path may throw it
     * @throws TunnelException never in this test; declared because the production insert path may throw it
     */
    @Test
    public void shouldGroupRecordsBasedOnPartitionSpecAndFlushAll() throws IOException, TunnelException {
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
        when(builder.setCreatePartition(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setPartitionSpec(Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
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
        RecordWrapper firstPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(firstPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=1"));
        RecordWrapper secondPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(secondPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=2"));
        List<RecordWrapper> recordWrappers = Arrays.asList(
                firstPartitionRecordWrapper,
                secondPartitionRecordWrapper
        );
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        StreamingSessionManager streamingSessionManager = StreamingSessionManager.createPartitioned(
                tableTunnel, maxComputeSinkConfig, instrumentation, Mockito.mock(MaxComputeMetrics.class)
        );
        PartitionedInsertManager partitionedInsertManager = new PartitionedInsertManager(maxComputeSinkConfig, instrumentation, Mockito.mock(MaxComputeMetrics.class), streamingSessionManager);
        int expectedPartitionFlushInvocation = 2;

        partitionedInsertManager.insert(recordWrappers);

        verify(streamRecordPack, Mockito.times(expectedPartitionFlushInvocation))
                .flush(Mockito.any(TableTunnel.FlushOption.class));
    }

    /**
     * Verifies that records are grouped by partition spec and flushed with the configured compression applied.
     *
     * <p>Given a {@link MaxComputeSinkConfig} mock that enables compression with the
     * {@link CompressOption.CompressAlgorithm#ODPS_RAW} algorithm, level {@code 1}, and strategy {@code 1}, two
     * record wrappers in the partitions {@code ds=1} and {@code ds=2}, and a session whose
     * {@link TableTunnel.StreamUploadSession#newRecordPack(CompressOption)} argument is recorded by an
     * {@link ArgumentCaptor}, when {@link PartitionedInsertManager#insert(List)} is called, then the captured
     * {@link CompressOption} is asserted to carry the configured algorithm, level, and strategy, and the record
     * pack is flushed exactly twice, once per partition.</p>
     *
     * @throws IOException     never in this test; declared because the production insert path may throw it
     * @throws TunnelException never in this test; declared because the production insert path may throw it
     */
    @Test
    public void shouldGroupRecordsBasedOnPartitionSpecAndFlushAllWithCompression() throws IOException, TunnelException {
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
        when(builder.setCreatePartition(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setPartitionSpec(Mockito.anyString()))
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
        RecordWrapper firstPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(firstPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=1"));
        RecordWrapper secondPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(secondPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=2"));
        List<RecordWrapper> recordWrappers = Arrays.asList(
                firstPartitionRecordWrapper,
                secondPartitionRecordWrapper
        );
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        StreamingSessionManager streamingSessionManager = StreamingSessionManager.createPartitioned(
                tableTunnel, maxComputeSinkConfig, instrumentation, Mockito.mock(MaxComputeMetrics.class)
        );
        PartitionedInsertManager partitionedInsertManager = new PartitionedInsertManager(maxComputeSinkConfig,
                instrumentation, Mockito.mock(MaxComputeMetrics.class), streamingSessionManager);
        int expectedPartitionFlushInvocation = 2;

        partitionedInsertManager.insert(recordWrappers);

        assertEquals(compressOptionArgumentCaptor.getValue().algorithm, CompressOption.CompressAlgorithm.ODPS_RAW);
        assertEquals(compressOptionArgumentCaptor.getValue().level, 1);
        assertEquals(compressOptionArgumentCaptor.getValue().strategy, 1);
        verify(streamRecordPack, Mockito.times(expectedPartitionFlushInvocation))
                .flush(Mockito.any(TableTunnel.FlushOption.class));
    }

    /**
     * Verifies that a schema mismatch encountered while appending a record is surfaced as a
     * {@link NonRetryableException}.
     *
     * <p>Given a record pack whose {@code append} method is stubbed to throw a {@link SchemaMismatchException},
     * when {@link PartitionedInsertManager#insert(List)} is called with records spanning two partitions, then
     * the manager treats the mismatch as unrecoverable and rethrows it as a {@link NonRetryableException}, which
     * the test asserts through the {@code expected} attribute of the {@link Test} annotation.</p>
     *
     * @throws IOException     never in this test; declared because the production insert path may throw it
     * @throws TunnelException never in this test; declared because the production insert path may throw it
     */
    @Test(expected = NonRetryableException.class)
    public void shouldWrapExceptionToNonRetryableExceptionWhenSchemaMismatchExceptionOccurredDuringRecordAppend() throws IOException, TunnelException {
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
        Mockito.doThrow(new SchemaMismatchException("schema mismatch", "v1"))
                .when(streamRecordPack)
                .append(Mockito.any());
        when(streamRecordPack.flush())
                .thenReturn("traceId");
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.setCreatePartition(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setPartitionSpec(Mockito.anyString()))
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
        RecordWrapper firstPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(firstPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=1"));
        RecordWrapper secondPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(secondPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=2"));
        List<RecordWrapper> recordWrappers = Arrays.asList(
                firstPartitionRecordWrapper,
                secondPartitionRecordWrapper
        );
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        StreamingSessionManager streamingSessionManager = Mockito.spy(StreamingSessionManager.createPartitioned(
                tableTunnel, maxComputeSinkConfig, instrumentation, Mockito.mock(MaxComputeMetrics.class)
        ));
        PartitionedInsertManager partitionedInsertManager = new PartitionedInsertManager(maxComputeSinkConfig,
                instrumentation, Mockito.mock(MaxComputeMetrics.class), streamingSessionManager);

        partitionedInsertManager.insert(recordWrappers);
    }

    /**
     * Verifies that a schema mismatch encountered while flushing a record pack is surfaced as a
     * {@link NonRetryableException}.
     *
     * <p>Given a record pack whose {@code append} succeeds but whose {@code flush} is stubbed to throw a
     * {@link SchemaMismatchException}, when {@link PartitionedInsertManager#insert(List)} is called with records
     * spanning two partitions, then the manager treats the mismatch as unrecoverable and rethrows it as a
     * {@link NonRetryableException}, which the test asserts through the {@code expected} attribute of the
     * {@link Test} annotation.</p>
     *
     * @throws IOException     never in this test; declared because the production insert path may throw it
     * @throws TunnelException never in this test; declared because the production insert path may throw it
     */
    @Test(expected = NonRetryableException.class)
    public void shouldWrapToNonRetryableExceptionWhenSchemaMismatchHappensDuringFlush() throws IOException, TunnelException {
        TableTunnel.FlushResult flushResult = Mockito.mock(TableTunnel.FlushResult.class);
        when(flushResult.getRecordCount())
                .thenReturn(2L);
        TableTunnel.StreamRecordPack streamRecordPack = Mockito.mock(TableTunnel.StreamRecordPack.class);
        TableTunnel.StreamUploadSession streamUploadSession = Mockito.spy(TableTunnel.StreamUploadSession.class);
        doThrow(new SchemaMismatchException("schema mismatch", "v1"))
                .when(streamRecordPack)
                .flush(Mockito.any());
        ArgumentCaptor<CompressOption> compressOptionArgumentCaptor = ArgumentCaptor.forClass(CompressOption.class);
        when(streamUploadSession.newRecordPack(compressOptionArgumentCaptor.capture()))
                .thenReturn(streamRecordPack);
        Mockito.doNothing()
                .when(streamRecordPack)
                .append(Mockito.any());
        when(streamRecordPack.flush())
                .thenReturn("traceId");
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.setCreatePartition(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setPartitionSpec(Mockito.anyString()))
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
        RecordWrapper firstPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(firstPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=1"));
        RecordWrapper secondPartitionRecordWrapper = Mockito.mock(RecordWrapper.class);
        when(secondPartitionRecordWrapper.getPartitionSpec())
                .thenReturn(new PartitionSpec("ds=2"));
        List<RecordWrapper> recordWrappers = Arrays.asList(
                firstPartitionRecordWrapper,
                secondPartitionRecordWrapper
        );
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        StreamingSessionManager streamingSessionManager = Mockito.spy(StreamingSessionManager.createPartitioned(
                tableTunnel, maxComputeSinkConfig, instrumentation, Mockito.mock(MaxComputeMetrics.class)
        ));
        PartitionedInsertManager partitionedInsertManager = new PartitionedInsertManager(maxComputeSinkConfig,
                instrumentation, Mockito.mock(MaxComputeMetrics.class), streamingSessionManager);

        partitionedInsertManager.insert(recordWrappers);
    }

}
