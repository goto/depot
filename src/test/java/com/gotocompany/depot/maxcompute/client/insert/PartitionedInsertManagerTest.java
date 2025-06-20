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

public class PartitionedInsertManagerTest {

    @Mock
    private Instrumentation instrumentation;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        doNothing()
                .when(instrumentation)
                .captureCount(Mockito.any(), Mockito.any(), Mockito.any());
    }

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
