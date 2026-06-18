package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link StreamingSessionManager}.
 *
 * <p>These tests verify the caching, eviction, and refresh semantics of the streaming-session cache for both
 * the partitioned and non-partitioned manager flavors. The MaxCompute Tunnel SDK is mocked: a
 * {@link TableTunnel} returns a stubbed {@link TableTunnel.StreamUploadSession.Builder} whose fluent
 * configuration methods return the builder itself and whose {@code build} yields a mocked
 * {@link TableTunnel.StreamUploadSession}. {@link MaxComputeSinkConfig} is stubbed with project and table names
 * and a maximum session count of one, so that least-recently-used eviction can be triggered using two distinct
 * keys.</p>
 *
 * @see StreamingSessionManager
 */
public class StreamingSessionManagerTest {

    /**
     * Mocked metric instrumentation; stubbed so that the session-creation latency captures are no-ops.
     */
    @Mock
    private Instrumentation instrumentation;

    /**
     * Mocked holder of MaxCompute metric identifiers passed to the session manager under test.
     */
    @Mock
    private MaxComputeMetrics maxComputeMetrics;

    /**
     * Initializes the Mockito-annotated fields and neutralizes value instrumentation before each test.
     *
     * <p>Initializes the {@link Mock}-annotated fields and stubs the three-argument {@code captureValue} so that
     * the metric emission performed while building a session has no side effect during the test.</p>
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        doNothing()
                .when(instrumentation)
                .captureValue(Mockito.any(), Mockito.any(), Mockito.any());
    }

    /**
     * Verifies that a partitioned manager builds a new session on the first lookup of a key.
     *
     * <p>Given a partitioned {@link StreamingSessionManager} backed by an empty cache, when
     * {@link StreamingSessionManager#getSession(String)} is called once, then the session is built by calling
     * {@link TableTunnel#buildStreamUploadSession(String, String)} exactly once for the configured project and
     * table, and the returned session is the instance produced by the builder.</p>
     *
     * @throws TunnelException never in this test; declared because session creation may throw it
     */
    @Test
    public void shouldCreateNewPartitionedSessionIfCacheIsEmpty() throws TunnelException {
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
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        StreamingSessionManager partitionedStreamingSessionManager =
                StreamingSessionManager.createPartitioned(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);

        TableTunnel.StreamUploadSession streamUploadSession =
                partitionedStreamingSessionManager.getSession("test_session");

        verify(tableTunnel, Mockito.times(1))
                .buildStreamUploadSession("test_project", "test_table");
        assertEquals(streamUploadSessionMock, streamUploadSession);
    }

    /**
     * Verifies that a partitioned manager returns the cached session on repeated lookups of the same key.
     *
     * <p>Given a partitioned {@link StreamingSessionManager}, when
     * {@link StreamingSessionManager#getSession(String)} is called twice with the same key, then the session is
     * built only once (verified by a single call to
     * {@link TableTunnel#buildStreamUploadSession(String, String)}) and both lookups return the same
     * instance.</p>
     *
     * @throws TunnelException never in this test; declared because session creation may throw it
     */
    @Test
    public void shouldReturnSameInstanceIfCacheNotEmpty() throws TunnelException {
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
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        StreamingSessionManager partitionedStreamingSessionManager =
                StreamingSessionManager.createPartitioned(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);

        TableTunnel.StreamUploadSession streamUploadSession =
                partitionedStreamingSessionManager.getSession("test_session");
        TableTunnel.StreamUploadSession secondStreamUploadSession =
                partitionedStreamingSessionManager.getSession("test_session");

        verify(tableTunnel, Mockito.times(1))
                .buildStreamUploadSession("test_project", "test_table");
        assertEquals(streamUploadSessionMock, streamUploadSession);
        assertEquals(streamUploadSession, secondStreamUploadSession);
    }

    /**
     * Verifies that the bounded cache evicts an entry and rebuilds when a second distinct key exceeds capacity.
     *
     * <p>Given a partitioned {@link StreamingSessionManager} whose configured maximum session count is one,
     * when {@link StreamingSessionManager#getSession(String)} is called with two different keys, then the
     * least-recently-used entry is evicted and a session is built twice (verified by two calls to
     * {@link TableTunnel#buildStreamUploadSession(String, String)}).</p>
     *
     * @throws TunnelException never in this test; declared because session creation may throw it
     */
    @Test
    public void shouldEvictOldLatestInstanceWhenCapacityExceeded() throws TunnelException {
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
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        StreamingSessionManager partitionedStreamingSessionManager =
                StreamingSessionManager.createPartitioned(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);

        TableTunnel.StreamUploadSession streamUploadSession =
                partitionedStreamingSessionManager.getSession("test_session");
        TableTunnel.StreamUploadSession secondStreamUploadSession =
                partitionedStreamingSessionManager.getSession("different_test_session");

        verify(tableTunnel, Mockito.times(2))
                .buildStreamUploadSession("test_project", "test_table");
        assertEquals(streamUploadSessionMock, streamUploadSession);
        assertEquals(streamUploadSession, secondStreamUploadSession);
    }

    /**
     * Verifies that a non-partitioned manager builds a new session on the first lookup.
     *
     * <p>Given a non-partitioned {@link StreamingSessionManager} backed by an empty cache, when
     * {@link StreamingSessionManager#getSession(String)} is called once, then the session is built by calling
     * {@link TableTunnel#buildStreamUploadSession(String, String)} exactly once for the configured project and
     * table, and the returned session is the instance produced by the builder.</p>
     *
     * @throws TunnelException never in this test; declared because session creation may throw it
     */
    @Test
    public void shouldCreateNewNonPartitionedSessionIfCacheIsEmpty() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        StreamingSessionManager nonPartitionedStreamingSessionManager =
                StreamingSessionManager.createNonPartitioned(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);

        TableTunnel.StreamUploadSession streamUploadSession =
                nonPartitionedStreamingSessionManager.getSession("test_session");

        verify(tableTunnel, Mockito.times(1))
                .buildStreamUploadSession("test_project", "test_table");
        assertEquals(streamUploadSessionMock, streamUploadSession);
    }

    /**
     * Verifies that a non-partitioned manager returns the cached session on repeated lookups.
     *
     * <p>Given a non-partitioned {@link StreamingSessionManager}, when
     * {@link StreamingSessionManager#getSession(String)} is called twice with the same key, then the session is
     * built only once (verified by a single call to
     * {@link TableTunnel#buildStreamUploadSession(String, String)}) and both lookups return the same
     * instance.</p>
     *
     * @throws TunnelException never in this test; declared because session creation may throw it
     */
    @Test
    public void shouldReturnSameNonPartitionedSessionIfCacheNotEmpty() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        StreamingSessionManager nonPartitionedStreamingSessionManager =
                StreamingSessionManager.createNonPartitioned(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);

        TableTunnel.StreamUploadSession streamUploadSession =
                nonPartitionedStreamingSessionManager.getSession("test_session");
        TableTunnel.StreamUploadSession secondStreamUploadSession =
                nonPartitionedStreamingSessionManager.getSession("test_session");

        verify(tableTunnel, Mockito.times(1))
                .buildStreamUploadSession("test_project", "test_table");
        assertEquals(streamUploadSessionMock, streamUploadSession);
        assertEquals(streamUploadSession, secondStreamUploadSession);
    }

    /**
     * Verifies that refreshing a session forces it to be rebuilt for its cache key.
     *
     * <p>Given a non-partitioned {@link StreamingSessionManager} that has already loaded a session for a key,
     * when {@link StreamingSessionManager#refreshSession(String)} is invoked for that key, then the session is
     * reloaded, resulting in two total calls to
     * {@link TableTunnel#buildStreamUploadSession(String, String)}.</p>
     *
     * @throws TunnelException never in this test; declared because session creation may throw it
     */
    @Test
    public void shouldReturnRefreshTheSession() throws TunnelException {
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        when(builder.setSlotNum(Mockito.anyLong()))
                .thenReturn(builder);
        TableTunnel.StreamUploadSession streamUploadSessionMock = Mockito.mock(TableTunnel.StreamUploadSession.class);
        when(builder.build())
                .thenReturn(streamUploadSessionMock);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("test_project");
        when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("test_table");
        when(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .thenReturn(1);
        when(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                .thenReturn(1L);
        StreamingSessionManager nonPartitionedStreamingSessionManager =
                StreamingSessionManager.createNonPartitioned(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);

        nonPartitionedStreamingSessionManager.getSession("test_session");
        nonPartitionedStreamingSessionManager.refreshSession("test_session");

        verify(tableTunnel, Mockito.times(2))
                .buildStreamUploadSession("test_project", "test_table");
    }

}
