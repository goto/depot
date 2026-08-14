package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;

import com.aliyun.odps.tunnel.TunnelException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;

import java.time.Instant;

/**
 * StreamingSessionManager manages the streaming insert sessions for MaxCompute.
 * Streaming Insert Sessions are reused when the partition spec is the same.
 * Streaming sessions are created by TableTunnel service. Read more about it here: <a href="https://www.alibabacloud.com/help/en/maxcompute/user-guide/tabletunnel">Alibaba MaxCompute Table Tunnel</a>
 *
 * <p>Sessions are relatively expensive to create, so they are held in a Guava {@link LoadingCache} keyed by
 * partition spec and reused while the key stays the same. The cache is bounded by the configured maximum
 * session count and evicts entries using a least-recently-used policy. Two flavors are offered through the
 * static factory methods: a non-partitioned manager that backs a single logical session, and a partitioned
 * manager that builds one session per partition spec (creating the partition on demand).</p>
 */
public final class StreamingSessionManager {

    /**
     * Bounded, lazily populated cache of streaming upload sessions keyed by partition spec.
     */
    private final LoadingCache<String, TableTunnel.StreamUploadSession> sessionCache;

    /**
     * Creates a session manager backed by the given loading cache.
     *
     * @param loadingCache the cache that lazily builds and stores the streaming upload sessions by partition spec
     */
    private StreamingSessionManager(LoadingCache<String, TableTunnel.StreamUploadSession> loadingCache) {
        this.sessionCache = loadingCache;
    }

    /**
     * Create a StreamingSessionManager for non-partitioned tables.
     *
     * <p>The backing cache loads a session that is not bound to any partition spec, regardless of the key it is
     * queried with, and is bounded by the configured maximum session count.</p>
     *
     * @param tableTunnel          interface to connect with the MaxCompute tunnel service
     * @param maxComputeSinkConfig configuration for the MaxCompute sink
     * @param instrumentation      metrics instrumentation
     * @param maxComputeMetrics    metrics for MaxCompute
     * @return StreamingSessionManager
     */
    public static StreamingSessionManager createNonPartitioned(TableTunnel tableTunnel,
                                                               MaxComputeSinkConfig maxComputeSinkConfig,
                                                               Instrumentation instrumentation,
                                                               MaxComputeMetrics maxComputeMetrics) {
        CacheLoader<String, TableTunnel.StreamUploadSession> cacheLoader = new CacheLoader<String, TableTunnel.StreamUploadSession>() {
            @Override
            public TableTunnel.StreamUploadSession load(String partitionSpecKey) throws TunnelException {
                return buildStreamSession(getBaseStreamSessionBuilder(tableTunnel, maxComputeSinkConfig), instrumentation, maxComputeMetrics);
            }
        };
        return new StreamingSessionManager(CacheBuilder.newBuilder()
                .maximumSize(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .build(cacheLoader));
    }

    /**
     * Create a StreamingSessionManager for partitioned tables.
     * Each session is created with a partition spec. Sessions are cached and reused when the partition spec is the same.
     * Caching is done using a LoadingCache, using the combination of partition spec and thread name as the key.
     * Cache is bounded by the maximum number of sessions defined in the configuration.
     * Cache entry eviction is done using LRU(Least Recently Used) policy, when the cache size exceeds the maximum size.
     *
     * @param tableTunnel          interface to connect with the MaxCompute tunnel service
     * @param maxComputeSinkConfig configuration for the MaxCompute sink
     * @param instrumentation      metrics instrumentation
     * @param maxComputeMetrics    metrics for MaxCompute
     * @return StreamingSessionManager
     */
    public static StreamingSessionManager createPartitioned(TableTunnel tableTunnel,
                                                            MaxComputeSinkConfig maxComputeSinkConfig,
                                                            Instrumentation instrumentation,
                                                            MaxComputeMetrics maxComputeMetrics) {
        CacheLoader<String, TableTunnel.StreamUploadSession> cacheLoader = new CacheLoader<String, TableTunnel.StreamUploadSession>() {
            @Override
            public TableTunnel.StreamUploadSession load(String partitionSpecKey) throws TunnelException {
                return buildStreamSession(getBaseStreamSessionBuilder(tableTunnel, maxComputeSinkConfig)
                                .setCreatePartition(true)
                                .setPartitionSpec(partitionSpecKey),
                        instrumentation, maxComputeMetrics);
            }
        };
        return new StreamingSessionManager(CacheBuilder.newBuilder()
                .maximumSize(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .build(cacheLoader));
    }

    /**
     * Get the session for the given cache key.
     * If the session is not present in the cache, a new session is created and returned.
     * Creation of the session is done using the cache loader provided during the creation of the StreamingSessionManager.
     *
     * <p>If no session is cached for the key, one is built on demand by the cache loader supplied when this
     * manager was created, then stored and returned. The lookup uses the unchecked cache accessor, so a failure
     * to build the session surfaces as an unchecked exception.</p>
     *
     * @param partitionSpec combination of partition spec
     * @return StreamUploadSession
     */
    public TableTunnel.StreamUploadSession getSession(String partitionSpec) {
        return sessionCache.getUnchecked(partitionSpec);
    }

    /**
     * Refresh the session for the given cache key.
     * This is used whenever Table schema is updated.
     *
     * <p>Historically invoked after a table-schema update so that a subsequent insert would use a session bound
     * to the new schema.</p>
     *
     * @param partitionSpec combination of partition spec
     * @deprecated schema refresh is no longer driven through session reloading; retained only for backward
     *             compatibility and may be removed in a future release
     */
    @Deprecated
    public void refreshSession(String partitionSpec) {
        sessionCache.refresh(partitionSpec);
    }

    /**
     * Invalidate all the sessions in the cache.
     *
     * @deprecated schema refresh is no longer driven through session reloading; retained only for backward
     *             compatibility and may be removed in a future release
     */
    @Deprecated
    public void refreshAllSessions() {
        sessionCache.asMap()
                .keySet()
                .forEach(sessionCache::refresh);
    }

    /**
     * Builds a streaming upload session from the given builder and records the creation metrics.
     *
     * <p>Times how long the build takes, emitting it as the session-initialization latency, and increments the
     * session-created counter.</p>
     *
     * @param streamUploadSessionBuilder the builder configured for the target table and, optionally, the partition
     * @param instrumentation            metric instrumentation used to record the creation latency and count
     * @param maxComputeMetrics          holder of MaxCompute metric identifiers
     * @return the newly built {@link TableTunnel.StreamUploadSession}
     * @throws TunnelException if the tunnel service fails to create the session
     */
    private static TableTunnel.StreamUploadSession buildStreamSession(TableTunnel.StreamUploadSession.Builder streamUploadSessionBuilder,
                                                                      Instrumentation instrumentation,
                                                                      MaxComputeMetrics maxComputeMetrics) throws TunnelException {
        Instant start = Instant.now();
        TableTunnel.StreamUploadSession streamUploadSession = streamUploadSessionBuilder.build();
        instrumentation.captureDurationSince(maxComputeMetrics.getMaxComputeStreamingInsertSessionInitializationLatency(), start);
        instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeStreamingInsertSessionCreatedCount());
        return streamUploadSession;
    }

    /**
     * Creates the base stream-upload session builder shared by both manager flavors.
     *
     * <p>The builder targets the configured project and table, toggles schema-mismatch tolerance according to
     * the configuration, and sets the per-session tunnel slot count.</p>
     *
     * @param tableTunnel          handle used to build the session
     * @param maxComputeSinkConfig configuration providing the project, table, schema-mismatch flag, and slot count
     * @return a partially configured {@link TableTunnel.StreamUploadSession.Builder}
     */
    private static TableTunnel.StreamUploadSession.Builder getBaseStreamSessionBuilder(TableTunnel tableTunnel, MaxComputeSinkConfig maxComputeSinkConfig) {
        return tableTunnel.buildStreamUploadSession(maxComputeSinkConfig.getMaxComputeProjectId(), maxComputeSinkConfig.getMaxComputeTableName())
                .allowSchemaMismatch(maxComputeSinkConfig.isAllowSchemaMismatchEnabled())
                .setSlotNum(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession());
    }

}
