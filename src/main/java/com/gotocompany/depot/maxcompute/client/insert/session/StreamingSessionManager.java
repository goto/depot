package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;

import com.aliyun.odps.tunnel.TunnelException;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;

import java.time.Instant;

/**
 * StreamingSessionManager manages the streaming insert sessions for MaxCompute.
 * Streaming Insert Sessions are reused when the partition spec is the same.
 * Streaming sessions are created by TableTunnel service. Read more about it here: <a href="https://www.alibabacloud.com/help/en/maxcompute/user-guide/tabletunnel">Alibaba MaxCompute Table Tunnel</a>
 */
public final class StreamingSessionManager {

    private final LoadingCache<String, TableTunnel.StreamUploadSession> sessionCache;

    private StreamingSessionManager(LoadingCache<String, TableTunnel.StreamUploadSession> loadingCache) {
        this.sessionCache = loadingCache;
    }

    /**
     * Create a StreamingSessionManager for non-partitioned tables.
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
        CacheLoader<String, TableTunnel.StreamUploadSession> cacheLoader = partitionSpecKey -> buildStreamSession(getBaseStreamSessionBuilder(tableTunnel, maxComputeSinkConfig), instrumentation, maxComputeMetrics);
        return new StreamingSessionManager(Caffeine.newBuilder()
                .expireAfterAccess(maxComputeSinkConfig.getStreamingInsertSessionExpirationTimeAfterAccessDuration())
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
        CacheLoader<String, TableTunnel.StreamUploadSession> cacheLoader = partitionSpecKey -> buildStreamSession(getBaseStreamSessionBuilder(tableTunnel, maxComputeSinkConfig)
                        .setCreatePartition(true)
                        .setPartitionSpec(partitionSpecKey),
                instrumentation, maxComputeMetrics);
        return new StreamingSessionManager(Caffeine.newBuilder()
                .expireAfterAccess(maxComputeSinkConfig.getStreamingInsertSessionExpirationTimeAfterAccessDuration())
                .maximumSize(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .build(cacheLoader));
    }

    /**
     * Get the session for the given cache key.
     * If the session is not present in the cache, a new session is created and returned.
     * Creation of the session is done using the cache loader provided during the creation of the StreamingSessionManager.
     *
     * @param partitionSpec combination of partition spec
     * @return StreamUploadSession
     */
    public TableTunnel.StreamUploadSession getSession(String partitionSpec) {
        return sessionCache.get(partitionSpec);
    }

    /**
     * Refresh the session for the given cache key.
     * This is used whenever Table schema is updated.
     *
     * @param partitionSpec combination of partition spec
     */
    public void refreshSession(String partitionSpec) {
        sessionCache.refresh(partitionSpec);
    }

    /**
     * Invalidate all the sessions in the cache.
     */
    public void refreshAllSessions() {
        sessionCache.asMap()
                .keySet()
                .forEach(sessionCache::refresh);
    }

    private static TableTunnel.StreamUploadSession buildStreamSession(TableTunnel.StreamUploadSession.Builder streamUploadSessionBuilder,
                                                                      Instrumentation instrumentation,
                                                                      MaxComputeMetrics maxComputeMetrics) throws TunnelException {
        Instant start = Instant.now();
        TableTunnel.StreamUploadSession streamUploadSession = streamUploadSessionBuilder.build();
        instrumentation.captureDurationSince(maxComputeMetrics.getMaxComputeStreamingInsertSessionInitializationLatency(), start);
        instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeStreamingInsertSessionCreatedCount());
        return streamUploadSession;
    }

    private static TableTunnel.StreamUploadSession.Builder getBaseStreamSessionBuilder(TableTunnel tableTunnel, MaxComputeSinkConfig maxComputeSinkConfig) {
        return tableTunnel.buildStreamUploadSession(maxComputeSinkConfig.getMaxComputeProjectId(), maxComputeSinkConfig.getMaxComputeTableName())
                .allowSchemaMismatch(maxComputeSinkConfig.isAllowSchemaMismatchEnabled())
                .setSlotNum(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession());
    }

}
