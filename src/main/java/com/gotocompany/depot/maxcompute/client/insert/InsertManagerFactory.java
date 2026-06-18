package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.insert.session.StreamingSessionManager;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;

/**
 * Factory class to create InsertManager based on the configuration.
 *
 * <p>The choice is driven by whether table partitioning is enabled: a {@link PartitionedInsertManager} is
 * produced for partitioned tables and a {@link NonPartitionedInsertManager} otherwise. Each manager is paired
 * with a matching {@link StreamingSessionManager}, created for the partitioned or non-partitioned case as
 * appropriate.</p>
 */
public class InsertManagerFactory {

    /**
     * Creates an InsertManager based on the configuration.
     * PartitionedInsertManager is created if table partitioning is enabled. Otherwise, NonPartitionedInsertManager is created.
     * Each InsertManager uses a StreamingSessionManager to manage the streaming session.
     *
     * @param maxComputeSinkConfig configuration for MaxCompute sink
     * @param tableTunnel          tunnel service for MaxCompute
     * @param instrumentation      metrics instrumentation
     * @param maxComputeMetrics    metrics for MaxCompute
     * @return InsertManager
     */
    public static InsertManager createInsertManager(MaxComputeSinkConfig maxComputeSinkConfig,
                                                    TableTunnel tableTunnel,
                                                    Instrumentation instrumentation,
                                                    MaxComputeMetrics maxComputeMetrics) {
        if (maxComputeSinkConfig.isTablePartitioningEnabled()) {
            StreamingSessionManager partitionedStreamingSessionManager = StreamingSessionManager.createPartitioned(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);
            return new PartitionedInsertManager(maxComputeSinkConfig, instrumentation, maxComputeMetrics, partitionedStreamingSessionManager);
        } else {
            StreamingSessionManager nonPartitionedStreamingSessionManager = StreamingSessionManager.createNonPartitioned(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);
            return new NonPartitionedInsertManager(maxComputeSinkConfig, instrumentation, maxComputeMetrics, nonPartitionedStreamingSessionManager);
        }
    }

}
