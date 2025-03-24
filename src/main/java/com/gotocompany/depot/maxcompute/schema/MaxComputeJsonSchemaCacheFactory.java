package com.gotocompany.depot.maxcompute.schema;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;

public class MaxComputeJsonSchemaCacheFactory {
    public static DepotStencilUpdateListener create(
            MaxComputeSinkConfig maxComputeSinkConfig,
            PartitioningStrategy partitioningStrategy,
            MaxComputeClient maxComputeClient,
            MetadataUtil metadataUtil
    ) {
        return new MaxComputeJsonSchemaCache(maxComputeSinkConfig,
                new MaxComputeJsonSchemaBuilder(maxComputeSinkConfig, partitioningStrategy, metadataUtil),
                maxComputeClient
        );
    }
}
