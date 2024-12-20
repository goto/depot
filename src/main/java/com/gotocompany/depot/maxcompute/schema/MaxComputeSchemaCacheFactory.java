package com.gotocompany.depot.maxcompute.schema;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;

public class MaxComputeSchemaCacheFactory {

    public static MaxComputeSchemaCache createMaxComputeSchemaCache(
            ProtobufConverterOrchestrator protobufConverterOrchestrator,
            MaxComputeSinkConfig maxComputeSinkConfig,
            PartitioningStrategy partitioningStrategy,
            SinkConfig sinkConfig,
            MaxComputeClient maxComputeClient,
            MetadataUtil metadataUtil
    ) {
        return new MaxComputeSchemaCache(
                new MaxComputeSchemaBuilder(protobufConverterOrchestrator, maxComputeSinkConfig, partitioningStrategy, metadataUtil),
                sinkConfig,
                protobufConverterOrchestrator, maxComputeClient
        );
    }
}
