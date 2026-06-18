package com.gotocompany.depot.maxcompute.schema;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;

/**
 * Factory that assembles a fully-wired {@link MaxComputeSchemaCache}, including the
 * {@link MaxComputeSchemaBuilder} it depends on.
 *
 * <p>It centralises the construction of the schema cache so callers need only supply the
 * collaborators and configuration, keeping the wiring of the builder and cache in one place.</p>
 */
public class MaxComputeSchemaCacheFactory {

    /**
     * Creates a {@link MaxComputeSchemaCache} backed by a freshly constructed
     * {@link MaxComputeSchemaBuilder}.
     *
     * @param protobufConverterOrchestrator orchestrator used for Protobuf-to-MaxCompute type mapping
     *        and whose cache is cleared on schema updates
     * @param maxComputeSinkConfig MaxCompute-specific sink configuration
     * @param partitioningStrategy partitioning strategy supplying the partition column, or {@code null}
     *        when partitioning is disabled
     * @param sinkConfig general sink configuration identifying the tracked Protobuf schema class
     * @param maxComputeClient client used to upsert the table and read back its schema
     * @param metadataUtil helper used to resolve metadata column types
     * @return a configured schema cache ready to track schema updates
     */
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
