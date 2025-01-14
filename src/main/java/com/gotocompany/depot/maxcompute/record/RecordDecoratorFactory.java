package com.gotocompany.depot.maxcompute.record;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.schema.ProtobufMaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import lombok.RequiredArgsConstructor;

public class RecordDecoratorFactory {

    /**
     * Create a record decorator based on the sink configuration.
     * Creates a nested decorator in case of metadata column being enabled.
     *
     * @param recordDecoratorConfig record decorator configuration
     * @return record decorator
     */
    public static RecordDecorator createRecordDecorator(RecordDecoratorConfig recordDecoratorConfig) {
        RecordDecorator dataColumnRecordDecorator = new ProtoDataColumnRecordDecorator(null,
                recordDecoratorConfig.protobufConverterOrchestrator,
                recordDecoratorConfig.messageParser,
                recordDecoratorConfig.sinkConfig,
                recordDecoratorConfig.partitioningStrategy,
                recordDecoratorConfig.statsDReporter,
                recordDecoratorConfig.maxComputeMetrics);
        if (!recordDecoratorConfig.maxComputeSinkConfig.shouldAddMetadata()) {
            return dataColumnRecordDecorator;
        }
        return new ProtoMetadataColumnRecordDecorator(dataColumnRecordDecorator, recordDecoratorConfig.maxComputeSinkConfig,
                recordDecoratorConfig.protobufMaxComputeSchemaCache, recordDecoratorConfig.metadataUtil);
    }

    @RequiredArgsConstructor
    public static class RecordDecoratorConfig {
        private final ProtobufConverterOrchestrator protobufConverterOrchestrator;
        private final ProtobufMaxComputeSchemaCache protobufMaxComputeSchemaCache;
        private final MessageParser messageParser;
        private final PartitioningStrategy partitioningStrategy;
        private final MaxComputeSinkConfig maxComputeSinkConfig;
        private final SinkConfig sinkConfig;
        private final StatsDReporter statsDReporter;
        private final MaxComputeMetrics maxComputeMetrics;
        private final MetadataUtil metadataUtil;
    }

}
