package com.gotocompany.depot.maxcompute.record;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RecordDecoratorFactory {

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final MaxComputeSchemaCache maxComputeSchemaCache;
    private final ConverterOrchestrator converterOrchestrator;
    private final ProtoMessageParser protoMessageParser;
    private final SinkConfig sinkConfig;
    private final PartitioningStrategy partitioningStrategy;

    public RecordDecorator createRecordDecorator() {
        RecordDecorator dataColumnRecordDecorator = new ProtoDataColumnRecordDecorator(null, converterOrchestrator, maxComputeSchemaCache, protoMessageParser, sinkConfig, partitioningStrategy);
        if (!maxComputeSinkConfig.shouldAddMetadata()) {
            return dataColumnRecordDecorator;
        }
        return new ProtoMetadataColumnRecordDecorator(dataColumnRecordDecorator, maxComputeSinkConfig, maxComputeSchemaCache);
    }

}
