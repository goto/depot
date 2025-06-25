package com.gotocompany.depot.maxcompute;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.converter.record.ProtoMessageRecordConverter;
import com.gotocompany.depot.maxcompute.record.RecordDecorator;
import com.gotocompany.depot.maxcompute.record.RecordDecoratorFactory;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCacheFactory;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategyFactory;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class MaxComputeSinkFactory {

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final SinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;
    private final StencilClient stencilClient;
    private final ProtobufConverterOrchestrator protobufConverterOrchestrator;
    private final MaxComputeMetrics maxComputeMetrics;
    private final MaxComputeClient maxComputeClient;
    private final MetadataUtil metadataUtil;

    private MaxComputeSchemaCache maxComputeSchemaCache;
    private PartitioningStrategy partitioningStrategy;
    private MessageParser messageParser;

    public MaxComputeSinkFactory(StatsDReporter statsDReporter,
                                 StencilClient stencilClient,
                                 Map<String, String> env) {
        this.statsDReporter = statsDReporter;
        this.maxComputeSinkConfig = ConfigFactory.create(MaxComputeSinkConfig.class, env);
        this.sinkConfig = ConfigFactory.create(SinkConfig.class, env);
        this.stencilClient = stencilClient;
        this.protobufConverterOrchestrator = new ProtobufConverterOrchestrator(maxComputeSinkConfig);
        this.maxComputeMetrics = new MaxComputeMetrics(sinkConfig);
        this.maxComputeClient = new MaxComputeClient(maxComputeSinkConfig, statsDReporter, maxComputeMetrics);
        this.metadataUtil = new MetadataUtil(maxComputeSinkConfig);
    }

    public void init() {
        Descriptors.Descriptor descriptor = stencilClient.get(getProtoSchemaClassName(sinkConfig));
        this.partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(protobufConverterOrchestrator, maxComputeSinkConfig, descriptor);
        this.maxComputeSchemaCache = MaxComputeSchemaCacheFactory.createMaxComputeSchemaCache(protobufConverterOrchestrator,
                maxComputeSinkConfig, partitioningStrategy, sinkConfig, maxComputeClient, metadataUtil);
        this.messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter, maxComputeSchemaCache);
        maxComputeSchemaCache.setMessageParser(messageParser);
        maxComputeSchemaCache.updateSchema();
    }

    public Sink create() {
        RecordDecorator recordDecorator = RecordDecoratorFactory.createRecordDecorator(
                new RecordDecoratorFactory.RecordDecoratorConfig(protobufConverterOrchestrator, maxComputeSchemaCache, messageParser,
                        partitioningStrategy, maxComputeSinkConfig, sinkConfig, statsDReporter, maxComputeMetrics, metadataUtil)
        );
        ProtoMessageRecordConverter protoMessageRecordConverter = new ProtoMessageRecordConverter(recordDecorator, maxComputeSchemaCache);
        return new MaxComputeSink(maxComputeClient.createInsertManager(), protoMessageRecordConverter, statsDReporter, maxComputeMetrics);
    }

    private static String getProtoSchemaClassName(SinkConfig sinkConfig) {
        return SinkConnectorSchemaMessageMode.LOG_MESSAGE == sinkConfig.getSinkConnectorSchemaMessageMode()
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
    }

}
