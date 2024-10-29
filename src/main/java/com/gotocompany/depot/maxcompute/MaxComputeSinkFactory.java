package com.gotocompany.depot.maxcompute;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.converter.record.RecordConverter;
import com.gotocompany.depot.maxcompute.helper.MaxComputeSchemaHelper;
import com.gotocompany.depot.maxcompute.record.RecordDecorator;
import com.gotocompany.depot.maxcompute.record.RecordDecoratorFactory;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategyFactory;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.utils.StencilUtils;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class MaxComputeSinkFactory {

    private final PartitioningStrategyFactory partitioningStrategyFactory;
    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final SinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;
    private final ConverterOrchestrator converterOrchestrator;
    private MaxComputeClient maxComputeClient;
    private MaxComputeSchemaCache maxComputeSchemaCache;
    private PartitioningStrategy partitioningStrategy;

    public MaxComputeSinkFactory(StatsDReporter statsDReporter, Map<String, String> env) {
        this.statsDReporter = statsDReporter;
        this.maxComputeSinkConfig = ConfigFactory.create(MaxComputeSinkConfig.class, env);
        this.sinkConfig = ConfigFactory.create(SinkConfig.class, env);
        this.converterOrchestrator = new ConverterOrchestrator();
        this.partitioningStrategyFactory = new PartitioningStrategyFactory(converterOrchestrator, maxComputeSinkConfig);
    }

    public void init() {
        String schemaClass = SinkConnectorSchemaMessageMode.LOG_MESSAGE == sinkConfig.getSinkConnectorSchemaMessageMode() ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
        StencilClient stencilClient = getStencilClient(statsDReporter);
        Descriptors.Descriptor descriptor = stencilClient.get(schemaClass);
        this.partitioningStrategy = partitioningStrategyFactory.createPartitioningStrategy(descriptor);
        this.maxComputeClient = new MaxComputeClient(maxComputeSinkConfig);
        MaxComputeSchemaHelper maxComputeSchemaHelper = new MaxComputeSchemaHelper(converterOrchestrator, maxComputeSinkConfig, partitioningStrategy);
        this.maxComputeSchemaCache = new MaxComputeSchemaCache(maxComputeSchemaHelper, sinkConfig, converterOrchestrator, maxComputeClient);
        MessageParser messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter, maxComputeSchemaCache);
        this.maxComputeSchemaCache.setMessageParser(messageParser);
        this.maxComputeSchemaCache.updateSchema();
    }

    public Sink create() {
        RecordConverter recordConverter = new RecordConverter(buildRecordDecorator(), maxComputeSchemaCache);
        return new MaxComputeSink(maxComputeClient, recordConverter);
    }

    private RecordDecorator buildRecordDecorator() {
        MessageParser messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter, maxComputeSchemaCache);
        return RecordDecoratorFactory.createRecordDecorator(
                converterOrchestrator,
                maxComputeSchemaCache,
                messageParser,
                partitioningStrategy,
                maxComputeSinkConfig,
                sinkConfig
        );
    }

    private StencilClient getStencilClient(StatsDReporter reporter) {
        StencilConfig stencilConfig = StencilUtils.getStencilConfig(sinkConfig, reporter.getClient(), null);
        if (sinkConfig.isSchemaRegistryStencilEnable()) {
            return StencilClientFactory.getClient(sinkConfig.getSchemaRegistryStencilUrls(), stencilConfig);
        } else {
            return StencilClientFactory.getClient();
        }
    }

}
