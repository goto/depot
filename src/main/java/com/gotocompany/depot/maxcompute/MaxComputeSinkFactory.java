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

/**
 * Factory that assembles a fully wired MaxCompute {@link Sink}.
 *
 * <p>The factory follows a three-phase lifecycle:</p>
 * <ol>
 *     <li>construction, which materializes the {@link MaxComputeSinkConfig} and {@link SinkConfig} from the
 *     supplied environment map and eagerly creates the collaborators that do not depend on the protobuf
 *     schema (the {@link ProtobufConverterOrchestrator}, {@link MaxComputeMetrics}, {@link MaxComputeClient},
 *     and {@link MetadataUtil});</li>
 *     <li>{@link #init()}, which resolves the protobuf descriptor through the {@link StencilClient}, builds the
 *     {@link PartitioningStrategy} and {@link MaxComputeSchemaCache}, wires the {@link MessageParser}, and
 *     performs the initial table-schema creation or update;</li>
 *     <li>{@link #create()}, which builds the per-sink record decorator and converter and returns a ready
 *     {@link MaxComputeSink}.</li>
 * </ol>
 *
 * <p>{@link #init()} must be invoked exactly once before {@link #create()} is called.</p>
 *
 * @see MaxComputeSink
 * @see MaxComputeClient
 */
public class MaxComputeSinkFactory {

    /**
     * Sink configuration specific to MaxCompute, materialized from the environment map.
     */
    private final MaxComputeSinkConfig maxComputeSinkConfig;
    /**
     * Generic Depot sink configuration shared across all sink implementations.
     */
    private final SinkConfig sinkConfig;
    /**
     * Reporter used to build the metric instrumentation handed to the created collaborators.
     */
    private final StatsDReporter statsDReporter;
    /**
     * Stencil client used to resolve the protobuf descriptor that backs the configured schema class.
     */
    private final StencilClient stencilClient;
    /**
     * Orchestrates protobuf-to-MaxCompute type and value conversion; shared by the schema and record layers.
     */
    private final ProtobufConverterOrchestrator protobufConverterOrchestrator;
    /**
     * Holder of MaxCompute metric identifiers and tag templates.
     */
    private final MaxComputeMetrics maxComputeMetrics;
    /**
     * Client used to talk to MaxCompute for table DDL and to create {@code InsertManager} instances.
     */
    private final MaxComputeClient maxComputeClient;
    /**
     * Helper that contributes the metadata columns appended to each record.
     */
    private final MetadataUtil metadataUtil;

    /**
     * Cache of the current MaxCompute schema; populated lazily by {@link #init()}.
     */
    private MaxComputeSchemaCache maxComputeSchemaCache;
    /**
     * Strategy that derives the partition column and spec for each record; populated lazily by {@link #init()}.
     */
    private PartitioningStrategy partitioningStrategy;
    /**
     * Parser that turns raw payload bytes into protobuf messages; populated lazily by {@link #init()}.
     */
    private MessageParser messageParser;

    /**
     * Creates the factory and the schema-independent collaborators of the MaxCompute sink.
     *
     * <p>The {@link MaxComputeSinkConfig} and {@link SinkConfig} are materialized from {@code env} using
     * Owner's {@link ConfigFactory}. The constructor also eagerly creates the
     * {@link ProtobufConverterOrchestrator}, {@link MaxComputeMetrics}, {@link MaxComputeClient}, and
     * {@link MetadataUtil}; the schema-dependent collaborators are deferred to {@link #init()}.</p>
     *
     * @param statsDReporter reporter used to emit metrics from the sink and its collaborators
     * @param stencilClient  client used to resolve the protobuf descriptor during {@link #init()}
     * @param env            environment key-value pairs used to materialize the sink configuration
     */
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

    /**
     * Performs the schema-dependent initialization that must run before {@link #create()}.
     *
     * <p>The method resolves the protobuf {@link Descriptors.Descriptor} for the configured schema class via
     * the {@link StencilClient}, and then, in order:</p>
     * <ul>
     *     <li>builds the {@link PartitioningStrategy} for that descriptor;</li>
     *     <li>builds the {@link MaxComputeSchemaCache} from the orchestrator, partitioning strategy,
     *     {@link MaxComputeClient}, and {@link MetadataUtil};</li>
     *     <li>creates the {@link MessageParser} and injects it back into the schema cache;</li>
     *     <li>triggers an initial schema update so the target table is created or altered to match the
     *     resolved schema.</li>
     * </ul>
     */
    public void init() {
        Descriptors.Descriptor descriptor = stencilClient.get(getProtoSchemaClassName(sinkConfig));
        this.partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(protobufConverterOrchestrator, maxComputeSinkConfig, descriptor);
        this.maxComputeSchemaCache = MaxComputeSchemaCacheFactory.createMaxComputeSchemaCache(protobufConverterOrchestrator,
                maxComputeSinkConfig, partitioningStrategy, sinkConfig, maxComputeClient, metadataUtil);
        this.messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter, maxComputeSchemaCache);
        maxComputeSchemaCache.setMessageParser(messageParser);
        maxComputeSchemaCache.updateSchema();
    }

    /**
     * Builds a ready-to-use MaxCompute {@link Sink}.
     *
     * <p>The method constructs the {@link RecordDecorator} chain through {@link RecordDecoratorFactory}, wraps
     * it in a {@link ProtoMessageRecordConverter}, and returns a {@link MaxComputeSink} bound to a freshly
     * created {@code InsertManager} obtained from the {@link MaxComputeClient}.</p>
     *
     * <p>{@link #init()} must have completed first so that the schema cache, partitioning strategy, and message
     * parser are available.</p>
     *
     * @return a configured {@link Sink} that writes converted records to the MaxCompute table
     */
    public Sink create() {
        RecordDecorator recordDecorator = RecordDecoratorFactory.createRecordDecorator(
                new RecordDecoratorFactory.RecordDecoratorConfig(protobufConverterOrchestrator, maxComputeSchemaCache, messageParser,
                        partitioningStrategy, maxComputeSinkConfig, sinkConfig, statsDReporter, maxComputeMetrics, metadataUtil)
        );
        ProtoMessageRecordConverter protoMessageRecordConverter = new ProtoMessageRecordConverter(recordDecorator, maxComputeSchemaCache);
        return new MaxComputeSink(maxComputeClient.createInsertManager(), protoMessageRecordConverter, statsDReporter, maxComputeMetrics);
    }

    /**
     * Resolves the fully qualified protobuf class name that describes the records to be written.
     *
     * <p>When the connector runs in {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE} mode the configured
     * message proto class is used; otherwise the configured key proto class is used.</p>
     *
     * @param sinkConfig the sink configuration carrying the schema mode and the proto class names
     * @return the proto class name to resolve through the {@link StencilClient}
     */
    private static String getProtoSchemaClassName(SinkConfig sinkConfig) {
        return SinkConnectorSchemaMessageMode.LOG_MESSAGE == sinkConfig.getSinkConnectorSchemaMessageMode()
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
    }

}
