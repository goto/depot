package com.gotocompany.depot.kafka.schema;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunction;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunctionCache;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunctionFactory;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.KafkaSinkMetrics;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import com.gotocompany.depot.utils.MessageConfigUtils;
import com.gotocompany.stencil.client.StencilClient;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Stencil update listener that builds and refreshes the Kafka sink proto mapping function.
 *
 * <p>The listener resolves the source descriptor from the message parser and the sink key and value
 * descriptors from the sink Stencil client, then compiles the proto mapping into a cached mapping function.
 * A refresh that fails to rebuild the mapping keeps the previously built mapping so that processing
 * continues, while the initial build fails fast.
 */
public class KafkaSinkStencilUpdateListener extends DepotStencilUpdateListener {

    private static final String SCHEMA_UPDATE_SUCCESS = "success";
    private static final String SCHEMA_UPDATE_FAILURE = "failure";

    private final KafkaSinkConfig sinkConfig;
    private final ProtoMappingFunctionFactory mappingFunctionFactory;
    private final ProtoMappingFunctionCache mappingFunctionCache;
    private final KafkaSinkMetrics metrics;
    private final Instrumentation instrumentation;
    private final AtomicBoolean updating = new AtomicBoolean(false);
    @Setter
    private StencilClient sinkStencilClient;

    /**
     * Creates a schema update listener for the Kafka sink.
     *
     * @param sinkConfig             the Kafka sink configuration
     * @param mappingFunctionFactory the factory that compiles the proto mapping into a mapping function
     * @param mappingFunctionCache   the cache that holds the active mapping function
     * @param metrics                the Kafka sink metric names
     * @param instrumentation        the instrumentation used for logging and metric capture
     */
    public KafkaSinkStencilUpdateListener(KafkaSinkConfig sinkConfig,
                                          ProtoMappingFunctionFactory mappingFunctionFactory,
                                          ProtoMappingFunctionCache mappingFunctionCache,
                                          KafkaSinkMetrics metrics,
                                          Instrumentation instrumentation) {
        this.sinkConfig = sinkConfig;
        this.mappingFunctionFactory = mappingFunctionFactory;
        this.mappingFunctionCache = mappingFunctionCache;
        this.metrics = metrics;
        this.instrumentation = instrumentation;
    }

    /**
     * Rebuilds the mapping function in response to a Stencil schema refresh.
     *
     * <p>The latest descriptors are re-read from the message parser and the sink Stencil client. If the
     * rebuild fails, the failure is logged, the schema update failure metric is captured and the previously
     * built mapping function is retained so that message processing is not interrupted.
     *
     * @param newDescriptors the refreshed descriptors supplied by Stencil
     */
    @Override
    public void onSchemaUpdate(Map<String, Descriptors.Descriptor> newDescriptors) {
        try {
            updateSchema();
        } catch (RuntimeException e) {
            instrumentation.logError(
                    "Failed to rebuild kafka sink proto mapping function on schema refresh, retaining the previous mapping: {}",
                    e.getMessage());
            instrumentation.incrementCounter(metrics.getKafkaSchemaUpdateTotalMetric(),
                    String.format(KafkaSinkMetrics.KAFKA_SCHEMA_UPDATE_STATE_TAG, SCHEMA_UPDATE_FAILURE));
        }
    }

    /**
     * Builds the proto mapping function from the latest source and sink descriptors and caches it.
     *
     * <p>The build is skipped until both the message parser and the sink Stencil client are set, and a
     * concurrent rebuild already in progress is skipped. A successful build captures the schema update
     * success metric.
     *
     * @throws ConfigurationException if the message parser is not a protobuf parser or a descriptor or mapping is invalid
     */
    @Override
    public void updateSchema() {
        MessageParser messageParser = getMessageParser();
        if (messageParser == null || sinkStencilClient == null) {
            return;
        }
        if (!(messageParser instanceof ProtoMessageParser)) {
            throw new ConfigurationException("kafka sink requires a protobuf message parser");
        }
        if (!updating.compareAndSet(false, true)) {
            return;
        }
        try {
            mappingFunctionCache.set(buildMappingFunction((ProtoMessageParser) messageParser));
            instrumentation.logInfo("kafka sink proto mapping function updated successfully");
            instrumentation.incrementCounter(metrics.getKafkaSchemaUpdateTotalMetric(),
                    String.format(KafkaSinkMetrics.KAFKA_SCHEMA_UPDATE_STATE_TAG, SCHEMA_UPDATE_SUCCESS));
        } finally {
            updating.set(false);
        }
    }

    /**
     * Builds a mapping function from the source, value and optional key descriptors.
     *
     * @param protoMessageParser the source proto message parser providing the source descriptor
     * @return the compiled mapping function
     * @throws ConfigurationException if a required descriptor cannot be resolved or the mapping is invalid
     */
    private ProtoMappingFunction buildMappingFunction(ProtoMessageParser protoMessageParser) {
        String sourceSchemaClass = MessageConfigUtils.getModeAndSchema(sinkConfig).getSecond();
        Descriptors.Descriptor sourceDescriptor = getDescriptor(protoMessageParser, sourceSchemaClass);
        Descriptors.Descriptor valueDescriptor = getDescriptor(sinkStencilClient, sinkConfig.getSinkKafkaProtoMessage());
        Descriptors.Descriptor keyDescriptor = sinkConfig.getSinkKafkaProtoKey() == null ? null
                : getDescriptor(sinkStencilClient, sinkConfig.getSinkKafkaProtoKey());
        return mappingFunctionFactory.create(sourceDescriptor, valueDescriptor, keyDescriptor, sinkConfig.getSinkKafkaProtoMapping());
    }

    /**
     * Resolves a source descriptor from the proto message parser.
     *
     * @param protoMessageParser the source proto message parser
     * @param schemaClass        the fully qualified source proto class name
     * @return the resolved descriptor
     * @throws ConfigurationException if the descriptor cannot be found
     */
    private Descriptors.Descriptor getDescriptor(ProtoMessageParser protoMessageParser, String schemaClass) {
        Descriptors.Descriptor descriptor = protoMessageParser.getDescriptor(schemaClass);
        if (descriptor == null) {
            throw new ConfigurationException(String.format("descriptor not found for the source proto class %s", schemaClass));
        }
        return descriptor;
    }

    /**
     * Resolves a sink descriptor from the sink Stencil client.
     *
     * @param stencilClient the sink Stencil client
     * @param schemaClass   the fully qualified sink proto class name
     * @return the resolved descriptor
     * @throws ConfigurationException if the descriptor cannot be found
     */
    private Descriptors.Descriptor getDescriptor(StencilClient stencilClient, String schemaClass) {
        Descriptors.Descriptor descriptor = stencilClient.get(schemaClass);
        if (descriptor == null) {
            throw new ConfigurationException(String.format("descriptor not found for the sink proto class %s", schemaClass));
        }
        return descriptor;
    }
}
