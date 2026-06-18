package com.gotocompany.depot.maxcompute.record;

import com.aliyun.odps.PartitionSpec;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.schema.partition.DefaultPartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.TimestampPartitioningStrategy;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.ProtoUnknownFieldValidationType;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.utils.ProtoUtils;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Decorator to convert protobuf message to maxcompute record.
 * Populates the data column and partition column.
 *
 * <p>For each message this decorator parses the configured Protobuf class, optionally validates it
 * for unknown fields, and copies every populated top-level field into the record using the
 * {@link ProtobufConverterOrchestrator}. The field that backs a replacing partition column (when the
 * partitioning strategy replaces the original column) is skipped, and empty or unset message/string
 * fields are left out. Finally it resolves the record's {@link PartitionSpec} from the configured
 * {@link PartitioningStrategy}.</p>
 *
 * <p>As the terminal link in the decorator chain it is typically created first and then optionally
 * wrapped by a {@link ProtoMetadataColumnRecordDecorator}. Unknown-field validation latency and
 * missing-partition occurrences are instrumented through {@link MaxComputeMetrics}.</p>
 *
 * @see RecordDecorator
 * @see RecordDecoratorFactory
 */
public class ProtoDataColumnRecordDecorator extends RecordDecorator {

    /**
     * Converts Protobuf field values into the corresponding MaxCompute column values.
     */
    private final ProtobufConverterOrchestrator protobufConverterOrchestrator;
    /**
     * Parser used to decode the raw Depot message into a Protobuf message before conversion.
     */
    private final MessageParser protoMessageParser;
    /**
     * Strategy that supplies the partition column and resolves per-record partition specifications,
     * or {@code null} when partitioning is disabled.
     */
    private final PartitioningStrategy partitioningStrategy;
    /**
     * Indicates whether the key or the log message carries the schema, controlling how the message is
     * parsed.
     */
    private final SinkConnectorSchemaMessageMode sinkConnectorSchemaMessageMode;
    /**
     * Name of the original Protobuf field used as the partition key, or {@code null} when there is no
     * partitioning strategy.
     */
    private final String partitionFieldName;
    /**
     * Whether the partition column replaces the original field, in which case that field is not also
     * written as a data column.
     */
    private final boolean shouldReplaceOriginalColumn;
    /**
     * Fully-qualified Protobuf class name used to parse the message, selected from configuration based
     * on the schema message mode.
     */
    private final String schemaClass;
    /**
     * Strategy describing how unknown Protobuf fields are detected during validation.
     */
    private final ProtoUnknownFieldValidationType protoUnknownFieldValidationType;
    /**
     * Instrumentation handle used to emit latency and counter metrics for this decorator.
     */
    private final Instrumentation instrumentation;
    /**
     * Holder of MaxCompute metric names and tags referenced when recording metrics.
     */
    private final MaxComputeMetrics maxComputeMetrics;
    /**
     * When {@code true}, unknown-field validation is skipped and unknown fields are tolerated.
     */
    private final boolean sinkConnectorSchemaProtoAllowUnknownFieldsEnable;
    /**
     * When {@code true}, the duration of unknown-field validation is measured and reported.
     */
    private final boolean sinkConnectorSchemaProtoUnknownFieldsValidationInstrumentationEnable;

    /**
     * Creates a data-column decorator wired with the converters, parser, and configuration it needs
     * to populate records.
     *
     * <p>The constructor derives several cached values from the configuration: the schema message
     * mode, the original partition field name and whether it should be replaced (both obtained from
     * the partitioning strategy when present), the Protobuf schema class, the unknown-field
     * validation type, and the unknown-field handling flags. It also creates the {@link Instrumentation}
     * used for metrics.</p>
     *
     * @param decorator the next decorator in the chain, or {@code null} if this decorator terminates
     *        the chain
     * @param protobufConverterOrchestrator orchestrator used to convert Protobuf field values into
     *        MaxCompute column values
     * @param messageParser parser used to decode the raw message into a Protobuf message
     * @param sinkConfig sink configuration providing the schema message mode, Protobuf schema classes,
     *        and unknown-field handling flags
     * @param partitioningStrategy strategy used to derive the partition specification, or {@code null}
     *        when the table is not partitioned
     * @param statsDReporter reporter backing the {@link Instrumentation} used for metrics
     * @param maxComputeMetrics holder of MaxCompute metric names and tags
     */
    public ProtoDataColumnRecordDecorator(RecordDecorator decorator,
                                          ProtobufConverterOrchestrator protobufConverterOrchestrator,
                                          MessageParser messageParser,
                                          SinkConfig sinkConfig,
                                          PartitioningStrategy partitioningStrategy,
                                          StatsDReporter statsDReporter,
                                          MaxComputeMetrics maxComputeMetrics) {
        super(decorator);
        this.protobufConverterOrchestrator = protobufConverterOrchestrator;
        this.protoMessageParser = messageParser;
        this.partitioningStrategy = partitioningStrategy;
        this.sinkConnectorSchemaMessageMode = sinkConfig.getSinkConnectorSchemaMessageMode();
        this.partitionFieldName = Optional.ofNullable(partitioningStrategy)
                .map(PartitioningStrategy::getOriginalPartitionColumnName)
                .orElse(null);
        this.shouldReplaceOriginalColumn = Optional.ofNullable(partitioningStrategy)
                .map(PartitioningStrategy::shouldReplaceOriginalColumn)
                .orElse(false);
        this.schemaClass = sinkConfig.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
        this.protoUnknownFieldValidationType = sinkConfig.getSinkConnectorSchemaProtoUnknownFieldsValidation();
        this.instrumentation = new Instrumentation(statsDReporter, this.getClass());
        this.maxComputeMetrics = maxComputeMetrics;
        this.sinkConnectorSchemaProtoAllowUnknownFieldsEnable = sinkConfig.getSinkConnectorSchemaProtoAllowUnknownFieldsEnable();
        this.sinkConnectorSchemaProtoUnknownFieldsValidationInstrumentationEnable = sinkConfig.getSinkConnectorSchemaProtoUnknownFieldsValidationInstrumentationEnable();
    }

    /**
     * Converts protobuf message to maxcompute record, populating the data column and partition column.
     *
     * <p>The message is first parsed into a Protobuf message using the configured schema class and
     * message mode. Unless unknown fields are explicitly allowed, the parsed message is validated for
     * unknown fields; when validation instrumentation is enabled the validation latency is recorded.
     * Each populated top-level field is then written to the record, except: the original partition
     * field when the partitioning strategy replaces it, fields whose string representation is empty,
     * and non-repeated message or string fields that are not actually set. Field values are converted
     * via the {@link ProtobufConverterOrchestrator}. Finally the partition specification is resolved
     * from the message and record.</p>
     *
     * @param recordWrapper record template to be populated
     * @param message protobuf raw message
     * @return populated record
     * @throws IOException if an error occurs while processing the message
     */
    @Override
    public RecordWrapper process(RecordWrapper recordWrapper, Message message) throws IOException {
        ParsedMessage parsedMessage = protoMessageParser.parse(message, sinkConnectorSchemaMessageMode, schemaClass);
        if (!sinkConnectorSchemaProtoAllowUnknownFieldsEnable) {
            Instant unknownFieldValidationStart = Instant.now();
            parsedMessage.validate(protoUnknownFieldValidationType);
            if (sinkConnectorSchemaProtoUnknownFieldsValidationInstrumentationEnable) {
                instrumentation.captureDurationSince(
                        maxComputeMetrics.getMaxComputeUnknownFieldValidationLatencyMetric(),
                        unknownFieldValidationStart,
                        String.format(MaxComputeMetrics.MAXCOMPUTE_UNKNOWN_FIELD_VALIDATION_TYPE_TAG, protoUnknownFieldValidationType)
                );
            }
        }
        com.google.protobuf.Message protoMessage = (com.google.protobuf.Message) parsedMessage.getRaw();
        protoMessage.getDescriptorForType().getFields()
                .forEach(fieldDescriptor -> {
                    if (fieldDescriptor.getName().equals(partitionFieldName) && shouldReplaceOriginalColumn) {
                        return;
                    }
                    if (protoMessage.getField(fieldDescriptor).toString().isEmpty()) {
                        return;
                    }
                    if (ProtoUtils.isNonRepeatedProtoMessage(fieldDescriptor) && !protoMessage.hasField(fieldDescriptor)) {
                        return;
                    }
                    if (ProtoUtils.isNonRepeatedString(fieldDescriptor) && !protoMessage.hasField(fieldDescriptor)) {
                        return;
                    }
                    recordWrapper.getRecord()
                            .set(fieldDescriptor.getName(), protobufConverterOrchestrator.toMaxComputeValue(new ProtoPayload(fieldDescriptor, protoMessage.getField(fieldDescriptor), 0)));
                });
        PartitionSpec partitionSpec = getPartitionSpec(recordWrapper, protoMessage);
        return new RecordWrapper(recordWrapper.getRecord(), recordWrapper.getIndex(), recordWrapper.getErrorInfo(), partitionSpec);
    }

    /**
     * Resolves the {@link PartitionSpec} for the record according to the active partitioning strategy.
     *
     * <p>For a {@link DefaultPartitioningStrategy} the spec is derived from the value of the original
     * partition field in the Protobuf message (or {@code null} when the field is unset). For a
     * {@link TimestampPartitioningStrategy} the spec is generated from the already-populated record.
     * If the resolved partition value is absent or equal to the default {@code __NULL__} placeholder,
     * a missing-partition counter is incremented through {@link MaxComputeMetrics}.</p>
     *
     * @param recordWrapper the wrapper whose record is used by timestamp-based partitioning
     * @param protoMessage the parsed Protobuf message used by value-based partitioning
     * @return the resolved partition specification, or {@code null} when no partitioning strategy is
     *         configured or none applies
     */
    private @Nullable PartitionSpec getPartitionSpec(RecordWrapper recordWrapper, com.google.protobuf.Message protoMessage) {
        PartitionSpec partitionSpec = null;
        if (partitioningStrategy != null && partitioningStrategy instanceof DefaultPartitioningStrategy) {
            Descriptors.FieldDescriptor partitionFieldDescriptor = protoMessage.getDescriptorForType().findFieldByName(partitioningStrategy.getOriginalPartitionColumnName());
            Object object = protoMessage.hasField(partitionFieldDescriptor) ? protoMessage.getField(protoMessage.getDescriptorForType().findFieldByName(partitioningStrategy.getOriginalPartitionColumnName())) : null;
            partitionSpec = partitioningStrategy.getPartitionSpec(object);
        }
        if (partitioningStrategy != null && partitioningStrategy instanceof TimestampPartitioningStrategy) {
            partitionSpec = partitioningStrategy.getPartitionSpec(recordWrapper.getRecord());
        }
        if (partitionSpec != null && (partitionSpec.get(partitioningStrategy.getOriginalPartitionColumnName()) == null || Objects.equals(partitionSpec.get(partitioningStrategy.getOriginalPartitionColumnName()), "__NULL__"))) {
            instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeMissingPartitionRecrodsMetric(),
                    String.format(MaxComputeMetrics.MAXCOMPUTE_UNKNOWN_FIELD_VALIDATION_TYPE_TAG, "missing_partition"));
        }
        return partitionSpec;
    }

}
