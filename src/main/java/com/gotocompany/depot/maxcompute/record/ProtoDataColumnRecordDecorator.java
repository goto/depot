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
import java.util.Optional;

/**
 * Decorator to convert protobuf message to maxcompute record.
 * Populates the data column and partition column.
 */
public class ProtoDataColumnRecordDecorator extends RecordDecorator {

    private final ProtobufConverterOrchestrator protobufConverterOrchestrator;
    private final MessageParser protoMessageParser;
    private final PartitioningStrategy partitioningStrategy;
    private final SinkConnectorSchemaMessageMode sinkConnectorSchemaMessageMode;
    private final String partitionFieldName;
    private final boolean shouldReplaceOriginalColumn;
    private final String schemaClass;
    private final ProtoUnknownFieldValidationType protoUnknownFieldValidationType;
    private final Instrumentation instrumentation;
    private final MaxComputeMetrics maxComputeMetrics;
    private final boolean sinkConnectorSchemaProtoAllowUnknownFieldsEnable;
    private final boolean sinkConnectorSchemaProtoUnknownFieldsValidationInstrumentationEnable;

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
        if (partitionSpec != null && partitionSpec.get(partitioningStrategy.getOriginalPartitionColumnName()) == null) {
            instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeMissingPartitionRecrodsMetric(),
                    String.format(MaxComputeMetrics.MAXCOMPUTE_UNKNOWN_FIELD_VALIDATION_TYPE_TAG, "missing_partition"));
        }
        return partitionSpec;
    }

}
