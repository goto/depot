package com.gotocompany.depot.maxcompute.record;

import com.aliyun.odps.PartitionSpec;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.schema.partition.DefaultPartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.TimestampPartitioningStrategy;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class ProtoDataColumnRecordDecorator extends RecordDecorator {

    private final ProtobufConverterOrchestrator protobufConverterOrchestrator;
    private final MessageParser protoMessageParser;
    private final PartitioningStrategy partitioningStrategy;
    private final SinkConfig sinkConfig;

    public ProtoDataColumnRecordDecorator(RecordDecorator decorator,
                                          ProtobufConverterOrchestrator protobufConverterOrchestrator,
                                          MessageParser messageParser,
                                          SinkConfig sinkConfig,
                                          PartitioningStrategy partitioningStrategy) {
        super(decorator);
        this.protobufConverterOrchestrator = protobufConverterOrchestrator;
        this.protoMessageParser = messageParser;
        this.partitioningStrategy = partitioningStrategy;
        this.sinkConfig = sinkConfig;
    }

    @Override
    public RecordWrapper process(RecordWrapper recordWrapper, Message message) throws IOException {
        String schemaClass = getSchemaClass();
        ParsedMessage parsedMessage = protoMessageParser.parse(message, sinkConfig.getSinkConnectorSchemaMessageMode(), schemaClass);
        parsedMessage.validate(sinkConfig);
        com.google.protobuf.Message protoMessage = (com.google.protobuf.Message) parsedMessage.getRaw();
        Map<Descriptors.FieldDescriptor, Object> fields = protoMessage.getAllFields();
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : fields.entrySet()) {
            if (entry.getKey().getName().equals(getPartitionFieldName()) && shouldReplaceOriginalColumn()) {
                continue;
            }
            recordWrapper.getRecord()
                    .set(entry.getKey().getName(), protobufConverterOrchestrator.toMaxComputeValue(entry.getKey(), entry.getValue()));
        }
        PartitionSpec partitionSpec = null;
        if (partitioningStrategy != null && partitioningStrategy instanceof DefaultPartitioningStrategy) {
            Descriptors.FieldDescriptor partitionFieldDescriptor = protoMessage.getDescriptorForType().findFieldByName(partitioningStrategy.getOriginalPartitionColumnName());
            Object object = protoMessage.hasField(partitionFieldDescriptor) ? protoMessage.getField(protoMessage.getDescriptorForType().findFieldByName(partitioningStrategy.getOriginalPartitionColumnName())) : null;
            partitionSpec = partitioningStrategy.getPartitionSpec(object);
        }
        if (partitioningStrategy != null && partitioningStrategy instanceof TimestampPartitioningStrategy) {
            partitionSpec = partitioningStrategy.getPartitionSpec(recordWrapper.getRecord());
        }
        return new RecordWrapper(recordWrapper.getRecord(), recordWrapper.getIndex(), recordWrapper.getErrorInfo(), partitionSpec);
    }

    private String getPartitionFieldName() {
        return Optional.ofNullable(partitioningStrategy)
                .map(PartitioningStrategy::getOriginalPartitionColumnName)
                .orElse(null);
    }

    private boolean shouldReplaceOriginalColumn() {
        return Optional.ofNullable(partitioningStrategy)
                .map(PartitioningStrategy::shouldReplaceOriginalColumn)
                .orElse(false);
    }

    private String getSchemaClass() {
        return sinkConfig.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
    }

}
