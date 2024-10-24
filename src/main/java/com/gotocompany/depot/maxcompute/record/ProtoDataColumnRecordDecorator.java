package com.gotocompany.depot.maxcompute.record;

import com.aliyun.odps.data.Record;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;

import java.io.IOException;

public class ProtoDataColumnRecordDecorator extends RecordDecorator {

    private final ConverterOrchestrator converterOrchestrator;
    private final MaxComputeSchemaCache maxComputeSchemaCache;
    private final ProtoMessageParser protoMessageParser;
    private final SinkConfig sinkConfig;

    public ProtoDataColumnRecordDecorator(RecordDecorator decorator,
                                          ConverterOrchestrator converterOrchestrator,
                                          MaxComputeSchemaCache maxComputeSchemaCache,
                                          ProtoMessageParser protoMessageParser,
                                          SinkConfig sinkConfig) {
        super(decorator);
        this.converterOrchestrator = converterOrchestrator;
        this.maxComputeSchemaCache = maxComputeSchemaCache;
        this.protoMessageParser = protoMessageParser;
        this.sinkConfig = sinkConfig;
    }

    @Override
    public void append(Record record, Message message) throws IOException {
        String schemaClass = getSchemaClass();
        ParsedMessage parsedMessage = protoMessageParser.parse(message, sinkConfig.getSinkConnectorSchemaMessageMode(), schemaClass);
        parsedMessage.validate(sinkConfig);
        com.google.protobuf.Message protoMessage = (com.google.protobuf.Message) parsedMessage.getRaw();
        for (Descriptors.FieldDescriptor fieldDescriptor : protoMessage.getDescriptorForType().getFields()) {
            record.set(fieldDescriptor.getName(), converterOrchestrator.convert(fieldDescriptor, protoMessage.getField(fieldDescriptor)));
        }
    }

    private String getSchemaClass() {
        return sinkConfig.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
    }

}
