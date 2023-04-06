package com.gotocompany.depot.bigquery.storage.proto;

import com.google.api.client.util.Preconditions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.converter.fields.DurationProtoField;
import com.gotocompany.depot.message.proto.converter.fields.MessageProtoField;
import com.gotocompany.depot.message.proto.converter.fields.ProtoField;
import com.gotocompany.depot.message.proto.converter.fields.ProtoFieldFactory;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@AllArgsConstructor
public class BigQueryProtoPayloadConverter {

    private BigQuerySinkConfig config;
    private MessageParser parser;
    private BigQueryProtoWriter writer;

    public BigQueryPayload convert(List<Message> messages) {
        ProtoRows.Builder rowBuilder = ProtoRows.newBuilder();
        BigQueryProtoPayload payload = new BigQueryProtoPayload();
        for (int index = 0; index < messages.size(); index++) {
            Message message = messages.get(index);
            try {
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(message.getMetadata(), index, null, true);
                DynamicMessage convertedMessage = convert(message);
                payload.addMetadataRecord(metadata);
                rowBuilder.addSerializedRows(convertedMessage.toByteString());
            } catch (Exception e) {
                e.printStackTrace();
                ErrorInfo info = new ErrorInfo(e, ErrorType.DEFAULT_ERROR);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(message.getMetadata(), index, info, false);
                payload.addMetadataRecord(metadata);
            }
        }
        payload.setPayload(rowBuilder.build());
        return payload;
    }


    private DynamicMessage convert(Message message) throws IOException {
        SinkConnectorSchemaMessageMode mode = this.config.getSinkConnectorSchemaMessageMode();
        String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? this.config.getSinkConnectorSchemaProtoMessageClass() : this.config.getSinkConnectorSchemaProtoKeyClass();
        ParsedMessage parsedMessage = parser.parse(message, mode, schemaClass);
        parsedMessage.validate(this.config);
        DynamicMessage.Builder messageBuilder = convert((DynamicMessage) parsedMessage.getRaw(), writer.getDescriptor());
        BigQueryProtoUtils.addMetadata(message.getMetadata(), messageBuilder, writer.getDescriptor(), config);
        return messageBuilder.build();
    }

    private DynamicMessage.Builder convert(DynamicMessage inputMessage, Descriptors.Descriptor descriptor) {
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);
        List<Descriptors.FieldDescriptor> allFields = inputMessage.getDescriptorForType().getFields();
        for (Descriptors.FieldDescriptor inputField : allFields) {
            Descriptors.FieldDescriptor outputField = descriptor.findFieldByName(inputField.getName());
            if (outputField == null) {
                // not found in table
                continue;
            }
            ProtoField protoField = ProtoFieldFactory.getField(inputField, inputMessage.getField(inputField));
            Object fieldValue = protoField.getValue();
            if (fieldValue.toString().isEmpty()) {
                continue;
            }
            if (fieldValue instanceof List) {
                addRepeatedFields(messageBuilder, outputField, (List<?>) fieldValue);
                continue;
            }
            if (fieldValue instanceof Instant) {
                long timeStampValue = getBQInstant((Instant) fieldValue);
                if (timeStampValue > 0) {
                    messageBuilder.setField(outputField, timeStampValue);
                }
            } else if (protoField.getClass().getName().equals(MessageProtoField.class.getName())
                    || protoField.getClass().getName().equals(DurationProtoField.class.getName())) {
                Descriptors.Descriptor messageType = outputField.getMessageType();
                messageBuilder.setField(outputField, convert((DynamicMessage) fieldValue, messageType).build());
            } else {
                floatCheck(fieldValue);
                messageBuilder.setField(outputField, fieldValue);
            }
        }
        return messageBuilder;
    }

    private long getBQInstant(Instant instant) {
        // Timestamp should be in microseconds
        return TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
    }

    private void floatCheck(Object fieldValue) {
        if (fieldValue instanceof Float) {
            float floatValue = ((Number) fieldValue).floatValue();
            Preconditions.checkArgument(!Float.isInfinite(floatValue) && !Float.isNaN(floatValue));
        } else if (fieldValue instanceof Double) {
            double doubleValue = ((Number) fieldValue).doubleValue();
            Preconditions.checkArgument(!Double.isInfinite(doubleValue) && !Double.isNaN(doubleValue));
        }
    }

    private void addRepeatedFields(DynamicMessage.Builder messageBuilder, Descriptors.FieldDescriptor outputField, List<?> fieldValue) {
        if (fieldValue.isEmpty()) {
            return;
        }
        List<Object> repeatedNestedFields = new ArrayList<>();
        for (Object f : fieldValue) {
            if (f instanceof DynamicMessage) {
                Descriptors.Descriptor messageType = outputField.getMessageType();
                repeatedNestedFields.add(convert((DynamicMessage) f, messageType).build());
            } else {
                if (f instanceof Instant) {
                    long timeStampValue = getBQInstant((Instant) f);
                    if (timeStampValue > 0) {
                        messageBuilder.setField(outputField, timeStampValue);
                    }
                } else {
                    floatCheck(f);
                    repeatedNestedFields.add(f);
                }
            }
        }
        messageBuilder.setField(outputField, repeatedNestedFields);
    }
}

