package com.gotocompany.depot.bigquery.storage.proto;

import com.google.api.client.util.Preconditions;
import com.google.api.core.ApiFutureCallback;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageClient;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.exception.UnknownFieldsException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.converter.fields.DurationProtoField;
import com.gotocompany.depot.message.proto.converter.fields.MessageProtoField;
import com.gotocompany.depot.message.proto.converter.fields.ProtoField;
import com.gotocompany.depot.message.proto.converter.fields.ProtoFieldFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BigQueryProtoStorageClient implements BigQueryStorageClient {

    private final BigQueryProtoWriter writer;
    private final BigQuerySinkConfig config;
    private final MessageParser parser;
    private final String schemaClass;
    private final SinkConnectorSchemaMessageMode mode;

    public BigQueryProtoStorageClient(BigQueryProtoWriter writer, BigQuerySinkConfig config, MessageParser parser) {
        this.writer = writer;
        this.config = config;
        this.parser = parser;
        this.mode = config.getSinkConnectorSchemaMessageMode();
        this.schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? config.getSinkConnectorSchemaProtoMessageClass() : config.getSinkConnectorSchemaProtoKeyClass();
    }


    public BigQueryPayload convert(List<Message> messages) {
        ProtoRows.Builder rowBuilder = ProtoRows.newBuilder();
        BigQueryPayload payload = new BigQueryPayload();
        Descriptors.Descriptor descriptor = writer.getDescriptor();
        for (int index = 0; index < messages.size(); index++) {
            Message message = messages.get(index);
            try {
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(message.getMetadata(), index, null, true);
                DynamicMessage convertedMessage = convert(message, descriptor);
                payload.addMetadataRecord(metadata);
                rowBuilder.addSerializedRows(convertedMessage.toByteString());
            } catch (UnknownFieldsException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(message.getMetadata(), index, errorInfo, false);
                payload.addMetadataRecord(metadata);
            } catch (EmptyMessageException | UnsupportedOperationException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(message.getMetadata(), index, errorInfo, false);
                payload.addMetadataRecord(metadata);
            } catch (DeserializerException | IllegalArgumentException | IOException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(message.getMetadata(), index, errorInfo, false);
                payload.addMetadataRecord(metadata);
            }
        }
        payload.setPayload(rowBuilder.build());
        return payload;
    }

    @Override
    public AppendRowsResponse appendAndGet(BigQueryPayload payload, ApiFutureCallback<AppendRowsResponse> callback) throws Exception {
        return writer.appendAndGet(payload, callback);
    }


    private DynamicMessage convert(Message message, Descriptors.Descriptor descriptor) throws IOException {
        ParsedMessage parsedMessage = parser.parse(message, mode, schemaClass);
        parsedMessage.validate(config);
        DynamicMessage.Builder messageBuilder = convert((DynamicMessage) parsedMessage.getRaw(), descriptor);
        BigQueryProtoUtils.addMetadata(message.getMetadata(), messageBuilder, descriptor, config);
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

