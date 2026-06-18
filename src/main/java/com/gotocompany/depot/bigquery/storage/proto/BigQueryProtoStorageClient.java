package com.gotocompany.depot.bigquery.storage.proto;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageClient;
import com.gotocompany.depot.bigquery.storage.BigQueryWriter;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.exception.UnknownFieldsException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.ProtoUnknownFieldValidationType;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.converter.fields.DurationProtoField;
import com.gotocompany.depot.message.proto.converter.fields.MessageProtoField;
import com.gotocompany.depot.message.proto.converter.fields.ProtoField;
import com.gotocompany.depot.message.proto.converter.fields.ProtoFieldFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * {@link BigQueryStorageClient} implementation that converts Protobuf messages into
 * Storage Write API rows and streams them to BigQuery.
 *
 * <p>This client bridges Depot's message model and the BigQuery Storage Write API for the Protobuf
 * data type. On {@link #convert(List)} it parses each incoming {@link Message}, recursively maps its
 * fields onto the destination table's Protobuf descriptor (handling nested messages, repeated fields,
 * timestamps and durations), and accumulates the serialized rows into a {@link BigQueryPayload};
 * records that cannot be converted are captured as invalid with classified
 * {@link com.gotocompany.depot.error.ErrorInfo} rather than failing the whole batch. On
 * {@link #appendAndGet(BigQueryPayload)} it delegates to the underlying {@link BigQueryProtoWriter}.</p>
 *
 * <p>A background scheduled task periodically refreshes the parser's cached schema so that descriptor
 * changes are picked up without restarting the sink. The executor is shut down by {@link #close()}.</p>
 *
 * @see BigQueryProtoWriter
 * @see BigQueryProtoUtils
 * @see TimeStampUtils
 */
public class BigQueryProtoStorageClient implements BigQueryStorageClient {

    /** Initial delay, in seconds, before the first scheduled parser schema refresh. */
    private static final long MESSAGE_PARSER_CHECKER_DELAY_SECONDS = 1;
    /** Interval, in seconds, between successive parser schema refreshes. */
    private static final long MESSAGE_PARSER_CHECKER_FREQUENCY_SECONDS = 60;
    /** The Protobuf writer this client delegates appends to. */
    private final BigQueryProtoWriter writer;
    /** Sink configuration providing schema classes, modes and validation toggles. */
    private final BigQuerySinkConfig config;
    /** Parser used to deserialize incoming messages into {@link ParsedMessage} instances. */
    private final MessageParser parser;
    /** Fully-qualified Protobuf class name of the schema to parse against. */
    private final String schemaClass;
    /** Whether key or message bytes are parsed, derived from the configured schema message mode. */
    private final SinkConnectorSchemaMessageMode mode;
    /** Single-threaded scheduler that periodically refreshes the parser's cached schema. */
    private final ScheduledExecutorService messageParserChecker = Executors.newScheduledThreadPool(1);
    /** Strategy controlling how unknown Protobuf fields are validated. */
    private final ProtoUnknownFieldValidationType protoUnknownFieldValidationType;
    /** Whether unknown Protobuf fields are permitted (when {@code true}, validation is skipped). */
    private final boolean sinkConnectorSchemaProtoAllowUnknownFieldsEnable;

    /**
     * Creates a Protobuf storage client and starts the background schema-refresh task.
     *
     * <p>Resolves the schema class to use from the configured message mode (the message class for
     * {@code LOG_MESSAGE}, otherwise the key class), captures the unknown-field validation settings and
     * schedules a recurring task that refreshes the parser's cached schema. The supplied writer must be
     * a {@link BigQueryProtoWriter}, to which it is downcast.</p>
     *
     * @param writer the writer to delegate appends to; must be a {@link BigQueryProtoWriter}
     * @param config the BigQuery sink configuration
     * @param parser the message parser used for deserialization and schema refresh
     */
    public BigQueryProtoStorageClient(BigQueryWriter writer, BigQuerySinkConfig config, MessageParser parser) {
        this.writer = (BigQueryProtoWriter) writer;
        this.config = config;
        this.parser = parser;
        this.mode = config.getSinkConnectorSchemaMessageMode();
        this.schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? config.getSinkConnectorSchemaProtoMessageClass() : config.getSinkConnectorSchemaProtoKeyClass();
        this.messageParserChecker.scheduleWithFixedDelay(
                () -> parser.refresh(schemaClass),
                MESSAGE_PARSER_CHECKER_DELAY_SECONDS,
                MESSAGE_PARSER_CHECKER_FREQUENCY_SECONDS,
                TimeUnit.SECONDS);
        this.protoUnknownFieldValidationType = config.getSinkConnectorSchemaProtoUnknownFieldsValidation();
        this.sinkConnectorSchemaProtoAllowUnknownFieldsEnable = config.getSinkConnectorSchemaProtoAllowUnknownFieldsEnable();
    }


    /**
     * Converts a batch of messages into a {@link BigQueryPayload} of serialized Protobuf rows.
     *
     * <p>Before processing, the writer's connection is refreshed (which may rebuild the stream and
     * descriptor on schema changes). Each message is then converted against the current descriptor:</p>
     * <ul>
     *     <li>On success a valid {@link BigQueryRecordMeta} is recorded, the valid-index to
     *     input-index mapping is updated and the serialized row is appended to the payload.</li>
     *     <li>On failure the thrown exception is classified into an
     *     {@link com.gotocompany.depot.error.ErrorType} (unknown fields, invalid message,
     *     deserialization or unknown sink error) and stored as an invalid {@link BigQueryRecordMeta};
     *     the row is omitted from the serialized payload so the rest of the batch can proceed.</li>
     * </ul>
     * The accumulated {@code ProtoRows} are set as the payload's serialized content before it is
     * returned.
     *
     * @param messages the ordered batch of messages to convert
     * @return a payload containing the serialized valid rows together with per-record metadata and the
     *         index mapping
     */
    public BigQueryPayload convert(List<Message> messages) {
        ProtoRows.Builder rowBuilder = ProtoRows.newBuilder();
        BigQueryPayload payload = new BigQueryPayload();
        writer.checkAndRefreshConnection();
        Descriptors.Descriptor descriptor = writer.getDescriptor();
        long validIndex = 0;
        for (int index = 0; index < messages.size(); index++) {
            Message message = messages.get(index);
            try {
                DynamicMessage convertedMessage = convert(message, descriptor);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(index, null, true);
                payload.addMetadataRecord(metadata);
                payload.putValidIndexToInputIndex(validIndex++, index);
                rowBuilder.addSerializedRows(convertedMessage.toByteString());
            } catch (UnknownFieldsException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(index, errorInfo, false);
                payload.addMetadataRecord(metadata);
            } catch (EmptyMessageException | UnsupportedOperationException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(index, errorInfo, false);
                payload.addMetadataRecord(metadata);
            } catch (DeserializerException | IllegalArgumentException | IOException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(index, errorInfo, false);
                payload.addMetadataRecord(metadata);
            } catch (Exception e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.SINK_UNKNOWN_ERROR);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(index, errorInfo, false);
                payload.addMetadataRecord(metadata);
            }
        }
        payload.setPayload(rowBuilder.build());
        return payload;
    }

    /**
     * Appends the converted payload to BigQuery by delegating to the underlying writer.
     *
     * @param payload the payload produced by {@link #convert(List)}
     * @return the {@code AppendRowsResponse} returned by the writer
     * @throws ExecutionException   if the underlying append future completes exceptionally
     * @throws InterruptedException if the current thread is interrupted while awaiting the response
     */
    @Override
    public AppendRowsResponse appendAndGet(BigQueryPayload payload) throws ExecutionException, InterruptedException {
        return writer.appendAndGet(payload);
    }


    /**
     * Parses and converts a single message into a {@link DynamicMessage} matching the table descriptor.
     *
     * <p>The message is deserialized through the parser using the configured mode and schema class.
     * Unless unknown fields are explicitly allowed, the parsed message is validated against the
     * configured unknown-field validation strategy. The raw Protobuf message is then mapped onto the
     * table descriptor (as a top-level message) and configured metadata columns are appended before the
     * row is built.</p>
     *
     * @param message    the message to parse and convert
     * @param descriptor the destination table's Protobuf descriptor the row must conform to
     * @return the fully converted row as a {@link DynamicMessage}
     * @throws IOException                    if the message cannot be parsed/deserialized
     * @throws com.gotocompany.depot.exception.UnknownFieldsException if validation detects disallowed
     *                                       unknown fields
     */
    private DynamicMessage convert(Message message, Descriptors.Descriptor descriptor) throws IOException {
        ParsedMessage parsedMessage = parser.parse(message, mode, schemaClass);
        if (!sinkConnectorSchemaProtoAllowUnknownFieldsEnable) {
            parsedMessage.validate(protoUnknownFieldValidationType);
        }
        DynamicMessage.Builder messageBuilder = convert((DynamicMessage) parsedMessage.getRaw(), descriptor, true);
        BigQueryProtoUtils.addMetadata(message.getMetadata(), messageBuilder, descriptor, config);
        return messageBuilder.build();
    }

    /**
     * Recursively maps the fields of an input Protobuf message onto the BigQuery table descriptor.
     *
     * <p>For every field present on the input message, the matching output field is looked up by its
     * lower-cased name on the table descriptor. Fields with no counterpart in the table are skipped.
     * The value is resolved through {@link ProtoFieldFactory} and then handled by type:</p>
     * <ul>
     *     <li>{@link List} values are delegated to {@link #addRepeatedFields(DynamicMessage.Builder,
     *     Descriptors.FieldDescriptor, List)}.</li>
     *     <li>Values whose string form is empty are skipped.</li>
     *     <li>{@link Instant} values with a positive epoch second are converted to BigQuery
     *     microseconds via {@link TimeStampUtils#getBQInstant(Instant, Descriptors.FieldDescriptor,
     *     boolean, BigQuerySinkConfig)} (passing through the top-level flag for partition validation).</li>
     *     <li>Nested message and duration fields are converted recursively (always as non-top-level).</li>
     *     <li>All other scalar values are set directly.</li>
     * </ul>
     *
     * @param inputMessage the source Protobuf message whose fields are mapped
     * @param descriptor   the destination descriptor whose fields define the target shape
     * @param isTopLevel   {@code true} when mapping the root message, which enables partition-key
     *                     timestamp validation; {@code false} for nested messages
     * @return a builder populated with the mapped fields (not yet built)
     * @throws UnsupportedOperationException if a timestamp value violates BigQuery's allowed range
     */
    private DynamicMessage.Builder convert(DynamicMessage inputMessage, Descriptors.Descriptor descriptor, boolean isTopLevel) {
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);
        List<Descriptors.FieldDescriptor> allFields = inputMessage.getDescriptorForType().getFields();
        for (Descriptors.FieldDescriptor inputField : allFields) {
            Descriptors.FieldDescriptor outputField = descriptor.findFieldByName(inputField.getName().toLowerCase());
            if (outputField == null) {
                // not found in table
                continue;
            }
            ProtoField protoField = ProtoFieldFactory.getField(inputField, inputMessage.getField(inputField));
            Object fieldValue = protoField.getValue();
            if (fieldValue instanceof List) {
                addRepeatedFields(messageBuilder, outputField, (List<?>) fieldValue);
                continue;
            }
            if (fieldValue.toString().isEmpty()) {
                continue;
            }
            if (fieldValue instanceof Instant) {
                if (((Instant) fieldValue).getEpochSecond() > 0) {
                    long timeStampValue = TimeStampUtils.getBQInstant((Instant) fieldValue, outputField, isTopLevel, config);
                    messageBuilder.setField(outputField, timeStampValue);
                }
            } else if (protoField.getClass().getName().equals(MessageProtoField.class.getName())
                    || protoField.getClass().getName().equals(DurationProtoField.class.getName())) {
                Descriptors.Descriptor messageType = outputField.getMessageType();
                messageBuilder.setField(outputField, convert((DynamicMessage) fieldValue, messageType, false).build());
            } else {
                messageBuilder.setField(outputField, fieldValue);
            }
        }
        return messageBuilder;
    }

    /**
     * Maps the elements of a repeated field onto the output builder.
     *
     * <p>Empty lists are ignored. Each element is converted according to its type: nested
     * {@link DynamicMessage} elements are converted recursively against the output field's message
     * type; {@link Instant} elements with a positive epoch second are converted to BigQuery
     * microseconds (never as a partition column); all other elements are added as-is. The resulting
     * list is then set on the output field.</p>
     *
     * @param messageBuilder the builder being populated
     * @param outputField    the repeated output field descriptor to set
     * @param fieldValue     the list of source element values to convert and add
     * @throws UnsupportedOperationException if a timestamp element violates BigQuery's allowed range
     */
    private void addRepeatedFields(DynamicMessage.Builder messageBuilder, Descriptors.FieldDescriptor outputField, List<?> fieldValue) {
        if (fieldValue.isEmpty()) {
            return;
        }
        List<Object> repeatedNestedFields = new ArrayList<>();
        for (Object f : fieldValue) {
            if (f instanceof DynamicMessage) {
                Descriptors.Descriptor messageType = outputField.getMessageType();
                repeatedNestedFields.add(convert((DynamicMessage) f, messageType, false).build());
            } else {
                if (f instanceof Instant) {
                    if (((Instant) f).getEpochSecond() > 0) {
                        repeatedNestedFields.add(TimeStampUtils.getBQInstant((Instant) f, outputField, false, config));
                    }
                } else {
                    repeatedNestedFields.add(f);
                }
            }
        }
        messageBuilder.setField(outputField, repeatedNestedFields);
    }

    /**
     * Closes the client by closing the underlying writer and stopping the schema-refresh scheduler.
     *
     * @throws IOException if closing the underlying writer fails
     */
    @Override
    public void close() throws IOException {
        writer.close();
        messageParserChecker.shutdownNow();
    }
}

