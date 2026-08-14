package com.gotocompany.depot.maxcompute.converter.record;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ReorderableRecord;
import com.aliyun.odps.exceptions.SchemaMismatchException;
import com.aliyun.odps.exceptions.SchemaMismatchRuntimeException;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.exception.UnknownFieldsException;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.maxcompute.record.RecordDecorator;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.message.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

/**
 * ProtoMessageRecordConverter converts a list of proto messages to RecordWrappers.
 *
 * <p>For each message it allocates a {@link com.aliyun.odps.data.ReorderableRecord} built from the current
 * {@link MaxComputeSchema}, then applies the {@link RecordDecorator} chain to populate it. Conversion failures
 * are caught per message and recorded as invalid records carrying a typed {@link ErrorInfo}, so that a single
 * bad message never aborts the rest of the batch. The mapping from exception to {@link ErrorType} is:</p>
 * <ul>
 *     <li>schema mismatch (checked or runtime) becomes {@link ErrorType#SINK_NON_RETRYABLE_ERROR};</li>
 *     <li>{@link java.io.IOException} becomes {@link ErrorType#DESERIALIZATION_ERROR};</li>
 *     <li>{@link UnknownFieldsException} becomes {@link ErrorType#UNKNOWN_FIELDS_ERROR};</li>
 *     <li>{@link InvalidMessageException} and {@link EmptyMessageException} become
 *     {@link ErrorType#INVALID_MESSAGE_ERROR};</li>
 *     <li>any other exception becomes {@link ErrorType#SINK_UNKNOWN_ERROR}.</li>
 * </ul>
 *
 * @see MessageRecordConverter
 * @see RecordDecorator
 */
@RequiredArgsConstructor
@Slf4j
public class ProtoMessageRecordConverter implements MessageRecordConverter {

    /**
     * Decorator chain that populates a record's columns (data and metadata) from a message.
     */
    private final RecordDecorator recordDecorator;
    /**
     * Cache supplying the current {@link MaxComputeSchema} used to allocate each record.
     */
    private final MaxComputeSchemaCache maxComputeSchemaCache;

    /**
     * Converts a list of messages to RecordWrappers.
     * MaxComputeSchema is used to create the Record object, which is used to represent logical structure of a row in a table.
     *
     * @param messages list of proto messages
     * @return RecordWrappers encapsulating valid and invalid records
     */
    @Override
    public RecordWrappers convert(List<Message> messages) {
        MaxComputeSchema maxComputeSchema = maxComputeSchemaCache.getMaxComputeSchema();
        RecordWrappers recordWrappers = new RecordWrappers();
        IntStream.range(0, messages.size())
                .forEach(index -> {
                    Record record = new ReorderableRecord(maxComputeSchema.getTableSchema());
                    RecordWrapper recordWrapper = new RecordWrapper(record, index, null, null);
                    try {
                        recordWrappers.addValidRecord(recordDecorator.decorate(recordWrapper, messages.get(index)));
                    } catch (SchemaMismatchException | SchemaMismatchRuntimeException e) {
                        log.debug("Schema mismatch error while converting message to record", e);
                        recordWrappers.addInvalidRecord(
                                toErrorRecordWrapper(recordWrapper, new ErrorInfo(e, ErrorType.SINK_NON_RETRYABLE_ERROR))
                        );
                    } catch (IOException e) {
                        log.debug("Deserialization error while converting message to record", e);
                        recordWrappers.addInvalidRecord(
                                toErrorRecordWrapper(recordWrapper, new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR))
                        );
                    } catch (UnknownFieldsException e) {
                        log.debug("Unknown field message error while converting message to record", e);
                        recordWrappers.addInvalidRecord(
                                toErrorRecordWrapper(recordWrapper, new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR))
                        );
                    } catch (InvalidMessageException | EmptyMessageException e) {
                        log.debug("Invalid message error while converting message to record", e);
                        recordWrappers.addInvalidRecord(
                                toErrorRecordWrapper(recordWrapper, new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR))
                        );
                    } catch (Exception e) {
                        log.debug("Unknown error while converting message to record", e);
                        recordWrappers.addInvalidRecord(
                                toErrorRecordWrapper(recordWrapper, new ErrorInfo(e, ErrorType.SINK_UNKNOWN_ERROR))
                        );
                    }
                });
        return recordWrappers;
    }

    /**
     * Builds an invalid-record wrapper that carries the error and preserves the original index and partition spec.
     *
     * @param recordWrapper the wrapper of the record that failed conversion, used for its index and partition spec
     * @param e             the error describing why the conversion failed
     * @return a {@link RecordWrapper} with a {@code null} record and the supplied {@link ErrorInfo}
     */
    private RecordWrapper toErrorRecordWrapper(RecordWrapper recordWrapper, ErrorInfo e) {
        return new RecordWrapper(null, recordWrapper.getIndex(), e, recordWrapper.getPartitionSpec());
    }
}
