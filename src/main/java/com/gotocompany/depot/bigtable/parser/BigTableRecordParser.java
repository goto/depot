package com.gotocompany.depot.bigtable.parser;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.gotocompany.depot.bigtable.model.BigTableRecord;
import com.gotocompany.depot.bigtable.model.BigTableSchema;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Converts incoming {@link Message}s into {@link BigTableRecord}s ready to be written to Bigtable.
 *
 * <p>For each message the parser decodes the configured portion with a {@link MessageParser}, derives
 * the row key from the configured template via {@link BigTableRowKeyParser}, and populates a
 * {@link com.google.cloud.bigtable.data.v2.models.RowMutationEntry} by reading, for every column
 * declared in the {@link BigTableSchema}, the corresponding source field from the parsed message.
 * Conversion never throws for a bad record: any failure is captured as a {@link BigTableRecord}
 * carrying a classified {@link ErrorInfo}, so the batch can be processed as a whole and failures
 * reported per record.</p>
 *
 * @see com.gotocompany.depot.bigtable.BigTableSink
 * @see BigTableSchema
 * @see BigTableRowKeyParser
 */
@Slf4j
public class BigTableRecordParser {
    /** Parser used to decode each message's configured payload. */
    private final MessageParser messageParser;
    /** Parser that renders the configured row-key template against a parsed message. */
    private final BigTableRowKeyParser bigTableRowKeyParser;
    /** Schema describing the column families, columns and their source fields. */
    private final BigTableSchema bigTableSchema;
    /** The schema message mode and schema class used when parsing each message. */
    private final Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema;

    /**
     * Creates a record parser from its collaborators and the active mode and schema.
     *
     * @param messageParser the parser used to decode each message
     * @param bigTableRowKeyParser the parser that builds the row key from the parsed message
     * @param modeAndSchema the schema message mode and schema class to parse each message against
     * @param bigTableSchema the schema describing the column families, columns and source fields
     */
    public BigTableRecordParser(MessageParser messageParser,
                                BigTableRowKeyParser bigTableRowKeyParser,
                                Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema,
                                BigTableSchema bigTableSchema) {
        this.messageParser = messageParser;
        this.bigTableRowKeyParser = bigTableRowKeyParser;
        this.modeAndSchema = modeAndSchema;
        this.bigTableSchema = bigTableSchema;
    }

    /**
     * Converts a batch of messages into Bigtable records, preserving order and index.
     *
     * <p>Each message is converted independently; the resulting list contains one
     * {@link BigTableRecord} per input message, valid or invalid, with each record tagged by its
     * position in {@code messages}.</p>
     *
     * @param messages the messages to convert
     * @return a list of {@link BigTableRecord}s, one per input message in the same order
     */
    public List<BigTableRecord> convert(List<Message> messages) {
        ArrayList<BigTableRecord> records = new ArrayList<>();
        for (int index = 0; index < messages.size(); index++) {
            Message message = messages.get(index);
            BigTableRecord record = createRecord(message, index);
            records.add(record);
        }
        return records;
    }

    /**
     * Converts a single message into a {@link BigTableRecord}, capturing any failure as an error
     * record.
     *
     * <p>Parses the message, derives its row key, and sets a cell for every column declared in the
     * {@link BigTableSchema} using the value of the mapped source field. On success a valid record
     * wrapping the {@link com.google.cloud.bigtable.data.v2.models.RowMutationEntry} is returned (and
     * logged when debug logging is enabled). Failures are mapped to an error record with a classified
     * {@link ErrorType}: an empty payload becomes {@link ErrorType#INVALID_MESSAGE_ERROR}, a
     * configuration or illegal-argument problem becomes {@link ErrorType#UNKNOWN_FIELDS_ERROR}, and a
     * deserialization or I/O failure becomes {@link ErrorType#DESERIALIZATION_ERROR}.</p>
     *
     * @param message the message to convert
     * @param index the zero-based index of the message within the batch
     * @return a valid {@link BigTableRecord} on success, or an error record describing the failure
     */
    private BigTableRecord createRecord(Message message, long index) {
        try {
            ParsedMessage parsedMessage = messageParser.parse(message, modeAndSchema.getFirst(), modeAndSchema.getSecond());
            String rowKey = bigTableRowKeyParser.parse(parsedMessage);
            RowMutationEntry rowMutationEntry = RowMutationEntry.create(rowKey);
            bigTableSchema.getColumnFamilies().forEach(
                    columnFamily -> bigTableSchema
                            .getColumns(columnFamily)
                            .forEach(column -> {
                                String fieldName = bigTableSchema.getField(columnFamily, column);
                                String value = parsedMessage.getFieldByName(fieldName).toString();
                                rowMutationEntry.setCell(columnFamily, column, value);
                            }));
            BigTableRecord bigTableRecord = new BigTableRecord(rowMutationEntry, index, null, message.getMetadata());
            if (log.isDebugEnabled()) {
                log.debug(bigTableRecord.toString());
            }
            return bigTableRecord;
        } catch (EmptyMessageException e) {
            return createErrorRecord(e, ErrorType.INVALID_MESSAGE_ERROR, index, message.getMetadata());
        } catch (ConfigurationException | IllegalArgumentException e) {
            return createErrorRecord(e, ErrorType.UNKNOWN_FIELDS_ERROR, index, message.getMetadata());
        } catch (DeserializerException | IOException e) {
            return createErrorRecord(e, ErrorType.DESERIALIZATION_ERROR, index, message.getMetadata());
        }
    }

    /**
     * Builds an invalid {@link BigTableRecord} that carries the classified failure for a message.
     *
     * @param e the exception that caused conversion to fail
     * @param type the error category to associate with the failure
     * @param index the zero-based index of the message within the batch
     * @param metadata the originating message's metadata, retained for error reporting
     * @return an invalid {@link BigTableRecord} holding an {@link ErrorInfo} built from {@code e} and
     *     {@code type}
     */
    private BigTableRecord createErrorRecord(Exception e, ErrorType type, long index, Map<String, Object> metadata) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        return new BigTableRecord(null, index, errorInfo, metadata);
    }
}
