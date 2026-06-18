package com.gotocompany.depot.redis.parsers;

import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;


/**
 * Convert Messages to RedisRecords.
 *
 * <p>This is the bridge between Depot's generic message pipeline and the Redis sink. Each message is
 * decoded by the configured {@link MessageParser} using the resolved
 * {@link SinkConnectorSchemaMessageMode} and schema, then handed to the data-type specific
 * {@link RedisEntryParser} to produce one or more {@link RedisEntry} objects. Every entry becomes a
 * valid {@link RedisRecord}; any parsing failure becomes an invalid record that carries the
 * corresponding {@link ErrorType} so the sink can report it without aborting the rest of the
 * batch.</p>
 *
 * @see RedisEntryParser
 * @see RedisRecord
 */

@AllArgsConstructor
@Slf4j
public class RedisParser {
    /**
     * Decodes each raw message into a {@link ParsedMessage} according to the schema mode.
     */
    private final MessageParser messageParser;
    /**
     * Converts a decoded message into the {@link RedisEntry} objects to write.
     */
    private final RedisEntryParser redisEntryParser;
    /**
     * Resolved parsing mode and schema class passed to the message parser; the first element is the
     * {@link SinkConnectorSchemaMessageMode} and the second is the schema class name.
     */
    private final Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema;

    /**
     * Converts every message in the batch into one or more {@link RedisRecord} instances.
     *
     * <p>Each message is parsed and converted in order. A successful conversion appends one valid
     * record per produced {@link RedisEntry}, tagging it with the message's batch index and metadata.
     * A failure is caught and mapped to an invalid record via
     * {@link #createAndLogErrorRecord(Exception, ErrorType, int, List)} with an {@link ErrorType}
     * chosen by exception: {@link UnsupportedOperationException} maps to {@code INVALID_MESSAGE_ERROR},
     * {@link ConfigurationException} to {@code UNKNOWN_FIELDS_ERROR}, {@link IllegalArgumentException}
     * to {@code DEFAULT_ERROR}, and {@link DeserializerException} or {@link IOException} to
     * {@code DESERIALIZATION_ERROR}.</p>
     *
     * @param messages the batch of messages to convert
     * @return the resulting records, containing both valid records and invalid (error) records; this
     *     is not necessarily one-to-one with the input, since a single message may yield several
     *     entries
     */
    public List<RedisRecord> convert(List<Message> messages) {
        List<RedisRecord> records = new ArrayList<>();
        IntStream.range(0, messages.size()).forEach(index -> {
            try {
                ParsedMessage parsedMessage = messageParser.parse(messages.get(index), modeAndSchema.getFirst(), modeAndSchema.getSecond());
                List<RedisEntry> redisDataEntries = redisEntryParser.getRedisEntry(parsedMessage);
                for (RedisEntry redisEntry : redisDataEntries) {
                    records.add(new RedisRecord(redisEntry, (long) index, null, messages.get(index).getMetadataString(), true));
                }
            } catch (UnsupportedOperationException e) {
                records.add(createAndLogErrorRecord(e, ErrorType.INVALID_MESSAGE_ERROR, index, messages));
            } catch (ConfigurationException e) {
                records.add(createAndLogErrorRecord(e, ErrorType.UNKNOWN_FIELDS_ERROR, index, messages));
            } catch (IllegalArgumentException e) {
                records.add(createAndLogErrorRecord(e, ErrorType.DEFAULT_ERROR, index, messages));
            } catch (DeserializerException | IOException e) {
                records.add(createAndLogErrorRecord(e, ErrorType.DESERIALIZATION_ERROR, index, messages));
            }
        });
        return records;
    }

    /**
     * Creates an invalid {@link RedisRecord} for a message that failed to parse and logs the failure.
     *
     * @param e the exception that caused the failure
     * @param type the {@link ErrorType} categorising the failure
     * @param index the batch index of the offending message
     * @param messages the batch being processed, used to read the offending message's metadata
     * @return an invalid record wrapping a new {@link ErrorInfo} built from {@code e} and {@code type}
     */
    private RedisRecord createAndLogErrorRecord(Exception e, ErrorType type, int index, List<Message> messages) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        RedisRecord record = new RedisRecord(null, (long) index, errorInfo, messages.get(index).getMetadataString(), false);
        log.error("Error while parsing record for message. Record: {}, Error: {}", record, errorInfo);
        return record;
    }
}
