package com.gotocompany.depot.kafka.parser;

import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.exception.ProtoMappingException;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunction;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunctionCache;
import com.gotocompany.depot.kafka.record.KafkaRecord;
import com.gotocompany.depot.kafka.serializer.KafkaMessageSerializer;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts source messages into Kafka records by parsing, mapping and serializing them.
 *
 * <p>Each message is converted independently so that a failure on one message does not stop the batch.
 * Failures are captured as invalid records carrying the classified error type rather than being thrown.
 */
@AllArgsConstructor
@Slf4j
public class KafkaRecordParser {

    private final MessageParser messageParser;
    private final ProtoMappingFunctionCache mappingFunctionCache;
    private final KafkaMessageSerializer serializer;
    private final Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema;

    /**
     * Converts a batch of source messages into Kafka records.
     *
     * <p>Deserialization failures map to a deserialization error, mapping evaluation failures to an invalid
     * message error and any other unexpected runtime failure to an unknown error.
     *
     * @param messages the source messages to convert
     * @return the converted records, each either valid or carrying its error info, aligned by index with the input
     */
    public List<KafkaRecord> convert(List<Message> messages) {
        List<KafkaRecord> records = new ArrayList<>();
        for (int index = 0; index < messages.size(); index++) {
            Message message = messages.get(index);
            try {
                records.add(createRecord(message, index));
            } catch (ProtoMappingException e) {
                records.add(createAndLogErrorRecord(e, ErrorType.INVALID_MESSAGE_ERROR, index, message));
            } catch (DeserializerException | IOException e) {
                records.add(createAndLogErrorRecord(e, ErrorType.DESERIALIZATION_ERROR, index, message));
            } catch (RuntimeException e) {
                records.add(createAndLogErrorRecord(e, ErrorType.SINK_UNKNOWN_ERROR, index, message));
            }
        }
        return records;
    }

    /**
     * Parses, maps and serializes a single message into a valid Kafka record.
     *
     * @param message the source message to convert
     * @param index   the position of the message in the batch
     * @return the valid Kafka record holding the serialized key and value
     * @throws IOException if the message cannot be parsed
     */
    private KafkaRecord createRecord(Message message, long index) throws IOException {
        ParsedMessage parsedMessage = messageParser.parse(message, modeAndSchema.getFirst(), modeAndSchema.getSecond());
        com.google.protobuf.Message sourceMessage = (com.google.protobuf.Message) parsedMessage.getRaw();
        ProtoMappingFunction mappingFunction = mappingFunctionCache.get();
        byte[] value = serializer.serialize(mappingFunction.mapValue(sourceMessage));
        byte[] key = mappingFunction.hasKeyMapping() ? serializer.serialize(mappingFunction.mapKey(sourceMessage)) : null;
        return KafkaRecord.validRecord(index, key, value, message.getMetadataString());
    }

    /**
     * Builds and logs an invalid record for a message that failed to convert.
     *
     * @param e       the failure that occurred while converting the message
     * @param type    the error type classifying the failure
     * @param index   the position of the message in the batch
     * @param message the source message that failed to convert
     * @return the invalid Kafka record carrying the error info
     */
    private KafkaRecord createAndLogErrorRecord(Exception e, ErrorType type, long index, Message message) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        KafkaRecord record = KafkaRecord.invalidRecord(index, errorInfo, message.getMetadataString());
        log.error("Error while parsing record for message. Record: {}, Error: {}", record, errorInfo);
        return record;
    }
}
