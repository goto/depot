package com.gotocompany.depot.kafka;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import dev.cel.runtime.CelEvaluationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

@Slf4j
public class KafkaSink implements Sink {

    private final KafkaMessageParser messageParser;
    private final ProtoMapper valueMapper;
    private final ProtoMapper keyMapper;
    private final KafkaMessageSerializer serializer;
    private final KafkaProducerWrapper producer;

    public KafkaSink(
            KafkaMessageParser messageParser,
            ProtoMapper valueMapper,
            ProtoMapper keyMapper,
            KafkaMessageSerializer serializer,
            KafkaProducerWrapper producer) {
        this.messageParser = messageParser;
        this.valueMapper = valueMapper;
        this.keyMapper = keyMapper;
        this.serializer = serializer;
        this.producer = producer;
    }

    @Override
    public SinkResponse pushToSink(List<Message> messages) {
        SinkResponse response = new SinkResponse();
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        List<Integer> validIndexes = new ArrayList<>();

        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            try {
                byte[] rawValue = (byte[]) message.getLogMessage();
                DynamicMessage sourceMessage = messageParser.parse(rawValue);

                DynamicMessage mappedValue = valueMapper.map(sourceMessage);
                byte[] serializedValue = serializer.serialize(mappedValue);

                byte[] serializedKey = new byte[0];
                if (keyMapper != null) {
                    DynamicMessage mappedKey = keyMapper.map(sourceMessage);
                    serializedKey = serializer.serialize(mappedKey);
                }

                Future<RecordMetadata> future = producer.send(serializedKey, serializedValue);
                futures.add(future);
                validIndexes.add(i);
            } catch (CelEvaluationException e) {
                log.error("CEL mapping error for message at index {}: {}", i, e.getMessage());
                response.addErrors(i, new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR));
            } catch (Exception e) {
                log.error("Error processing message at index {}: {}", i, e.getMessage());
                response.addErrors(i, new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
            }
        }

        producer.flush();

        for (int j = 0; j < futures.size(); j++) {
            int originalIndex = validIndexes.get(j);
            try {
                futures.get(j).get();
            } catch (Exception e) {
                log.error("Kafka produce error for message at index {}: {}", originalIndex, e.getMessage());
                response.addErrors(originalIndex, new ErrorInfo(
                        new Exception("Kafka produce failed: " + e.getMessage(), e),
                        ErrorType.SINK_RETRYABLE_ERROR));
            }
        }

        return response;
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
