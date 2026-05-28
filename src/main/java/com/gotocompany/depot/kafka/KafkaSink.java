package com.gotocompany.depot.kafka;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import dev.cel.runtime.CelEvaluationException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

@Slf4j
public class KafkaSink implements Sink {

    private final ProtoMessageParser sourceMessageParser;
    private final ProtoMappingFunction valueMappingFunction;
    private final ProtoMappingFunction keyMappingFunction;
    private final KafkaProducerClient producerClient;
    private final String sourceProtoMessageClass;
    private final String sourceProtoKeyClass;

    public KafkaSink(
            ProtoMessageParser sourceMessageParser,
            ProtoMappingFunction valueMappingFunction,
            ProtoMappingFunction keyMappingFunction,
            KafkaProducerClient producerClient,
            String sourceProtoMessageClass,
            String sourceProtoKeyClass) {
        this.sourceMessageParser = sourceMessageParser;
        this.valueMappingFunction = valueMappingFunction;
        this.keyMappingFunction = keyMappingFunction;
        this.producerClient = producerClient;
        this.sourceProtoMessageClass = sourceProtoMessageClass;
        this.sourceProtoKeyClass = sourceProtoKeyClass;
    }

    @Override
    public SinkResponse pushToSink(List<Message> messages) throws SinkException {
        SinkResponse sinkResponse = new SinkResponse();
        for (int i = 0; i < messages.size(); i++) {
            long index = i;
            Message message = messages.get((int) index);
            try {
                byte[] valueBytes = mapAndSerialize(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sourceProtoMessageClass, valueMappingFunction);
                byte[] keyBytes = mapAndSerialize(message, SinkConnectorSchemaMessageMode.LOG_KEY, sourceProtoKeyClass, keyMappingFunction);
                producerClient.send(keyBytes, valueBytes);
            } catch (IOException e) {
                log.error("Deserialization error for message index {}: {}", index, e.getMessage());
                sinkResponse.addErrors(index, new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
            } catch (CelEvaluationException e) {
                log.error("CEL mapping error for message index {}: {}", index, e.getMessage());
                sinkResponse.addErrors(index, new ErrorInfo(e, ErrorType.SINK_NON_RETRYABLE_ERROR));
            } catch (RuntimeException e) {
                log.error("Kafka producer error for message index {}: {}", index, e.getMessage());
                sinkResponse.addErrors(index, new ErrorInfo(e, ErrorType.SINK_RETRYABLE_ERROR));
            }
        }
        return sinkResponse;
    }

    private byte[] mapAndSerialize(
            Message message,
            SinkConnectorSchemaMessageMode mode,
            String protoClass,
            ProtoMappingFunction mappingFunction) throws IOException, CelEvaluationException {
        ParsedMessage parsed = sourceMessageParser.parse(message, mode, protoClass);
        DynamicMessage sourceDynMsg = (DynamicMessage) parsed.getRaw();
        DynamicMessage sinkDynMsg = mappingFunction.map(sourceDynMsg);
        return sinkDynMsg.toByteArray();
    }

    @Override
    public void close() throws IOException {
        producerClient.close();
    }
}
