package com.gotocompany.depot.kafka;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunction;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import dev.cel.runtime.CelEvaluationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;

import java.io.IOException;
import java.util.List;

public class KafkaSink implements Sink {

    private final KafkaProducer<byte[], byte[]> producer;
    private final ProtoMappingFunction mappingFunction;
    private final Descriptors.Descriptor sourceMessageDescriptor;
    private final Descriptors.Descriptor sourceKeyDescriptor;
    private final String topic;
    private final Instrumentation instrumentation;

    public KafkaSink(KafkaProducer<byte[], byte[]> producer,
                     ProtoMappingFunction mappingFunction,
                     Descriptors.Descriptor sourceMessageDescriptor,
                     Descriptors.Descriptor sourceKeyDescriptor,
                     String topic,
                     Instrumentation instrumentation) {
        this.producer = producer;
        this.mappingFunction = mappingFunction;
        this.sourceMessageDescriptor = sourceMessageDescriptor;
        this.sourceKeyDescriptor = sourceKeyDescriptor;
        this.topic = topic;
        this.instrumentation = instrumentation;
    }

    @Override
    public SinkResponse pushToSink(List<Message> messages) {
        SinkResponse response = new SinkResponse();
        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            try {
                DynamicMessage sourceMessage = parseMessage((byte[]) message.getLogMessage(), sourceMessageDescriptor);
                ProtoMappingFunction.MappedMessages mapped = mappingFunction.map(sourceMessage);

                byte[] outputValue = mapped.getMessage().toByteArray();
                byte[] outputKey = mapped.getKey() != null ? mapped.getKey().toByteArray() : null;

                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, outputKey, outputValue);
                producer.send(record).get();
            } catch (InvalidProtocolBufferException e) {
                response.addErrors(i, new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
                instrumentation.logWarn("Deserialization error at index {}: {}", i, e.getMessage());
            } catch (CelEvaluationException e) {
                response.addErrors(i, new ErrorInfo(new Exception(e), ErrorType.SINK_NON_RETRYABLE_ERROR));
                instrumentation.logWarn("Mapping error at index {}: {}", i, e.getMessage());
            } catch (Exception e) {
                ErrorType errorType = (e.getCause() instanceof RetriableException)
                        ? ErrorType.SINK_RETRYABLE_ERROR : ErrorType.SINK_NON_RETRYABLE_ERROR;
                response.addErrors(i, new ErrorInfo(new Exception(e), errorType));
                instrumentation.logWarn("Producer error at index {}: {}", i, e.getMessage());
            }
        }
        if (!response.hasErrors()) {
            instrumentation.logInfo("Pushed {} messages to Kafka topic {}", messages.size(), topic);
        }
        return response;
    }

    private DynamicMessage parseMessage(byte[] payload, Descriptors.Descriptor descriptor)
            throws InvalidProtocolBufferException {
        return DynamicMessage.parseFrom(descriptor, payload);
    }

    @Override
    public void close() throws IOException {
        producer.flush();
        producer.close();
        instrumentation.logInfo("Kafka producer closed");
    }
}
