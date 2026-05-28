package com.gotocompany.depot.kafka;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageUtils;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.stencil.client.StencilClient;
import dev.cel.runtime.CelEvaluationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaSink implements Sink {

    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;
    private final StencilClient sourceStencilClient;
    private final String sourceProtoClass;
    private final ProtoMappingEngine mappingEngine;
    private final Instrumentation instrumentation;

    public KafkaSink(
            KafkaProducer<byte[], byte[]> producer,
            String topic,
            StencilClient sourceStencilClient,
            String sourceProtoClass,
            ProtoMappingEngine mappingEngine,
            Instrumentation instrumentation) {
        this.producer = producer;
        this.topic = topic;
        this.sourceStencilClient = sourceStencilClient;
        this.sourceProtoClass = sourceProtoClass;
        this.mappingEngine = mappingEngine;
        this.instrumentation = instrumentation;
    }

    @Override
    public SinkResponse pushToSink(List<Message> messages) {
        SinkResponse response = new SinkResponse();
        for (int i = 0; i < messages.size(); i++) {
            try {
                com.google.protobuf.Message source = parseSource(messages.get(i));
                ProtoMappingEngine.MappedRecord mapped = mappingEngine.map(source);
                Future<RecordMetadata> future = producer.send(new ProducerRecord<>(
                        topic,
                        mapped.getKey().toByteArray(),
                        mapped.getMessage().toByteArray()));
                future.get();
            } catch (CelEvaluationException e) {
                response.addErrors(i, new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR));
            } catch (IOException | EmptyMessageException e) {
                response.addErrors(i, new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                response.addErrors(i, new ErrorInfo(e, ErrorType.SINK_RETRYABLE_ERROR));
            } catch (ExecutionException e) {
                response.addErrors(i, new ErrorInfo(e, ErrorType.SINK_UNKNOWN_ERROR));
            } catch (RuntimeException e) {
                response.addErrors(i, new ErrorInfo(e, ErrorType.SINK_NON_RETRYABLE_ERROR));
            }
        }
        if (!response.hasErrors()) {
            instrumentation.logInfo("Pushed {} records to Kafka topic {}", messages.size(), topic);
        }
        return response;
    }

    private com.google.protobuf.Message parseSource(Message message) throws IOException {
        MessageUtils.validate(message, byte[].class);
        byte[] payload = (byte[]) message.getLogMessage();
        if (payload == null || payload.length == 0) {
            throw new EmptyMessageException();
        }
        return sourceStencilClient.parse(sourceProtoClass, payload);
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
