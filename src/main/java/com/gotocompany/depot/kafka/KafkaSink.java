package com.gotocompany.depot.kafka;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.stencil.client.StencilClient;
import dev.cel.runtime.CelEvaluationException;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaSink implements Sink {

    private final KafkaSinkConfig config;
    private final KafkaProtoMappingEngine mappingEngine;
    private final KafkaSinkProducer producer;
    private final StencilClient sourceStencilClient;
    private final Instrumentation instrumentation;

    public KafkaSink(KafkaSinkConfig config,
                     KafkaProtoMappingEngine mappingEngine,
                     KafkaSinkProducer producer,
                     StencilClient sourceStencilClient,
                     Instrumentation instrumentation) {
        this.config = config;
        this.mappingEngine = mappingEngine;
        this.producer = producer;
        this.sourceStencilClient = sourceStencilClient;
        this.instrumentation = instrumentation;
    }

    @Override
    public SinkResponse pushToSink(List<Message> messages) {
        SinkResponse response = new SinkResponse();
        String sourceMessageClass = config.getSinkConnectorSchemaProtoMessageClass();
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        List<Integer> futureIndexes = new ArrayList<>();

        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            try {
                // 1. Parse source message bytes to DynamicMessage
                byte[] logMessage = (byte[]) message.getLogMessage();
                byte[] logKey = (byte[]) message.getLogKey();

                DynamicMessage sourceMessage = sourceStencilClient.parse(sourceMessageClass, logMessage);

                // 2. Map source to sink using CEL expressions
                DynamicMessage sinkMessage = mappingEngine.mapToSinkMessage(sourceMessage);
                DynamicMessage sinkKey = mappingEngine.mapToSinkKey(sourceMessage);

                // 3. Serialize to bytes
                byte[] serializedValue = sinkMessage.toByteArray();
                byte[] serializedKey = sinkKey != null ? sinkKey.toByteArray() : logKey;

                // 4. Produce to output topic
                Future<RecordMetadata> future = producer.produce(serializedKey, serializedValue);
                futures.add(future);
                futureIndexes.add(i);

            } catch (CelEvaluationException e) {
                response.addErrors(i, new ErrorInfo(new Exception(e), ErrorType.SINK_NON_RETRYABLE_ERROR));
            } catch (Exception e) {
                response.addErrors(i, new ErrorInfo(new Exception(e), ErrorType.DESERIALIZATION_ERROR));
            }
        }

        // 5. Flush and collect producer errors
        producer.flush();
        for (int j = 0; j < futures.size(); j++) {
            try {
                futures.get(j).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                response.addErrors(futureIndexes.get(j),
                        new ErrorInfo(new Exception(e), ErrorType.SINK_RETRYABLE_ERROR));
            } catch (ExecutionException e) {
                response.addErrors(futureIndexes.get(j),
                        new ErrorInfo(new Exception(e.getCause()), ErrorType.SINK_RETRYABLE_ERROR));
            }
        }

        instrumentation.logInfo("Pushed batch of {} messages to Kafka sink topic {}", messages.size(), config.getSinkKafkaTopic());
        return response;
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            producer.close();
        }
    }
}
