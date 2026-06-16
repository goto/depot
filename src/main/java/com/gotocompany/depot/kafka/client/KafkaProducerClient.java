package com.gotocompany.depot.kafka.client;

import com.gotocompany.depot.kafka.record.KafkaRecord;
import com.gotocompany.depot.kafka.response.KafkaProduceResponse;
import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * {@link KafkaSinkClient} backed by a Kafka {@link Producer} that produces records and resolves their acknowledgements.
 *
 * <p>All records are submitted first, then the producer is flushed and each acknowledgement future is
 * resolved into a {@link KafkaProduceResponse} aligned positionally with the input records.
 */
public class KafkaProducerClient implements KafkaSinkClient {

    private final Producer<byte[], byte[]> producer;
    private final String topic;
    private final Instrumentation instrumentation;

    /**
     * Creates a producer client.
     *
     * @param producer        the Kafka producer used to send records
     * @param topic           the output topic that all records are produced to
     * @param instrumentation the instrumentation used for logging
     */
    public KafkaProducerClient(Producer<byte[], byte[]> producer, String topic, Instrumentation instrumentation) {
        this.producer = producer;
        this.topic = topic;
        this.instrumentation = instrumentation;
    }

    /**
     * Produces all records to the configured topic and resolves their acknowledgements.
     *
     * @param records the records to produce
     * @return the produce responses aligned by index with the input records
     */
    @Override
    public List<KafkaProduceResponse> send(List<KafkaRecord> records) {
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        List<KafkaProduceResponse> responses = new ArrayList<>();
        List<Throwable> sendErrors = new ArrayList<>();
        instrumentation.logDebug("Sending {} records to the kafka topic {}", records.size(), topic);
        for (KafkaRecord record : records) {
            futures.add(sendRecord(record, sendErrors));
        }
        producer.flush();
        for (int index = 0; index < records.size(); index++) {
            responses.add(getResponse(futures.get(index), sendErrors.get(index)));
        }
        return responses;
    }

    /**
     * Submits a single record to the producer, capturing any synchronous send failure.
     *
     * @param record     the record to submit
     * @param sendErrors the accumulator that receives the synchronous send error, or {@code null} on success
     * @return the acknowledgement future, or {@code null} when the send failed synchronously
     */
    private Future<RecordMetadata> sendRecord(KafkaRecord record, List<Throwable> sendErrors) {
        try {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, record.getKey(), record.getValue()));
            sendErrors.add(null);
            return future;
        } catch (KafkaException | IllegalStateException e) {
            instrumentation.logError("Error while sending record to kafka. Record: {}, Error: {}", record, e.getMessage());
            sendErrors.add(e);
            return null;
        }
    }

    /**
     * Resolves a single record acknowledgement into a produce response.
     *
     * <p>If the producing thread is interrupted while waiting, the interrupt flag is preserved and the
     * record is reported as failed.
     *
     * @param future    the acknowledgement future, or {@code null} when the send failed synchronously
     * @param sendError the synchronous send error, or {@code null} when the record was submitted successfully
     * @return the produce response for the record
     */
    private KafkaProduceResponse getResponse(Future<RecordMetadata> future, Throwable sendError) {
        if (sendError != null) {
            return KafkaProduceResponse.failure(sendError);
        }
        try {
            future.get();
            return KafkaProduceResponse.success();
        } catch (ExecutionException e) {
            return KafkaProduceResponse.failure(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return KafkaProduceResponse.failure(e);
        }
    }

    /**
     * Closes the underlying Kafka producer.
     */
    @Override
    public void close() {
        instrumentation.logInfo("Closing kafka producer for the topic {}", topic);
        producer.close();
    }
}
