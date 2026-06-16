package com.gotocompany.depot.kafka;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.kafka.client.KafkaSinkClient;
import com.gotocompany.depot.kafka.parser.KafkaRecordParser;
import com.gotocompany.depot.kafka.record.KafkaRecord;
import com.gotocompany.depot.kafka.response.KafkaProduceResponse;
import com.gotocompany.depot.kafka.response.KafkaResponseParser;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.KafkaSinkMetrics;
import com.gotocompany.depot.metrics.SinkMetrics;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Depot {@link Sink} implementation that maps source proto messages to sink key and value proto messages
 * and produces them to the configured Kafka topic.
 *
 * <p>Each batch is partitioned into valid and invalid records. Parsing failures are reported per message,
 * the valid records are produced, and any producer failures are correlated back to their originating
 * message index while batch level metrics and logs are emitted.
 */
public class KafkaSink implements Sink {

    private final KafkaSinkClient kafkaSinkClient;
    private final KafkaRecordParser recordParser;
    private final KafkaSinkMetrics metrics;
    private final Instrumentation instrumentation;

    /**
     * Creates a Kafka sink.
     *
     * @param kafkaSinkClient the client that produces records to Kafka
     * @param recordParser    the parser that converts source messages into Kafka records
     * @param metrics         the Kafka sink metric names
     * @param instrumentation the instrumentation used for logging and metric capture
     */
    public KafkaSink(KafkaSinkClient kafkaSinkClient,
                     KafkaRecordParser recordParser,
                     KafkaSinkMetrics metrics,
                     Instrumentation instrumentation) {
        this.kafkaSinkClient = kafkaSinkClient;
        this.recordParser = recordParser;
        this.metrics = metrics;
        this.instrumentation = instrumentation;
    }

    /**
     * Parses, maps and produces a batch of messages to Kafka.
     *
     * <p>Invalid records are reported with their parsing error type, valid records are produced and any
     * producer failures are correlated back to their originating message index.
     *
     * @param messages the batch of source messages to push
     * @return the sink response holding the per message errors keyed by message index
     */
    @Override
    public SinkResponse pushToSink(List<Message> messages) {
        List<KafkaRecord> records = recordParser.convert(messages);
        Map<Boolean, List<KafkaRecord>> splitterRecords = records.stream().collect(Collectors.partitioningBy(KafkaRecord::isValid));
        List<KafkaRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<KafkaRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        SinkResponse sinkResponse = new SinkResponse();
        invalidRecords.forEach(invalidRecord -> {
            sinkResponse.addErrors(invalidRecord.getIndex(), invalidRecord.getErrorInfo());
            instrumentation.incrementCounter(metrics.getKafkaRecordParseErrorsTotalMetric(),
                    String.format(SinkMetrics.ERROR_TYPE_TAG, invalidRecord.getErrorInfo().getErrorType()));
        });
        if (!validRecords.isEmpty()) {
            instrumentation.captureHistogram(metrics.getKafkaProduceBatchSizeMetric(), (long) validRecords.size());
            Instant startTime = Instant.now();
            Map<Long, ErrorInfo> errorInfoMap = send(validRecords);
            instrumentation.captureDurationSince(metrics.getKafkaProduceLatencyMetric(), startTime);
            instrumentation.captureCount(metrics.getKafkaMessagesProducedTotalMetric(), (long) (validRecords.size() - errorInfoMap.size()));
            errorInfoMap.forEach(sinkResponse::addErrors);
            instrumentation.logInfo("Pushed {} valid records to Kafka, {} failed parsing, {} failed producing",
                    validRecords.size(), invalidRecords.size(), errorInfoMap.size());
        } else if (!invalidRecords.isEmpty()) {
            instrumentation.logInfo("No valid records to push to Kafka, {} records failed parsing", invalidRecords.size());
        }
        return sinkResponse;
    }

    /**
     * Produces the valid records and classifies any failures into per message errors.
     *
     * <p>If the client throws while producing the whole batch, every record in the batch is reported with
     * the classified error and the failure metrics are emitted for each record so that the counts stay
     * consistent with the per record failure path.
     *
     * @param validRecords the records that parsed and mapped successfully
     * @return the map of originating message index to error info for every failed record
     */
    private Map<Long, ErrorInfo> send(List<KafkaRecord> validRecords) {
        List<KafkaProduceResponse> responses;
        try {
            responses = kafkaSinkClient.send(validRecords);
        } catch (RuntimeException e) {
            instrumentation.logError("Error while pushing records to kafka: {}", e.getMessage());
            ErrorInfo errorInfo = KafkaResponseParser.getErrorInfo(e);
            Map<Long, ErrorInfo> errors = new HashMap<>();
            for (KafkaRecord record : validRecords) {
                errors.put(record.getIndex(), errorInfo);
                instrumentation.incrementCounter(metrics.getKafkaFailureResponseTotalMetric());
                instrumentation.incrementCounter(metrics.getKafkaErrorsTotalMetric(),
                        String.format(SinkMetrics.ERROR_TYPE_TAG, errorInfo.getErrorType()));
            }
            return errors;
        }
        return KafkaResponseParser.getErrors(validRecords, responses, metrics, instrumentation);
    }

    /**
     * Closes the underlying Kafka producer client.
     *
     * @throws IOException if the producer client fails to close
     */
    @Override
    public void close() throws IOException {
        kafkaSinkClient.close();
    }
}
