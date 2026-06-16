package com.gotocompany.depot.kafka.response;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.kafka.record.KafkaRecord;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.KafkaSinkMetrics;
import com.gotocompany.depot.metrics.SinkMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RetriableException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds per record errors from Kafka produce responses and classifies producer throwables into error types.
 */
public final class KafkaResponseParser {

    /**
     * Prevents instantiation of this utility class.
     */
    private KafkaResponseParser() {
    }

    /**
     * Maps failed produce responses to per message errors and captures the success and failure metrics.
     *
     * <p>The records and responses must be aligned by index. Each failure increments the failure total and
     * the errors total tagged by error type, while each success increments the success total.
     *
     * @param records         the produced records, aligned by index with the responses
     * @param responses       the produce responses, aligned by index with the records
     * @param metrics         the Kafka sink metric names
     * @param instrumentation the instrumentation used for logging and metric capture
     * @return the map of originating message index to error info for every failed record
     */
    public static Map<Long, ErrorInfo> getErrors(List<KafkaRecord> records,
                                                 List<KafkaProduceResponse> responses,
                                                 KafkaSinkMetrics metrics,
                                                 Instrumentation instrumentation) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        for (int index = 0; index < records.size(); index++) {
            KafkaRecord record = records.get(index);
            KafkaProduceResponse response = responses.get(index);
            if (response.isFailed()) {
                ErrorInfo errorInfo = getErrorInfo(response.getError());
                errors.put(record.getIndex(), errorInfo);
                instrumentation.logError("Error while producing record to kafka. Record: {}, Error: {}", record, response.getMessage());
                instrumentation.incrementCounter(metrics.getKafkaFailureResponseTotalMetric());
                instrumentation.incrementCounter(metrics.getKafkaErrorsTotalMetric(),
                        String.format(SinkMetrics.ERROR_TYPE_TAG, errorInfo.getErrorType()));
            } else {
                instrumentation.incrementCounter(metrics.getKafkaSuccessResponseTotalMetric());
            }
        }
        return errors;
    }

    /**
     * Classifies a producer throwable into a Depot error type.
     *
     * <p>Retriable Kafka exceptions map to a retryable error, other Kafka exceptions to a non retryable error
     * and any other throwable to an unknown error.
     *
     * @param throwable the producer throwable to classify
     * @return the classified error info
     */
    public static ErrorInfo getErrorInfo(Throwable throwable) {
        Exception exception = throwable instanceof Exception ? (Exception) throwable : new RuntimeException(throwable);
        if (throwable instanceof RetriableException) {
            return new ErrorInfo(exception, ErrorType.SINK_RETRYABLE_ERROR);
        }
        if (throwable instanceof KafkaException) {
            return new ErrorInfo(exception, ErrorType.SINK_NON_RETRYABLE_ERROR);
        }
        return new ErrorInfo(exception, ErrorType.SINK_UNKNOWN_ERROR);
    }
}
