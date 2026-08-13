package com.gotocompany.depot.metrics;

import com.gotocompany.depot.config.SinkConfig;

/**
 * Provides the fully qualified metric names emitted by the Kafka sink.
 *
 * <p>Every metric is built from the application prefix, the common sink prefix and the Kafka sink prefix so
 * that all names start with {@code application_sink_kafka_}.
 */
public class KafkaSinkMetrics extends SinkMetrics {

    /**
     * Metric name prefix shared by all Kafka sink metrics.
     */
    public static final String KAFKA_SINK_PREFIX = "kafka_";
    /**
     * Tag template describing the outcome state of a schema update.
     */
    public static final String KAFKA_SCHEMA_UPDATE_STATE_TAG = "state=%s";

    /**
     * Creates the Kafka sink metrics for the given configuration.
     *
     * @param config the sink configuration providing the application metric prefix
     */
    public KafkaSinkMetrics(SinkConfig config) {
        super(config);
    }

    /**
     * Returns the metric name for the total number of records produced successfully.
     *
     * @return the success response total metric name
     */
    public String getKafkaSuccessResponseTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + KAFKA_SINK_PREFIX + "success_response_total";
    }

    /**
     * Returns the metric name for the total number of records that failed to be produced.
     *
     * @return the failure response total metric name
     */
    public String getKafkaFailureResponseTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + KAFKA_SINK_PREFIX + "failure_response_total";
    }

    /**
     * Returns the metric name for the total number of records acknowledged by the broker per batch.
     *
     * @return the messages produced total metric name
     */
    public String getKafkaMessagesProducedTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + KAFKA_SINK_PREFIX + "messages_produced_total";
    }

    /**
     * Returns the metric name for the distribution of the valid batch size pushed to the producer.
     *
     * @return the produce batch size metric name
     */
    public String getKafkaProduceBatchSizeMetric() {
        return getApplicationPrefix() + SINK_PREFIX + KAFKA_SINK_PREFIX + "produce_batch_size";
    }

    /**
     * Returns the metric name for the time taken to produce a batch of records.
     *
     * @return the produce latency metric name
     */
    public String getKafkaProduceLatencyMetric() {
        return getApplicationPrefix() + SINK_PREFIX + KAFKA_SINK_PREFIX + "produce_latency_milliseconds";
    }

    /**
     * Returns the metric name for producer errors tagged by error type.
     *
     * @return the errors total metric name
     */
    public String getKafkaErrorsTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + KAFKA_SINK_PREFIX + "errors_total";
    }

    /**
     * Returns the metric name for parsing, mapping and serialization failures tagged by error type.
     *
     * @return the record parse errors total metric name
     */
    public String getKafkaRecordParseErrorsTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + KAFKA_SINK_PREFIX + "record_parse_errors_total";
    }

    /**
     * Returns the metric name for proto mapping rebuilds tagged by their outcome state.
     *
     * @return the schema update total metric name
     */
    public String getKafkaSchemaUpdateTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + KAFKA_SINK_PREFIX + "schema_update_total";
    }

    /**
     * Returns the metric name for the gauge indicating whether large message mode is enabled.
     *
     * @return the large message mode metric name
     */
    public String getKafkaLargeMessageModeMetric() {
        return getApplicationPrefix() + SINK_PREFIX + KAFKA_SINK_PREFIX + "large_message_mode";
    }
}
