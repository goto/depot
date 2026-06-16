package com.gotocompany.depot.metrics;

import com.gotocompany.depot.config.SinkConfig;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class KafkaSinkMetricsTest {

    @Test
    public void shouldReturnKafkaSinkMetricNames() {
        SinkConfig config = ConfigFactory.create(SinkConfig.class, new HashMap<String, String>());
        KafkaSinkMetrics metrics = new KafkaSinkMetrics(config);
        assertEquals("application_sink_kafka_success_response_total", metrics.getKafkaSuccessResponseTotalMetric());
        assertEquals("application_sink_kafka_failure_response_total", metrics.getKafkaFailureResponseTotalMetric());
        assertEquals("application_sink_kafka_messages_produced_total", metrics.getKafkaMessagesProducedTotalMetric());
        assertEquals("application_sink_kafka_produce_batch_size", metrics.getKafkaProduceBatchSizeMetric());
        assertEquals("application_sink_kafka_produce_latency_milliseconds", metrics.getKafkaProduceLatencyMetric());
        assertEquals("application_sink_kafka_errors_total", metrics.getKafkaErrorsTotalMetric());
        assertEquals("application_sink_kafka_record_parse_errors_total", metrics.getKafkaRecordParseErrorsTotalMetric());
        assertEquals("application_sink_kafka_schema_update_total", metrics.getKafkaSchemaUpdateTotalMetric());
        assertEquals("application_sink_kafka_large_message_mode", metrics.getKafkaLargeMessageModeMetric());
    }
}
