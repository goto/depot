package com.gotocompany.depot.kafka.client;

import com.gotocompany.depot.kafka.record.KafkaRecord;
import com.gotocompany.depot.kafka.response.KafkaProduceResponse;

import java.io.Closeable;
import java.util.List;

/**
 * Client abstraction that produces Kafka records and resolves their acknowledgements.
 */
public interface KafkaSinkClient extends Closeable {

    /**
     * Produces the given records and returns a response for each one.
     *
     * @param records the records to produce
     * @return the produce responses aligned by index with the input records
     */
    List<KafkaProduceResponse> send(List<KafkaRecord> records);
}
