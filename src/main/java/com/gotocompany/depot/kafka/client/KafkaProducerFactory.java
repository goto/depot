package com.gotocompany.depot.kafka.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

/**
 * Factory for creating byte array keyed and valued Kafka producers.
 */
public final class KafkaProducerFactory {

    /**
     * Prevents instantiation of this utility class.
     */
    private KafkaProducerFactory() {
    }

    /**
     * Creates a Kafka producer from the given producer properties.
     *
     * @param producerProperties the resolved producer properties
     * @return a new Kafka producer
     */
    public static Producer<byte[], byte[]> create(Properties producerProperties) {
        return new KafkaProducer<>(producerProperties);
    }
}
