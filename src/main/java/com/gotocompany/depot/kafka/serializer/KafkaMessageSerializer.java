package com.gotocompany.depot.kafka.serializer;

import com.google.protobuf.Message;

/**
 * Serializes a mapped proto message into the bytes produced to Kafka.
 */
public interface KafkaMessageSerializer {

    /**
     * Serializes the given proto message into bytes.
     *
     * @param message the proto message to serialize
     * @return the serialized bytes
     */
    byte[] serialize(Message message);
}
