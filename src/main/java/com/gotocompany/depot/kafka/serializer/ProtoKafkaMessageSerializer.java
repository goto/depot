package com.gotocompany.depot.kafka.serializer;

import com.google.protobuf.Message;
import com.gotocompany.depot.exception.DeserializerException;

/**
 * {@link KafkaMessageSerializer} that serializes a proto message using its native binary encoding.
 */
public class ProtoKafkaMessageSerializer implements KafkaMessageSerializer {

    /**
     * Serializes the proto message to its binary form.
     *
     * @param message the proto message to serialize
     * @return the serialized bytes
     * @throws DeserializerException if the message is null
     */
    @Override
    public byte[] serialize(Message message) {
        if (message == null) {
            throw new DeserializerException("the proto message to be serialized can not be null");
        }
        return message.toByteArray();
    }
}
