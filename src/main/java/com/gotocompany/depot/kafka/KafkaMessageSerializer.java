package com.gotocompany.depot.kafka;

import com.google.protobuf.DynamicMessage;

public class KafkaMessageSerializer {

    public byte[] serialize(DynamicMessage message) {
        if (message == null) {
            return new byte[0];
        }
        return message.toByteArray();
    }
}
