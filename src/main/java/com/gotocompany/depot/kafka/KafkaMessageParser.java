package com.gotocompany.depot.kafka;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.stencil.client.StencilClient;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class KafkaMessageParser {

    private final StencilClient sourceStencilClient;
    private final String sourceProtoClass;

    public DynamicMessage parse(byte[] payload) throws Exception {
        if (payload == null || payload.length == 0) {
            throw new IllegalArgumentException("Empty or null message payload");
        }
        return sourceStencilClient.parse(sourceProtoClass, payload);
    }
}
