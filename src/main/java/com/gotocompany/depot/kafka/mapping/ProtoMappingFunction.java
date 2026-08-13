package com.gotocompany.depot.kafka.mapping;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

/**
 * Holds the compiled proto-to-proto mapping used by the Kafka sink for a single record.
 *
 * <p>A mapping function wraps an optional key mapper and a mandatory value mapper that transform a
 * source proto message into the sink key and value proto messages using the configured CEL expressions.
 */
public class ProtoMappingFunction {

    /**
     * Name of the CEL variable that the source proto message is bound to in every mapping expression.
     */
    public static final String SOURCE_VARIABLE_NAME = "source";

    private final ProtoMessageMapper keyMapper;
    private final ProtoMessageMapper valueMapper;

    /**
     * Creates a mapping function from the given key and value mappers.
     *
     * @param keyMapper   the mapper that builds the sink key message, or {@code null} when no key proto is configured
     * @param valueMapper the mapper that builds the sink value message
     */
    public ProtoMappingFunction(ProtoMessageMapper keyMapper, ProtoMessageMapper valueMapper) {
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
    }

    /**
     * Returns whether a key mapping is configured for the sink.
     *
     * @return {@code true} if a key mapper is present, {@code false} otherwise
     */
    public boolean hasKeyMapping() {
        return keyMapper != null;
    }

    /**
     * Maps the source message to the sink key proto message.
     *
     * @param sourceMessage the parsed source proto message
     * @return the mapped key message
     * @throws IllegalStateException if no key mapping is configured for the sink
     */
    public DynamicMessage mapKey(Message sourceMessage) {
        if (keyMapper == null) {
            throw new IllegalStateException("key mapping is not configured for the kafka sink");
        }
        return keyMapper.map(sourceMessage);
    }

    /**
     * Maps the source message to the sink value proto message.
     *
     * @param sourceMessage the parsed source proto message
     * @return the mapped value message
     */
    public DynamicMessage mapValue(Message sourceMessage) {
        return valueMapper.map(sourceMessage);
    }
}
