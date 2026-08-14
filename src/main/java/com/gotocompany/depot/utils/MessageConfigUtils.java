package com.gotocompany.depot.utils;

import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.common.Tuple;

/**
 * Helper that resolves the schema message mode and the matching schema class from sink configuration.
 *
 * <p>Several sinks need the same pair of values before they can parse records: which portion of a
 * record carries the schema (its key or its value) and the schema class that portion should be parsed
 * against. This helper derives both from a {@link SinkConfig} so the logic is not duplicated across
 * sinks.</p>
 */
public class MessageConfigUtils {

    /**
     * Resolves the configured schema message mode together with the schema class it applies to.
     *
     * <p>Reads {@link SinkConfig#getSinkConnectorSchemaMessageMode()} and selects the schema class
     * accordingly: the Protobuf message class when the mode is
     * {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE}, otherwise the Protobuf key class. The two
     * values are returned together as a {@link Tuple}.</p>
     *
     * @param sinkConfig the configuration to read the mode and schema classes from
     * @return a {@link Tuple} whose first element is the {@link SinkConnectorSchemaMessageMode} and
     *     whose second element is the fully qualified schema class name for that mode
     */
    public static Tuple<SinkConnectorSchemaMessageMode, String> getModeAndSchema(SinkConfig sinkConfig) {
        SinkConnectorSchemaMessageMode mode = sinkConfig.getSinkConnectorSchemaMessageMode();
        String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
        return new Tuple<>(mode, schemaClass);
    }
}
