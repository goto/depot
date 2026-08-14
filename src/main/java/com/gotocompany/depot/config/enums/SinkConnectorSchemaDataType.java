package com.gotocompany.depot.config.enums;

/**
 * Enumerates the wire formats that a Depot sink connector can consume for incoming messages.
 *
 * <p>The active value is selected by the {@code SINK_CONNECTOR_SCHEMA_DATA_TYPE} configuration
 * property (see {@link com.gotocompany.depot.config.SinkConfig#getSinkConnectorSchemaDataType()},
 * which defaults to {@link #PROTOBUF}). It determines how raw record payloads are interpreted and
 * therefore which message parser implementation Depot uses to deserialize keys and values before
 * they are converted into sink-specific records.
 */
public enum SinkConnectorSchemaDataType {
    /**
     * Indicates that messages are encoded using Protocol Buffers and should be parsed against a
     * Protobuf schema, resolved from the configured message class or the Stencil schema registry.
     */
    PROTOBUF,
    /**
     * Indicates that messages are encoded as JSON and should be parsed using Depot's JSON message
     * parser.
     */
    JSON
}
