package com.gotocompany.depot.message;

/**
 * Selects which portion of a record a sink should treat as its schema-bearing payload.
 *
 * <p>A Depot record carries two payloads: a key and a value (the "log message"). This mode tells the
 * parsing and schema-derivation stages which of the two to operate on, and it is configured per sink
 * through the {@code SINK_CONNECTOR_SCHEMA_MESSAGE_MODE} property. It is also passed to
 * {@link MessageParser#parse} to choose the payload to decode.</p>
 *
 * @see MessageParser
 * @see MessageContainer
 */
public enum SinkConnectorSchemaMessageMode {
    /**
     * Use the record's key payload.
     */
    LOG_KEY,

    /**
     * Use the record's value (message) payload.
     */
    LOG_MESSAGE
}
