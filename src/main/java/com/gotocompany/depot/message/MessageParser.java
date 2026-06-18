package com.gotocompany.depot.message;

import java.io.IOException;

/**
 * Strategy for decoding the raw payloads of a {@link Message} into a schema-aware
 * {@link ParsedMessage}.
 *
 * <p>A {@code MessageParser} encapsulates a single serialization format. Depot ships a Protobuf
 * implementation and a JSON implementation, and the appropriate one is selected at runtime by
 * {@link MessageParserFactory} based on the sink configuration. Parsers translate the opaque key or
 * value carried by a {@link Message} into a {@link ParsedMessage} that exposes the record's fields,
 * schema and JSON form to the rest of the sink pipeline.</p>
 *
 * @see ParsedMessage
 * @see MessageParserFactory
 * @see MessageContainer
 */
public interface MessageParser {
    /**
     * Parses one portion of a record into a {@link ParsedMessage}.
     *
     * <p>The {@code type} selects whether the record's key or its value is decoded, and
     * {@code schemaClass} names the schema (for Protobuf, the fully qualified message class) the
     * payload should be interpreted against. Implementations are responsible for resolving that
     * schema, deserializing the corresponding payload and wrapping it in a format-specific
     * {@link ParsedMessage}.</p>
     *
     * @param message the record whose key or value should be parsed
     * @param type which portion of the record to parse, either
     *     {@link SinkConnectorSchemaMessageMode#LOG_KEY} or
     *     {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE}
     * @param schemaClass the schema identifier the payload should be parsed against, such as the
     *     fully qualified Protobuf class name
     * @return the parsed representation of the selected portion of the record
     * @throws IOException if the payload cannot be deserialized or the schema cannot be resolved
     */
    ParsedMessage parse(Message message, SinkConnectorSchemaMessageMode type, String schemaClass) throws IOException;

    /**
     * Refreshes any cached schema state held for the given schema class.
     *
     * <p>The default implementation does nothing. Implementations that cache resolved schemas (for
     * example Protobuf descriptors fetched from a schema registry) may override this method to
     * proactively reload the schema identified by {@code schemaClass} so that subsequent calls to
     * {@link #parse(Message, SinkConnectorSchemaMessageMode, String)} observe the latest
     * definition.</p>
     *
     * @param schemaClass the schema identifier whose cached state should be refreshed
     */
    default void refresh(String schemaClass) {

    }
}
