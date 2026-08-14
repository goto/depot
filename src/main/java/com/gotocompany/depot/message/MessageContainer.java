package com.gotocompany.depot.message;

import java.io.IOException;

/**
 * Lazily parses and caches the key and value portions of a single {@link Message}.
 *
 * <p>A {@code MessageContainer} pairs a raw {@link Message} with the {@link MessageParser} able to
 * decode it and memoizes the resulting {@link ParsedMessage} views. Parsing is performed on first
 * access and the outcome is retained, so repeated reads of the same portion avoid redundant work.
 * This is convenient when both the schema-derivation and record-conversion stages of a sink need the
 * parsed form of the same record.</p>
 *
 * <p>The two parsed portions correspond to the {@link SinkConnectorSchemaMessageMode#LOG_KEY} and
 * {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE} modes. Because the parsed value is cached on
 * first use, the schema class passed to a later invocation has no effect once a portion has already
 * been parsed.</p>
 *
 * <p>This class performs unsynchronized lazy initialization and is therefore not thread-safe; a
 * single container instance should not be shared across threads without external
 * synchronization.</p>
 *
 * @see Message
 * @see MessageParser
 * @see ParsedMessage
 */
public class MessageContainer {
    /**
     * Raw record whose key and value portions are parsed on demand.
     */
    private final Message message;

    /**
     * Parser used to decode the record's key and value payloads.
     */
    private final MessageParser parser;
    /**
     * Cached result of parsing the record's key, or {@code null} until
     * {@link #getParsedLogKey(String)} is first invoked.
     */
    private ParsedMessage parsedLogKey = null;
    /**
     * Cached result of parsing the record's value, or {@code null} until
     * {@link #getParsedLogMessage(String)} is first invoked.
     */
    private ParsedMessage parsedLogMessage = null;

    /**
     * Creates a container that will parse the given message with the supplied parser on demand.
     *
     * @param message the raw record to wrap; its key and value are parsed lazily
     * @param parser the parser used to decode the message's key and value payloads
     */
    public MessageContainer(Message message, MessageParser parser) {
        this.message = message;
        this.parser = parser;
    }

    /**
     * Returns the parsed view of the record's key, parsing and caching it on first access.
     *
     * <p>On the first invocation the wrapped {@link MessageParser} is asked to parse the message in
     * {@link SinkConnectorSchemaMessageMode#LOG_KEY} mode against the supplied schema class, and the
     * result is cached. Subsequent invocations return the cached value and ignore the
     * {@code schemaProtoKeyClass} argument.</p>
     *
     * @param schemaProtoKeyClass the fully qualified schema (Protobuf) class name describing the key,
     *     used only when the key has not yet been parsed
     * @return the parsed representation of the record's key
     * @throws IOException if the parser fails to decode the key payload
     */
    public ParsedMessage getParsedLogKey(String schemaProtoKeyClass) throws IOException {
        if (parsedLogKey == null) {
            parsedLogKey = parser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, schemaProtoKeyClass);
        }
        return parsedLogKey;
    }

    /**
     * Returns the parsed view of the record's value, parsing and caching it on first access.
     *
     * <p>On the first invocation the wrapped {@link MessageParser} is asked to parse the message in
     * {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE} mode against the supplied schema class, and
     * the result is cached. Subsequent invocations return the cached value and ignore the
     * {@code schemaProtoMessageClass} argument.</p>
     *
     * @param schemaProtoMessageClass the fully qualified schema (Protobuf) class name describing the
     *     value, used only when the value has not yet been parsed
     * @return the parsed representation of the record's value
     * @throws IOException if the parser fails to decode the value payload
     */
    public ParsedMessage getParsedLogMessage(String schemaProtoMessageClass) throws IOException {
        if (parsedLogMessage == null) {
            parsedLogMessage = parser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaProtoMessageClass);
        }
        return parsedLogMessage;
    }

    /**
     * Returns the raw, unparsed message backing this container.
     *
     * @return the wrapped {@link Message}
     */
    public Message getMessage() {
        return message;
    }
}
