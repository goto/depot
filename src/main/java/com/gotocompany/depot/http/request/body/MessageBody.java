package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;

import java.io.IOException;

/**
 * {@link RequestBody} that serializes a single parsed message portion directly to JSON.
 *
 * <p>Selected for {@link com.gotocompany.depot.http.enums.HttpRequestBodyType#MESSAGE}. Unlike
 * {@link JsonBody}, it emits no enclosing envelope or metadata: just the JSON form of either the
 * parsed key or the parsed value, chosen according to the configured schema message mode.</p>
 *
 * @see RequestBody
 * @see com.gotocompany.depot.http.enums.HttpRequestBodyType#MESSAGE
 */
public class MessageBody implements RequestBody {

    /**
     * Sink configuration supplying the schema message mode and schema classes.
     */
    private final HttpSinkConfig config;

    /**
     * Creates a message body serializer bound to the given configuration.
     *
     * @param config the HTTP sink configuration
     */
    public MessageBody(HttpSinkConfig config) {
        this.config = config;
    }

    /**
     * Serializes the selected message portion to its JSON string form.
     *
     * <p>The parsed key is used in
     * {@link com.gotocompany.depot.message.SinkConnectorSchemaMessageMode#LOG_KEY} mode; otherwise the
     * parsed value is used. The chosen portion is rendered directly to JSON without metadata.</p>
     *
     * @param messageContainer the lazily-parsed view of the message
     * @return the JSON representation of the selected portion as a string
     * @throws IOException if the selected message portion cannot be parsed
     */
    @Override
    public String build(MessageContainer messageContainer) throws IOException {
        ParsedMessage parsedMessage;
        if (config.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_KEY) {
            parsedMessage = messageContainer.getParsedLogKey(config.getSinkConnectorSchemaProtoKeyClass());
        } else {
            parsedMessage = messageContainer.getParsedLogMessage(config.getSinkConnectorSchemaProtoMessageClass());
        }
        return parsedMessage.toJson().toString();
    }
}
