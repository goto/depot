package com.gotocompany.depot.http.request.body;

import com.fasterxml.jackson.databind.JsonNode;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.http.request.util.JsonParserUtils;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;

import java.io.IOException;

/**
 * {@link RequestBody} that renders a user-supplied JSON template populated with message fields.
 *
 * <p>Selected for {@link com.gotocompany.depot.http.enums.HttpRequestBodyType#TEMPLATIZED_JSON}. The
 * template configured under {@code SINK_HTTPV2_JSON_BODY_TEMPLATE} is parsed once into a reusable
 * {@link JsonNode} tree at construction time; at publish time the placeholders in that tree are
 * substituted with values from the parsed message by
 * {@link JsonParserUtils#parse(JsonNode, com.gotocompany.depot.message.ParsedMessage)}. This gives
 * full control over the body shape, in contrast to the fixed envelopes of the other body types.</p>
 *
 * <p>The portion of the message used for substitution depends on the configured schema message mode:
 * the parsed key in {@link com.gotocompany.depot.message.SinkConnectorSchemaMessageMode#LOG_KEY} mode,
 * otherwise the parsed value.</p>
 *
 * @see RequestBody
 * @see JsonParserUtils
 * @see com.gotocompany.depot.http.enums.HttpRequestBodyType#TEMPLATIZED_JSON
 */
public class TemplatizedJsonBody implements RequestBody {
    /**
     * Parsed, reusable JSON template tree whose placeholders are substituted per message.
     */
    private final JsonNode templateJsonNode;
    /**
     * Sink configuration supplying the template string, schema mode and schema classes.
     */
    private final HttpSinkConfig config;

    /**
     * Creates a templatized JSON body serializer, parsing the configured template eagerly.
     *
     * @param config the HTTP sink configuration supplying the JSON body template
     * @throws com.gotocompany.depot.exception.ConfigurationException if the configured template is
     *     empty or is not valid JSON
     */
    public TemplatizedJsonBody(HttpSinkConfig config) {
        this.config = config;
        this.templateJsonNode = JsonParserUtils.createJsonNode(config.getSinkHttpJsonBodyTemplate());
    }

    /**
     * Substitutes message fields into the parsed template and returns the resulting JSON string.
     *
     * <p>The parsed message portion (key or value, per the configured schema message mode) is used to
     * resolve the template placeholders, and the populated tree is rendered to a string.</p>
     *
     * @param msgContainer the lazily-parsed view of the message
     * @return the populated JSON template rendered as a string
     * @throws IOException if the selected message portion cannot be parsed
     */
    @Override
    public String build(MessageContainer msgContainer) throws IOException {
        ParsedMessage parsedMessage;
        if (config.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_KEY) {
            parsedMessage = msgContainer.getParsedLogKey(config.getSinkConnectorSchemaProtoKeyClass());
        } else {
            parsedMessage = msgContainer.getParsedLogMessage(config.getSinkConnectorSchemaProtoMessageClass());
        }
        JsonNode parsedJsonNode = JsonParserUtils.parse(templateJsonNode, parsedMessage);
        return parsedJsonNode.toString();
    }
}
