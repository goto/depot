package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageUtils;
import com.gotocompany.depot.message.ParsedMessage;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Date;

/**
 * {@link RequestBody} that serializes both the parsed key and value of a message into a JSON envelope.
 *
 * <p>Selected for {@link com.gotocompany.depot.http.enums.HttpRequestBodyType#JSON}. The produced
 * document nests the parsed key under {@code logKey}, the parsed value under {@code logMessage}, and
 * appends any configured metadata columns as top-level fields. Metadata keys are normalized by
 * stripping the {@value #METADATA_PREFIX} prefix so they appear without the internal namespace.</p>
 *
 * @see RequestBody
 * @see com.gotocompany.depot.http.enums.HttpRequestBodyType#JSON
 */
public class JsonBody implements RequestBody {

    /**
     * Prefix stripped from metadata column names before they are placed into the JSON body.
     */
    private static final String METADATA_PREFIX = "message_";

    /**
     * Sink configuration supplying the schema classes and metadata settings.
     */
    private final HttpSinkConfig config;

    /**
     * Creates a JSON body serializer bound to the given configuration.
     *
     * @param config the HTTP sink configuration
     */
    public JsonBody(HttpSinkConfig config) {
        this.config = config;
    }

    /**
     * Serializes the message's parsed key and value plus metadata into a JSON object string.
     *
     * <p>The key and value are parsed against their configured schema classes, each rendered to JSON
     * under the {@code logKey} and {@code logMessage} fields. Configured metadata (with timestamp
     * columns rendered via {@link java.util.Date}) is then merged in, each entry keyed by its name
     * with the {@value #METADATA_PREFIX} prefix removed.</p>
     *
     * @param messageContainer the lazily-parsed view of the message
     * @return the JSON object rendered as a string
     * @throws IOException if the key or value cannot be parsed
     */
    @Override
    public String build(MessageContainer messageContainer) throws IOException {
        ParsedMessage parsedLogKey = messageContainer.getParsedLogKey(config.getSinkConnectorSchemaProtoKeyClass());
        ParsedMessage parsedLogMessage = messageContainer.getParsedLogMessage(config.getSinkConnectorSchemaProtoMessageClass());
        JSONObject payload = new JSONObject();
        payload.put("logKey", buildJsonMessage(parsedLogKey));
        payload.put("logMessage", buildJsonMessage(parsedLogMessage));
        MessageUtils.getMetaData(messageContainer.getMessage(), config, Date::new)
                .forEach((key, value) -> payload.put(removePrefixMetadata(key), value));
        return payload.toString();
    }

    /**
     * Renders a parsed message portion to its JSON string form.
     *
     * @param parsedMessage the parsed key or value to render
     * @return the JSON representation of {@code parsedMessage} as a string
     */
    private String buildJsonMessage(ParsedMessage parsedMessage) {
        JSONObject jsonMessage = parsedMessage.toJson();
        return jsonMessage.toString();
    }

    /**
     * Strips the {@value #METADATA_PREFIX} prefix from a metadata field name if present.
     *
     * @param metadataFieldName the raw metadata column name
     * @return the name with the leading {@value #METADATA_PREFIX} removed, or unchanged if absent
     */
    private String removePrefixMetadata(String metadataFieldName) {
        return StringUtils.removeStart(metadataFieldName, METADATA_PREFIX);
    }
}
