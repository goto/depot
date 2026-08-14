package com.gotocompany.depot.message.json;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.utils.JsonUtils;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.message.MessageUtils;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.JsonParserMetrics;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;

/**
 * {@link MessageParser} implementation that parses JSON-encoded payloads into {@link JsonParsedMessage}
 * instances.
 *
 * <p>Depending on the requested mode it reads either the log key or the log message bytes, builds a
 * {@link JSONObject} from them through {@link JsonUtils} (honouring the sink's string-mode
 * configuration) and wraps the result together with a JSONPath {@link Configuration} backed by a
 * {@link JsonOrgJsonProvider}. The time taken to parse each payload is recorded through the supplied
 * {@link Instrumentation}.</p>
 *
 * <p>The class is annotated with Lombok's {@code @Slf4j} and logs empty payloads at info level.</p>
 *
 * @see MessageParser
 * @see JsonParsedMessage
 */
@Slf4j
public class JsonMessageParser implements MessageParser {

    /**
     * Sink configuration controlling JSON parsing behaviour, such as string mode.
     */
    private final SinkConfig config;
    /**
     * Instrumentation used to record JSON parsing metrics.
     */
    private final Instrumentation instrumentation;
    /**
     * Metric definitions for JSON parsing, such as the parse-duration metric.
     */
    private final JsonParserMetrics jsonParserMetrics;
    /**
     * JSONPath configuration backed by a {@link JsonOrgJsonProvider}, propagated to each parsed
     * message for field-by-name lookups.
     */
    private final Configuration jsonPathConfig = Configuration.builder()
            .jsonProvider(new JsonOrgJsonProvider())
            .build();

    /**
     * Creates a JSON message parser with the given configuration, instrumentation and metrics.
     *
     * @param config the sink configuration controlling JSON parsing behaviour
     * @param instrumentation the instrumentation used to record parsing metrics
     * @param jsonParserMetrics the metric definitions for JSON parsing
     */
    public JsonMessageParser(SinkConfig config, Instrumentation instrumentation, JsonParserMetrics jsonParserMetrics) {
        this.instrumentation = instrumentation;
        this.jsonParserMetrics = jsonParserMetrics;
        this.config = config;

    }

    /**
     * Parses the configured portion of a {@link Message} into a {@link JsonParsedMessage}.
     *
     * <p>The {@code type} selects whether the log key or the log message bytes are parsed. The payload
     * is validated to be a {@code byte[]} and checked for emptiness, then decoded into a
     * {@link JSONObject} (with the parse duration recorded through the instrumentation) and wrapped in
     * a {@link JsonParsedMessage}.</p>
     *
     * @param message the inbound message carrying the raw log key and log message bytes
     * @param type whether to parse the {@link SinkConnectorSchemaMessageMode#LOG_KEY} or the
     *     {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE} payload
     * @param schemaClass the schema class name; unused by JSON parsing but part of the
     *     {@link MessageParser} contract
     * @return a {@link JsonParsedMessage} wrapping the parsed JSON object
     * @throws IOException if {@code type} is {@code null}, the payload is not a {@code byte[]}, or the
     *     bytes are not valid JSON
     * @throws ConfigurationException if {@code type} is not a supported schema mode
     * @throws EmptyMessageException if the selected payload is {@code null} or empty
     */
    @Override
    public ParsedMessage parse(Message message, SinkConnectorSchemaMessageMode type, String schemaClass) throws IOException {
        if (type == null) {
            throw new IOException("message mode not defined");
        }
        MessageUtils.validate(message, byte[].class);
        byte[] payload;
        switch (type) {
            case LOG_KEY:
                payload = (byte[]) message.getLogKey();
                break;
            case LOG_MESSAGE:
                payload = (byte[]) message.getLogMessage();
                break;
            default:
                throw new ConfigurationException("Schema type not supported");
        }
        try {
            if (payload == null || payload.length == 0) {
                log.info("empty message found {}", message.getMetadataString());
                throw new EmptyMessageException();
            }
            Instant instant = Instant.now();
            JSONObject jsonObject = JsonUtils.getJsonObject(config, payload);
            instrumentation.captureDurationSince(jsonParserMetrics.getJsonParseTimeTakenMetric(), instant);
            return new JsonParsedMessage(jsonObject, jsonPathConfig);
        } catch (JSONException ex) {
            throw new IOException("invalid json error", ex);
        }
    }

}
