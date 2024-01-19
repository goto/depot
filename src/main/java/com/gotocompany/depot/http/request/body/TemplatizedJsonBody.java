package com.gotocompany.depot.http.request.body;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import com.google.gson.TypeAdapter;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.http.request.parser.JsonElementParser;
import com.gotocompany.depot.http.request.util.JsonParserUtils;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;

import java.io.IOException;

public class TemplatizedJsonBody implements RequestBody {
    public static final TypeAdapter<JsonElement> JSON_ADAPTER = new Gson().getAdapter(JsonElement.class);
    private final JsonElement jsonElement;
    private final HttpSinkConfig config;

    public TemplatizedJsonBody(HttpSinkConfig config) {
        this.config = config;
        this.jsonElement = createJsonElement(config.getSinkHttpJsonBodyTemplate());
    }

    @Override
    public String build(MessageContainer msgContainer) throws IOException {
        ParsedMessage parsedMessage;
        if (config.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_KEY) {
            parsedMessage = msgContainer.getParsedLogKey(config.getSinkConnectorSchemaProtoKeyClass());
        } else {
            parsedMessage = msgContainer.getParsedLogMessage(config.getSinkConnectorSchemaProtoMessageClass());
        }
        JsonElementParser jsonElementParser = JsonParserUtils.getParser(jsonElement);
        return jsonElementParser.parse(jsonElement, parsedMessage).toString();
    }

    private JsonElement createJsonElement(String jsonTemplate) {
        if (jsonTemplate.isEmpty()) {
            throw new ConfigurationException("Json body template cannot be empty");
        }
        try {
            return JSON_ADAPTER.fromJson(jsonTemplate);
        } catch (JsonSyntaxException | IOException e) {
            throw new ConfigurationException(String.format("Json body template is not a valid json. %s", e.getMessage()));
        }
    }
}
