package com.gotocompany.depot.http.request.body;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.http.request.parser.JsonElementParser;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Set;

public class TemplatizedJsonBody implements RequestBody {

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
        JsonElementParser jsonElementParser = JsonElementParser.getParser(jsonElement);
        return jsonElementParser.parse(jsonElement, parsedMessage);
    }

    private JsonElement createJsonElement(String jsonTemplate) {
        if (jsonTemplate.isEmpty()) {
            throw new ConfigurationException("Json body template cannot be empty");
        }
        try {
            return JsonParser.parseString(jsonTemplate);
        } catch (JSONException e) {
            throw new ConfigurationException(String.format("Json body template is not a valid json. %s", e.getMessage()));
        }
    }
}