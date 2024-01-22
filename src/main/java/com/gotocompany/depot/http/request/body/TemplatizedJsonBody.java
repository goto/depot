package com.gotocompany.depot.http.request.body;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonSyntaxException;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.http.request.util.JsonParserUtils;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;

import java.io.IOException;

public class TemplatizedJsonBody implements RequestBody {
    private final JsonNode templateJsonNode;
    private final HttpSinkConfig config;

    public TemplatizedJsonBody(HttpSinkConfig config) {
        this.config = config;
        this.templateJsonNode = createJsonNode(config.getSinkHttpJsonBodyTemplate());
    }

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


    private JsonNode createJsonNode(String jsonTemplate) {
        if (jsonTemplate.isEmpty()) {
            throw new ConfigurationException("Json body template cannot be empty");
        }
        try {
            return JsonParserUtils.getObjectMapper().readTree(jsonTemplate);

        } catch (JsonSyntaxException | IOException e) {
            throw new ConfigurationException(String.format("Json body template is not a valid json. %s", e.getMessage()));
        }
    }
}
