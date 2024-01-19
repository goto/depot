package com.gotocompany.depot.http.request.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.TextNode;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.http.request.util.JsonParserUtils;
import com.gotocompany.depot.message.ParsedMessage;

public class JsonStringParser implements JsonNodeParser {

    @Override
    public JsonNode parse(JsonNode jsonNode, ParsedMessage parsedMessage) {

        if (jsonNode.getNodeType() != JsonNodeType.STRING) {
            throw new IllegalArgumentException("Provided Json type is not a String");
        }
        TextNode textNode = (TextNode) jsonNode;
        Template templateValue;
        try {
            templateValue = new Template(textNode.asText());
        } catch (InvalidTemplateException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        Object parsedValue = templateValue.parseWithType(parsedMessage);
        if (parsedValue instanceof String) {
            parsedValue = "\"" + parsedValue + "\"";
        }

        ObjectMapper objectMapper = JsonParserUtils.getObjectMapper();
        String parsedJsonString = parsedValue.toString();

        JsonNode parsedJsonNode;
        try {
            parsedJsonNode = objectMapper.readTree(parsedJsonString);
        } catch (JsonProcessingException e) {
            throw new ConfigurationException("An error occurred while parsing the template string : " + parsedJsonString + "\nError: " + e.getMessage());
        }
        return parsedJsonNode;
    }
}
