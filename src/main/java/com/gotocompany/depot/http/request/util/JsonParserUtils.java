package com.gotocompany.depot.http.request.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.ParsedMessage;

import java.util.Iterator;
import java.util.Map;


public class JsonParserUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);


    public static JsonNode parse(JsonNode jsonNode, ParsedMessage parsedMessage) {


        switch (jsonNode.getNodeType()) {

            case ARRAY: {
                return parseInternal((ArrayNode) jsonNode, parsedMessage);
            }
            case OBJECT: {
                return parseInternal((ObjectNode) jsonNode, parsedMessage);
            }
            case STRING: {
                return parseInternal((TextNode) jsonNode, parsedMessage);
            }
            case NUMBER:
            case BOOLEAN:
            case NULL: {
                return parseInternal(jsonNode, parsedMessage);
            }

            default: {
                throw new IllegalArgumentException("The provided Json type is not supported");
            }
        }

    }

    public static JsonNode parseInternal(ObjectNode objectNode, ParsedMessage parsedMessage) {

        ObjectNode finalJsonObject = new JsonNodeFactory(false).objectNode();
        for (Map.Entry<String, JsonNode> entry : objectNode.properties()) {
            String keyString = entry.getKey();
            TextNode keyStringNode = new TextNode(keyString);
            JsonNode parsedKeyNode = parseInternal(keyStringNode, parsedMessage);
            String parsedKeyString = parsedKeyNode.toString();
            JsonNode valueNode = entry.getValue();
            JsonNode parsedValue = JsonParserUtils.parse(valueNode, parsedMessage);
            finalJsonObject.put(parsedKeyString, parsedValue);
        }
        return finalJsonObject;

    }


    public static JsonNode parseInternal(ArrayNode arrayNode, ParsedMessage parsedMessage) {
        ArrayNode tempJsonArray = new JsonNodeFactory(false).arrayNode();
        for (Iterator<JsonNode> it = arrayNode.elements(); it.hasNext(); ) {
            JsonNode jsonElement1 = it.next();
            JsonNode parsedJsonNode = JsonParserUtils.parse(jsonElement1, parsedMessage);

            tempJsonArray.add(parsedJsonNode);
        }
        return tempJsonArray;
    }

    public static JsonNode parseInternal(TextNode textNode, ParsedMessage parsedMessage) {

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

    public static JsonNode parseInternal(JsonNode jsonElement, ParsedMessage parsedMessage) {
        return jsonElement;
    }


    public static ObjectMapper getObjectMapper() {

        return OBJECT_MAPPER;
    }
}
