package com.gotocompany.depot.http.request.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.http.request.util.JsonParserUtils;
import com.gotocompany.depot.message.ParsedMessage;
import jdk.nashorn.internal.runtime.regexp.joni.ast.StringNode;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class JsonObjectParser implements JsonNodeParser {


    @Override
    public JsonNode parse(JsonNode jsonNode, ParsedMessage parsedMessage) {
        if (jsonNode.getNodeType() != JsonNodeType.OBJECT) {
            throw new IllegalArgumentException("Provided Json type is not an object");
        }
        ObjectNode objectNode = (ObjectNode) jsonNode;
        ObjectNode finalJsonObject = new JsonNodeFactory(false).objectNode();
        for (Map.Entry<String, JsonNode> entry : objectNode.properties()) {
            String keyString = entry.getKey();
            TextNode keyStringNode = new TextNode(keyString);
            JsonNode parsedKeyNode = new JsonStringParser().parse(keyStringNode, parsedMessage);
            String parsedKeyString = parsedKeyNode.toString();
            JsonNode valueNode = entry.getValue();
            JsonNodeParser jsonElementParser = JsonParserUtils.getParser(valueNode);
            JsonNode parsedValue = jsonElementParser.parse(valueNode, parsedMessage);
            finalJsonObject.put(parsedKeyString, parsedValue);
        }
        return finalJsonObject;

    }
}
