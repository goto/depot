package com.gotocompany.depot.http.request.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.gotocompany.depot.http.request.util.JsonParserUtils;
import com.gotocompany.depot.message.ParsedMessage;

import java.util.Iterator;
import java.util.List;

public class JsonArrayParser implements JsonNodeParser {


    @Override
    public JsonNode parse(JsonNode jsonElement, ParsedMessage parsedMessage) {
        if (jsonElement.getNodeType() != JsonNodeType.ARRAY) {
            throw new IllegalArgumentException("Provided Json type is not an array");
        }
        ArrayNode arrayNode = (ArrayNode) jsonElement;
        ArrayNode tempJsonArray = new JsonNodeFactory(false).arrayNode();
        for (Iterator<JsonNode> it = arrayNode.elements(); it.hasNext(); ) {
            JsonNode jsonElement1 = it.next();
            JsonNodeParser jsonElementParser = JsonParserUtils.getParser(jsonElement1);

            tempJsonArray.add(jsonElementParser.parse(jsonElement1, parsedMessage));
        }
        return tempJsonArray;
    }
}
