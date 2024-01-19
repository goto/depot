package com.gotocompany.depot.http.request.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.gotocompany.depot.message.ParsedMessage;

public class JsonDefaultParser implements JsonNodeParser {

    @Override
    public JsonNode parse(JsonNode jsonElement, ParsedMessage parsedMessage) {
        return jsonElement;
    }
}
