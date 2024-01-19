package com.gotocompany.depot.http.request.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonElement;
import com.gotocompany.depot.message.ParsedMessage;

public interface JsonNodeParser {

    JsonNode parse(JsonNode jsonElement, ParsedMessage parsedMessage);


}
