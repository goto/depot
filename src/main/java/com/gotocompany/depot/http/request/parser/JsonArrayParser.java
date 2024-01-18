package com.gotocompany.depot.http.request.parser;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.gotocompany.depot.http.request.util.JsonParserUtils;
import com.gotocompany.depot.message.ParsedMessage;

import java.util.List;

public class JsonArrayParser implements JsonElementParser {


    @Override
    public JsonElement parse(JsonElement jsonElement, ParsedMessage parsedMessage) {
        JsonArray tempJsonArray = new JsonArray();
        List<JsonElement> jsonElementList = jsonElement.getAsJsonArray().asList();
        for (JsonElement jsonElement1 : jsonElementList) {
            JsonElementParser jsonElementParser = JsonParserUtils.getParser(jsonElement1);

            tempJsonArray.add(jsonElementParser.parse(jsonElement1, parsedMessage));
        }
        return tempJsonArray;
    }
}
