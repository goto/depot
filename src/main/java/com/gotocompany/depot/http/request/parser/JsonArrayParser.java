package com.gotocompany.depot.http.request.parser;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.gotocompany.depot.http.request.util.JsonParserUtils;
import com.gotocompany.depot.message.ParsedMessage;

public class JsonArrayParser implements JsonElementParser {


    @Override
    public JsonElement parse(JsonElement jsonElement, ParsedMessage parsedMessage) {
        JsonArray tempJsonArray = new JsonArray();
        for (JsonElement jsonElement1 : (JsonArray) jsonElement) {
            JsonElementParser jsonElementParser = JsonParserUtils.getParser(jsonElement1);

            tempJsonArray.add(jsonElementParser.parse(jsonElement, parsedMessage));
        }
        return tempJsonArray;
    }
}
