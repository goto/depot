package com.gotocompany.depot.http.request.parser;

import com.google.gson.JsonElement;
import com.gotocompany.depot.message.ParsedMessage;

public interface JsonElementParser {

    String parse(JsonElement jsonElement, ParsedMessage parsedMessage);


    static JsonElementParser getParser(JsonElement jsonElement) {
        if (jsonElement.isJsonArray()) {
            return new JsonArrayParser();
        }
        if (jsonElement.isJsonObject()) {
            return new JsonObjectParser();
        }
        if (jsonElement.isJsonNull()) {
            return new JsonNullParser();
        }
        if (jsonElement.isJsonPrimitive() && jsonElement.getAsJsonPrimitive().isString()) {
            return new JsonStringParser();
        }
        if (jsonElement.isJsonPrimitive()) {
            return new JsonPrimitiveParser();
        }

        throw new IllegalArgumentException("Invalid json");
    }

}
