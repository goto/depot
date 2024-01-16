package com.gotocompany.depot.http.request.util;

import com.google.gson.JsonElement;
import com.gotocompany.depot.http.request.parser.*;

public class JsonParserUtils {

    public static JsonElementParser getParser(JsonElement jsonElement) {
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
