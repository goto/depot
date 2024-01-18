package com.gotocompany.depot.http.request.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.gotocompany.depot.exception.ConfigurationException;
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

    public static void validateJson(String jsonString) throws JsonProcessingException {

        new ObjectMapper()
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .readTree(jsonString);

    }
}
