package com.gotocompany.depot.http.request.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.http.request.util.JsonParserUtils;
import com.gotocompany.depot.message.ParsedMessage;

import java.util.Set;

public class JsonObjectParser implements JsonElementParser {


    @Override
    public JsonElement parse(JsonElement object, ParsedMessage parsedMessage) {
        try {
            Set<String> keys = ((JsonObject) object).keySet();
            JsonObject finalJsonObject = new JsonObject();
            for (String key : keys) {
                JsonElement value = ((JsonObject) object).get(key);
                Template templateKey = new Template(key);
                Object parsedKey = templateKey.parseWithType(parsedMessage);
                JsonElementParser jsonElementParser = JsonParserUtils.getParser(value);
                JsonElement parsedValue = jsonElementParser.parse(value, parsedMessage);
                finalJsonObject.add(parsedKey.toString(), parsedValue);
            }
            return finalJsonObject;
        } catch (InvalidTemplateException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }
}
