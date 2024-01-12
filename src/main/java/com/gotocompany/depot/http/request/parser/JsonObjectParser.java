package com.gotocompany.depot.http.request.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.ParsedMessage;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Set;

public class JsonObjectParser implements JsonElementParser {


    @Override
    public String parse(JsonElement jsonElement, ParsedMessage parsedMessage) {
        return parseInternal(jsonElement, parsedMessage).toString();
    }


    private JsonObject parseInternal(JsonElement jsonElement, ParsedMessage parsedMessage) {
        try {
            Set<String> keys = object.keySet();
            JSONObject finalJsonObject = new JSONObject();
            for (String key : keys) {
                Object value = object.get(key);
                Template templateKey = new Template(key);
                Object parsedKey = templateKey.parseWithType(parsedMessage);
                if (value instanceof JSONObject) {
                    finalJsonObject.put(parsedKey.toString(), parse((JSONObject) value, parsedMessage));
                } else if (value instanceof JSONArray) {
                    JSONArray tempJsonArray = parseJsonArray((JSONArray) value, parsedMessage);
                    finalJsonObject.put(parsedKey.toString(), tempJsonArray);
                } else if (value instanceof String) {
                    Template templateValue = new Template((String) value);
                    finalJsonObject.put(parsedKey.toString(), templateValue.parseWithType(parsedMessage));
                } else {
                    finalJsonObject.put(parsedKey.toString(), value);
                }
            }
            return finalJsonObject;
        } catch (InvalidTemplateException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }
}
