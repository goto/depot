package com.gotocompany.depot.http.request.parser;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.message.ParsedMessage;
import org.json.JSONArray;
import org.json.JSONObject;

public class JsonArrayParser implements JsonElementParser {


    @Override
    public JsonElement parse(JsonElement jsonElement, ParsedMessage parsedMessage) {
        JsonArray tempJsonArray = new JsonArray();
        for (int i = 0; i < jsonArray.length(); i++) {
            Object object = jsonArray.get(i);
            if (object instanceof JSONObject) {
                tempJsonArray.put(parse((JSONObject) object, parsedMessage));
            } else if (object instanceof JSONArray) {
                tempJsonArray.put(parseJsonArray((JSONArray) object, parsedMessage));
            } else if (object instanceof String) {
                Template templateValue = new Template((String) object);
                tempJsonArray.put(templateValue.parseWithType(parsedMessage));
            } else {
                tempJsonArray.put(object);
            }
        }
        return tempJsonArray;
    }
}
