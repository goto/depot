package com.gotocompany.depot.http.request.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.ParsedMessage;

public class JsonStringParser implements JsonElementParser {
    @Override
    public JsonElement parse(JsonElement jsonElement, ParsedMessage parsedMessage) {
        Template templateValue = null;
        try {
            templateValue = new Template(jsonElement.getAsJsonPrimitive().getAsString());
        } catch (InvalidTemplateException e) {
            e.printStackTrace();
        }
        Object parsedValue = templateValue.parseWithType(parsedMessage);
        if (parsedValue instanceof String) parsedValue = "\"" + parsedValue + "\"";
        return new JsonParser().parse(parsedValue.toString());
    }
}
