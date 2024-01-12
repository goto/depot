package com.gotocompany.depot.http.request.parser;

import com.google.gson.JsonElement;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.message.ParsedMessage;

public class JsonStringParser implements JsonElementParser {
    @Override
    public JsonElement parse(JsonElement jsonElement, ParsedMessage parsedMessage) {
        Template templateValue = new Template((String) object);
        return templateValue.parseWithType(parsedMessage);
    }
}
