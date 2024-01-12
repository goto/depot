package com.gotocompany.depot.http.request.parser;

import com.google.gson.JsonElement;
import com.gotocompany.depot.message.ParsedMessage;

public class JsonNullParser implements JsonElementParser{
    @Override
    public String parse(JsonElement jsonElement, ParsedMessage parsedMessage) {
        return "null";
    }
}
