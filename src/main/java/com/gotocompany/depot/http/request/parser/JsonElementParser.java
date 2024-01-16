package com.gotocompany.depot.http.request.parser;

import com.google.gson.JsonElement;
import com.gotocompany.depot.message.ParsedMessage;

public interface JsonElementParser {

    String parse(JsonElement jsonElement, ParsedMessage parsedMessage);




}
