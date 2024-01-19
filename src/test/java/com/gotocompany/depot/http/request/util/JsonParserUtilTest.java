package com.gotocompany.depot.http.request.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.gotocompany.depot.http.request.parser.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JsonParserUtilTest {

    @Test
    public void shouldReturnArrayParserForJsonArray() {

        JsonElement jsonElement = JsonParser.parseString("[22,true,false]");
        JsonElementParser jsonElementParser = JsonParserUtils.getParser(jsonElement);
        assertEquals(JsonArrayParser.class, jsonElementParser.getClass());
    }

    @Test
    public void shouldReturnStringParserForJsonString() {

        JsonElement jsonElement = JsonParser.parseString("\"wwa\"");
        JsonElementParser jsonElementParser = JsonParserUtils.getParser(jsonElement);
        assertEquals(JsonStringParser.class, jsonElementParser.getClass());
    }

    @Test
    public void shouldReturnObjectParserForJsonObject() {

        JsonElement jsonElement = JsonParser.parseString("{\"aa\":22,\"qq\":true}");
        JsonElementParser jsonElementParser = JsonParserUtils.getParser(jsonElement);
        assertEquals(JsonObjectParser.class, jsonElementParser.getClass());
    }

    @Test
    public void shouldReturnPrimitiveParserForJsonPrimitive() {

        JsonElement jsonElement = JsonParser.parseString("true");
        JsonElementParser jsonElementParser = JsonParserUtils.getParser(jsonElement);
        assertEquals(JsonPrimitiveParser.class, jsonElementParser.getClass());
    }

    @Test
    public void shouldReturnNullParserForJsonNull() {

        JsonElement jsonElement = JsonParser.parseString("null");
        JsonElementParser jsonElementParser = JsonParserUtils.getParser(jsonElement);
        assertEquals(JsonNullParser.class, jsonElementParser.getClass());
    }


}
