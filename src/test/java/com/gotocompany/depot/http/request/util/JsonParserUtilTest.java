package com.gotocompany.depot.http.request.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JsonParserUtilTest {

    @Test
    public void shouldReturnArrayParserForJsonArray() throws JsonProcessingException {


        JsonNode jsonElement = JsonParserUtils.getObjectMapper().readTree("\"wwa\"");
        JsonParserUtils.parse(jsonElement, null);



    }

}
