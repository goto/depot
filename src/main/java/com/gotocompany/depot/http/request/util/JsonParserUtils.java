package com.gotocompany.depot.http.request.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gotocompany.depot.http.request.parser.*;


public class JsonParserUtils {

    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper()
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    public static JsonNodeParser getParser(JsonNode jsonNode) {


        switch (jsonNode.getNodeType()) {

            case ARRAY: {
                return new JsonArrayParser();
            }
            case OBJECT: {
                return new JsonObjectParser();
            }
            case STRING: {
                return new JsonStringParser();
            }
            case NUMBER:
            case BOOLEAN:
            case NULL:
                return new JsonDefaultParser();

            default: {
                throw new IllegalArgumentException("The provided Json type is not supported");
            }
        }

    }

    public static ObjectMapper getObjectMapper() {

        return OBJECT_MAPPER;
    }
}
