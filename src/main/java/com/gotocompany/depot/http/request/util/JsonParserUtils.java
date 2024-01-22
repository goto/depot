package com.gotocompany.depot.http.request.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gotocompany.depot.http.request.parser.*;


public class JsonParserUtils {

    private static final ObjectMapper OBJECT_MAPPER;
    private static final JsonArrayParser JSON_ARRAY_PARSER;
    private static final JsonDefaultParser JSON_DEFAULT_PARSER;
    private static final JsonObjectParser JSON_OBJECT_PARSER;
    private static final JsonStringParser JSON_STRING_PARSER;


    static {
        OBJECT_MAPPER = new ObjectMapper()
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        JSON_ARRAY_PARSER = new JsonArrayParser();
        JSON_DEFAULT_PARSER = new JsonDefaultParser();
        JSON_STRING_PARSER = new JsonStringParser();
        JSON_OBJECT_PARSER = new JsonObjectParser();
    }

    public static JsonNodeParser getParser(JsonNode jsonNode) {


        switch (jsonNode.getNodeType()) {

            case ARRAY: {
                return JSON_ARRAY_PARSER;
            }
            case OBJECT: {
                return JSON_OBJECT_PARSER;
            }
            case STRING: {
                return JSON_STRING_PARSER;
            }
            case NUMBER:
            case BOOLEAN:
            case NULL:
                return JSON_DEFAULT_PARSER;

            default: {
                throw new IllegalArgumentException("The provided Json type is not supported");
            }
        }

    }

    public static ObjectMapper getObjectMapper() {

        return OBJECT_MAPPER;
    }
}
