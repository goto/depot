package com.gotocompany.depot.message.json;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link JsonParsedMessage}, the JSON-backed parsed-message implementation.
 *
 * <p>Each test wraps an {@code org.json} {@link JSONObject} in a {@link JsonParsedMessage} using a
 * JsonPath {@link Configuration} backed by a {@code JsonOrgJsonProvider}, then exercises field
 * lookups via {@link JsonParsedMessage#getFieldByName(String)} and JSON round-tripping via
 * {@link JsonParsedMessage#toJson()}. Coverage includes flat fields, dotted nested paths, arrays of
 * objects and the error raised for a missing field.</p>
 */
public class JsonParsedMessageTest {
    /** JsonPath configuration backed by an {@code org.json} provider, shared by the parsed messages. */
    private final Configuration configuration = Configuration.builder()
            .jsonProvider(new JsonOrgJsonProvider())
            .build();

    /**
     * Verifies that a top-level field is read from a flat JSON object.
     *
     * <p>Given a flat object with {@code first_name} and {@code address}, when
     * {@link JsonParsedMessage#getFieldByName(String)} is queried for {@code "first_name"}, then it
     * returns {@code "john doe"}.</p>
     */
    @Test
    public void shouldReturnValueFromFlatJson() {
        JSONObject personDetails = new JSONObject("{\"first_name\": \"john doe\", \"address\": \"planet earth\"}");
        JsonParsedMessage parsedMessage = new JsonParsedMessage(personDetails, configuration);
        assertEquals("john doe", parsedMessage.getFieldByName("first_name"));
    }

    /**
     * Verifies that a nested field is read using a dotted path.
     *
     * <p>Given an object with a nested {@code family} object, when
     * {@link JsonParsedMessage#getFieldByName(String)} is queried for {@code "family.brother"}, then
     * it returns {@code "david doe"}.</p>
     */
    @Test
    public void shouldReturnValueFromNestedJson() {
        JSONObject personDetails = new JSONObject(""
                + "{\"first_name\": \"john doe\","
                + " \"address\": \"planet earth\", "
                + "\"family\" : {\"brother\" : \"david doe\"}"
                + "}");
        JsonParsedMessage parsedMessage = new JsonParsedMessage(personDetails, configuration);
        assertEquals("david doe", parsedMessage.getFieldByName("family.brother"));
    }

    /**
     * Verifies that querying a missing nested field is rejected.
     *
     * <p>Given an object whose {@code family} has no {@code sister}, when
     * {@link JsonParsedMessage#getFieldByName(String)} is queried for {@code "family.sister"}, then an
     * {@link IllegalArgumentException} with the message {@code "Invalid field config : family.sister"}
     * is thrown.</p>
     */
    @Test
    public void shouldThrowExceptionIfNotFound() {
        JSONObject personDetails = new JSONObject(""
                + "{\"first_name\": \"john doe\","
                + " \"address\": \"planet earth\", "
                + "\"family\" : {\"brother\" : \"david doe\"}"
                + "}");
        JsonParsedMessage parsedMessage = new JsonParsedMessage(personDetails, configuration);
        java.lang.IllegalArgumentException illegalArgumentException = Assert.assertThrows(java.lang.IllegalArgumentException.class, () -> parsedMessage.getFieldByName("family.sister"));
        Assert.assertEquals("Invalid field config : family.sister", illegalArgumentException.getMessage());
    }

    /**
     * Verifies that a field holding an array of objects is returned as a JSON array.
     *
     * <p>Given a {@code family} array of two objects, when
     * {@link JsonParsedMessage#getFieldByName(String)} is queried for {@code "family"}, then it
     * returns a {@link JSONArray} of length two whose {@code brother} values are {@code "david doe"}
     * and {@code "cain doe"}.</p>
     */
    @Test
    public void shouldReturnListFromNestedJson() {
        JSONObject personDetails = new JSONObject(""
                + "{\"first_name\": \"john doe\","
                + " \"address\": \"planet earth\", "
                + "\"family\" : [{\"brother\" : \"david doe\"}, {\"brother\" : \"cain doe\"}]"
                + "}");
        JsonParsedMessage parsedMessage = new JsonParsedMessage(personDetails, configuration);
        JSONArray family = (JSONArray) parsedMessage.getFieldByName("family");
        Assert.assertEquals(2, family.length());
        Assert.assertEquals("david doe", ((JSONObject) family.get(0)).get("brother"));
        Assert.assertEquals("cain doe", ((JSONObject) family.get(1)).get("brother"));
    }

    /**
     * Verifies that the parsed message round-trips back to its original JSON.
     *
     * <p>Given a JSON object wrapped in a {@link JsonParsedMessage}, when
     * {@link JsonParsedMessage#toJson()} is called, then its string form equals that of the original
     * object.</p>
     */
    @Test
    public void shouldReturnJsonObjectAsItIs() {
        JSONObject jsonObject = new JSONObject(""
                + "{\"first_name\": \"john doe\","
                + " \"address\": \"planet earth\", "
                + "\"family\" : [{\"brother\" : \"david doe\"}, {\"brother\" : \"cain doe\"}]"
                + "}");
        JsonParsedMessage parsedMessage = new JsonParsedMessage(jsonObject, configuration);
        assertEquals(jsonObject.toString(), parsedMessage.toJson().toString());
    }
}
