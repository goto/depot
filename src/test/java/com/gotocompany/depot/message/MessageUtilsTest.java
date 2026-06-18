package com.gotocompany.depot.message;

import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.SinkConfig;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


/**
 * Unit tests for {@link MessageUtils}, the helper methods for reading fields from JSON payloads and
 * assembling message metadata.
 *
 * <p>The shared {@link #configuration} is a JsonPath {@link Configuration} backed by a
 * {@code JsonOrgJsonProvider} so lookups operate over {@code org.json} objects. The tests cover field
 * extraction from flat, nested and repeated JSON, the error raised for invalid field paths,
 * timestamp-column coercion driven by the {@link SinkConfig} metadata column types, metadata assembly
 * from a {@link Message}, and the type validation performed by {@code MessageUtils.validate}.</p>
 */
public class MessageUtilsTest {
    /** JsonPath configuration backed by an {@code org.json} provider, shared by the lookups. */
    private final Configuration configuration = Configuration.builder()
            .jsonProvider(new JsonOrgJsonProvider())
            .build();

    /**
     * Verifies that a top-level string field is read from a JSON object.
     *
     * <p>Given a JSON object with a {@code test} property, when
     * {@link MessageUtils#getFieldFromJsonObject(String, JSONObject, Configuration)} is queried for
     * {@code "test"}, then it returns the string {@code "test"}.</p>
     */
    @Test
    public void shouldGetStringFieldFromJsonObject() {
        JSONObject object = new JSONObject("{\"test\" :\"test\"}");
        Assert.assertEquals("test", MessageUtils.getFieldFromJsonObject("test", object, configuration));
    }


    /**
     * Verifies that elements of a JSON array of objects are addressable by index and key.
     *
     * <p>Given a {@code test} array of person objects, when fields are read with the paths
     * {@code "test[1].name"} and {@code "test[2].height"}, then they resolve to {@code "Bob"} and
     * {@code 175} respectively.</p>
     */
    @Test
    public void shouldGetFieldFromNested() {
        JSONObject object = new JSONObject("{\"test\" :[{\"name\":\"John\",\"age\":50},{\"name\":\"Bob\",\"age\":60},{\"name\":\"Alice\",\"active\":true,\"height\":175}]}");
        Assert.assertEquals("Bob", MessageUtils.getFieldFromJsonObject("test[1].name", object, configuration));
        Assert.assertEquals(175, MessageUtils.getFieldFromJsonObject("test[2].height", object, configuration));
    }

    /**
     * Verifies that nested arrays and deeply nested values are read correctly.
     *
     * <p>Given a {@code test} array whose first element holds an {@code alist} array, when fields are
     * read by path, then {@code "test[2].height"} resolves to {@code 175}, {@code "test[0].alist"}
     * returns the nested array in its JSON form, and {@code "test[0].alist[0].value"} resolves to
     * {@code "sometest"}.</p>
     */
    @Test
    public void shouldGetRepeatedField() {
        String jsonString = "{\n"
                + "  \"test\": [\n"
                + "    {\n"
                + "      \"name\": \"John\",\n"
                + "      \"age\": 50,\n"
                + "      \"alist\": [\n"
                + "        {\n"
                + "          \"name\": \"test\",\n"
                + "          \"value\": \"sometest\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"name\": \"test2\",\n"
                + "          \"value\": \"sometest2\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"name\": \"John\",\n"
                + "      \"age\": 60\n"
                + "    },\n"
                + "    {\n"
                + "      \"name\": \"John\",\n"
                + "      \"active\": true,\n"
                + "      \"height\": 175\n"
                + "    }\n"
                + "  ]\n"
                + "}\n";

        JSONObject object = new JSONObject(jsonString);
        Assert.assertEquals(175, MessageUtils.getFieldFromJsonObject("test[2].height", object, configuration));
        Assert.assertEquals("[{\"name\":\"test\",\"value\":\"sometest\"},{\"name\":\"test2\",\"value\":\"sometest2\"}]", MessageUtils.getFieldFromJsonObject("test[0].alist", object, configuration).toString());
        Assert.assertEquals("sometest", MessageUtils.getFieldFromJsonObject("test[0].alist[0].value", object, configuration));
    }

    /**
     * Verifies that an unknown field path is rejected with a descriptive error.
     *
     * <p>Given a JSON object with only a {@code test} property, when a missing top-level path
     * ({@code "testing"}) and a missing nested path ({@code "test[0].testing"}) are queried, then each
     * throws an {@link IllegalArgumentException} whose message is {@code "Invalid field config : "}
     * followed by the offending path.</p>
     */
    @Test
    public void shouldThrowExceptionIfInvalidPath() {
        JSONObject object = new JSONObject("{\"test\" :\"test\"}");
        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () -> MessageUtils.getFieldFromJsonObject("testing", object, configuration));
        Assert.assertEquals("Invalid field config : testing", exception.getMessage());

        exception = Assert.assertThrows(IllegalArgumentException.class, () -> MessageUtils.getFieldFromJsonObject("test[0].testing", object, configuration));
        Assert.assertEquals("Invalid field config : test[0].testing", exception.getMessage());
    }

    /**
     * Verifies that metadata columns typed as timestamps are converted with the supplied function.
     *
     * <p>Given a metadata map and metadata column types declaring {@code col4} as a timestamp, when
     * {@link MessageUtils#checkAndSetTimeStampColumns(java.util.Map, java.util.List, java.util.function.Function)}
     * applies a {@link Date}-producing converter, then {@code col4} becomes a {@link Date} while the
     * string and integer columns are left unchanged.</p>
     */
    @Test
    public void shouldCheckAndSetTimeStampColumns() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("col1", "value1");
        metadata.put("col2", "value2");
        metadata.put("col3", 50000);
        metadata.put("col4", 1668158346000L);

        Map<String, String> configMap = new HashMap<>();
        configMap.put("SINK_ADD_METADATA_ENABLED", "true");
        configMap.put("SINK_METADATA_COLUMNS_TYPES", "col1=string,col2=string,col3=integer,col4=timestamp");
        SinkConfig config = ConfigFactory.create(SinkConfig.class, configMap);
        Function<Long, Object> timeStampConvertor = (Date::new);
        Map<String, Object> finalMetadata = MessageUtils.checkAndSetTimeStampColumns(metadata, config.getMetadataColumnsTypes(), timeStampConvertor);

        Assert.assertEquals(4, finalMetadata.size());
        Assert.assertEquals("value1", finalMetadata.get("col1"));
        Assert.assertEquals("value2", finalMetadata.get("col2"));
        Assert.assertEquals(50000, finalMetadata.get("col3"));
        Assert.assertEquals(new Date(1668158346000L), finalMetadata.get("col4"));
    }

    /**
     * Verifies that message metadata is assembled and timestamp columns are coerced.
     *
     * <p>Given a {@link Message} carrying metadata tuples and a config declaring {@code col4} as a
     * timestamp, when
     * {@link MessageUtils#getMetaData(Message, SinkConfig, java.util.function.Function)} is called with
     * a {@link Date}-producing converter, then the returned map has four entries with {@code col4}
     * converted to a {@link Date} and the remaining values preserved.</p>
     */
    @Test
    public void shouldReturnMetadata() {
        Message message = new Message(
                null,
                null,
                new Tuple<>("col1", "value1"),
                new Tuple<>("col2", "value2"),
                new Tuple<>("col3", 50000),
                new Tuple<>("col4", 1668158346000L));

        Map<String, String> configMap = new HashMap<>();
        configMap.put("SINK_ADD_METADATA_ENABLED", "true");
        configMap.put("SINK_METADATA_COLUMNS_TYPES", "col1=string,col2=string,col3=integer,col4=timestamp");
        SinkConfig config = ConfigFactory.create(SinkConfig.class, configMap);
        Function<Long, Object> timeStampConvertor = (Date::new);
        Map<String, Object> finalMetadata = MessageUtils.getMetaData(message, config, timeStampConvertor);
        Assert.assertEquals(4, finalMetadata.size());
        Assert.assertEquals("value1", finalMetadata.get("col1"));
        Assert.assertEquals("value2", finalMetadata.get("col2"));
        Assert.assertEquals(50000, finalMetadata.get("col3"));
        Assert.assertEquals(new Date(1668158346000L), finalMetadata.get("col4"));
    }

    /**
     * Verifies that validating a message against a mismatched type fails.
     *
     * <p>Given a {@link Message} whose key and value are {@link String}s, when
     * {@link MessageUtils#validate(Message, Class)} is asked to validate it as {@link Integer}, then an
     * {@link IOException} is thrown describing the expected type alongside the actual key and message
     * types.</p>
     */
    @Test
    public void shouldThrowExceptionIfNotValid() {
        Message message = new Message("test", "test");
        IOException ioException = Assertions.assertThrows(IOException.class, () -> MessageUtils.validate(message, Integer.class));
        Assert.assertEquals("Expected class class java.lang.Integer, but found: LogKey class: class java.lang.String, LogMessage class: class java.lang.String", ioException.getMessage());
    }
}
