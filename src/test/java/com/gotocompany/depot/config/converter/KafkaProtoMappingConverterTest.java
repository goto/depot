package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.exception.ConfigurationException;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class KafkaProtoMappingConverterTest {

    private KafkaProtoMappingConverter converter;

    @Before
    public void setup() {
        converter = new KafkaProtoMappingConverter();
    }

    @Test
    public void shouldConvertJsonToMapping() {
        Map<String, String> mapping = converter.convert(null,
                "{\"order_id\": \"\\\"wee\\\" + string(source.order_number)\", \"user_id\": \"source.account_go_id\"}");
        assertEquals(2, mapping.size());
        assertEquals("\"wee\" + string(source.order_number)", mapping.get("order_id"));
        assertEquals("source.account_go_id", mapping.get("user_id"));
    }

    @Test
    public void shouldReturnEmptyMappingForNullOrEmptyInput() {
        assertTrue(converter.convert(null, null).isEmpty());
        assertTrue(converter.convert(null, "").isEmpty());
        assertTrue(converter.convert(null, "   ").isEmpty());
    }

    @Test
    public void shouldFailForInvalidJson() {
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                () -> converter.convert(null, "{\"order_id\": "));
        assertTrue(exception.getMessage().contains("SINK_KAFKA_PROTO_MAPPING is not a valid JSON object"));
    }

    @Test
    public void shouldFailForDuplicateFieldNames() {
        assertThrows(ConfigurationException.class,
                () -> converter.convert(null, "{\"order_id\": \"source.a\", \"order_id\": \"source.b\"}"));
    }

    @Test
    public void shouldFailForNonStringExpressionValues() {
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                () -> converter.convert(null, "{\"order_id\": 34}"));
        assertTrue(exception.getMessage().contains("order_id"));
    }

    @Test
    public void shouldFailForEmptyExpressionValues() {
        assertThrows(ConfigurationException.class, () -> converter.convert(null, "{\"order_id\": \" \"}"));
    }

    @Test
    public void shouldFailForJsonArrayInput() {
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                () -> converter.convert(null, "[\"order_id\", \"user_id\"]"));
        assertTrue(exception.getMessage().contains("SINK_KAFKA_PROTO_MAPPING is not a valid JSON object"));
    }

    @Test
    public void shouldFailForScalarJsonInput() {
        assertThrows(ConfigurationException.class, () -> converter.convert(null, "\"order_id\""));
    }

    @Test
    public void shouldFailForNullExpressionValues() {
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                () -> converter.convert(null, "{\"order_id\": null}"));
        assertTrue(exception.getMessage().contains("order_id"));
    }

    @Test
    public void shouldFailForNestedObjectExpressionValues() {
        assertThrows(ConfigurationException.class,
                () -> converter.convert(null, "{\"order_id\": {\"nested\": \"source.a\"}}"));
    }

    @Test
    public void shouldFailForBooleanExpressionValues() {
        assertThrows(ConfigurationException.class, () -> converter.convert(null, "{\"is_valid\": true}"));
    }
}
