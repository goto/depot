package com.gotocompany.depot.http.request.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JsonParserUtilTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    @Mock
    private StatsDReporter statsDReporter;
    private Instant time;
    private HttpSinkConfig sinkConfig;
    private ParsedMessage parsedLogMessage;
    private final Map<String, String> configuration = new HashMap<>();

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        time = Instant.ofEpochSecond(1669160207, 600000000);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestTypesMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "com.gotocompany.depot.TestMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        configuration.put("SINK_HTTPV2_DEFAULT_FIELD_VALUE_ENABLE", "false");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        ProtoMessageParser protoMessageParser = (ProtoMessageParser) MessageParserFactory.getParser(sinkConfig, statsDReporter);
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build();
        TestMessage testMessage = TestMessage.newBuilder()
                .setOrderNumber("test-order-1")
                .setOrderDetails("ORDER-DETAILS-1")
                .build();
        TestTypesMessage testTypesMessage = TestTypesMessage.newBuilder()
                .setStringValue("test-string")
                .setFloatValue(10.0f)
                .setInt32Value(445)
                .setBoolValue(true)
                .addListValues("test-list-1").addListValues("test-list-2").addListValues("test-list-3")
                .addListMessageValues(testMessage).addListMessageValues(testMessage)
                .setMessageValue(testMessage)
                .setTimestampValue(timestamp)
                .build();
        Message message = new Message(testMessage.toByteArray(), testTypesMessage.toByteArray());
        parsedLogMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestTypesMessage");
    }


    @Test
    public void shouldParseJsonFloat() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("23.6677");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("23.6677", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJsonInteger() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("234");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("234", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJsonBoolean() throws JsonProcessingException {


        JsonNode jsonElement = OBJECT_MAPPER.readTree("false");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("false", parsedJsonElement.toString());
    }


    @Test
    public void shouldParseJSONStringWithoutTemplate() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("\"sss\"");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("\"sss\"", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithArrayArgument() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("\"%s,list_values\"");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("[\"test-list-1\",\"test-list-2\",\"test-list-3\"]", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJSONStringTemplateWithObjectArgument() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("\"%s,message_value\"");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"}", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithFloatArgument() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("\"%s,float_value\"");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("10.0", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithTimestampArgument() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("\"%s,timestamp_value\"");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("\"2022-11-22T23:36:47.600Z\"", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithBooleanArgument() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("\"%s,bool_value\"");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("true", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithStringArgument() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("\"%s,string_value\"");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("\"test-string\"", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJSONObjectStringWithoutTemplate() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("{\"aa\":\"dd\",\"gg\":\"hh\"}");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("{\"aa\":\"dd\",\"gg\":\"hh\"}", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithArgument() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("{\"ss\":\"%s,bool_value\",\"%s,float_value\":\"hh\"}");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("{\"ss\":true,\"10.0\":\"hh\"}", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJsonNullStringWithoutTemplate() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("null");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("null", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJsonStringWithoutTemplate() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("[\"ss\",23]");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("[\"ss\",23]", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithObjectArgument() throws JsonProcessingException {
        JsonNode jsonElement = OBJECT_MAPPER.readTree("[\"%s,message_value\",\"%s,float_value\"]");
        JsonNode parsedJsonElement = JsonParserUtils.parse(jsonElement, parsedLogMessage);
        assertEquals("[{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"},10.0]", parsedJsonElement.toString());
    }
}
