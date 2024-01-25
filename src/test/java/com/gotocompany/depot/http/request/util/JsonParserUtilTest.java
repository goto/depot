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
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("23.6677");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("23.6677", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonInteger() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("234");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("234", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonBoolean() throws JsonProcessingException {


        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("false");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("false", parsedJsonNode.toString());
    }


    @Test
    public void shouldParseJSONStringWithoutTemplate() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"sss\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"sss\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithArrayArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,list_values\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[\"test-list-1\",\"test-list-2\",\"test-list-3\"]", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJSONStringTemplateWithObjectArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,message_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithFloatArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,float_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("10.0", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithTimestampArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,timestamp_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"2022-11-22T23:36:47.600Z\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithBooleanArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,bool_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("true", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithStringArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,string_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"test-string\"", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJSONObjectStringWithoutTemplate() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"aa\":\"dd\",\"gg\":\"hh\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"aa\":\"dd\",\"gg\":\"hh\"}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"ss\":\"%s,bool_value\",\"%s,float_value\":\"hh\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"ss\":true,\"10.0\":\"hh\"}", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonNullStringWithoutTemplate() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("null");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("null", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringWithoutTemplate() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[\"ss\",23]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[\"ss\",23]", parsedJsonNode.toString());
    }

    @Test
    public void shouldParseJsonStringTemplateWithObjectArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[\"%s,message_value\",\"%s,float_value\"]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"},10.0]", parsedJsonNode.toString());
    }
}
