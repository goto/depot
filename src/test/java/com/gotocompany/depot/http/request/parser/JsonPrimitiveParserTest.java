package com.gotocompany.depot.http.request.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
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

public class JsonPrimitiveParserTest {


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
    public void shouldParseJsonFloat() {
        JsonElement jsonElement = JsonParser.parseString("23.6677");
        JsonElement parsedJsonElement = new JsonPrimitiveParser().parse(jsonElement, parsedLogMessage);
        assertEquals("23.6677", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJsonInteger() {
        JsonElement jsonElement = JsonParser.parseString("234");
        JsonElement parsedJsonElement = new JsonPrimitiveParser().parse(jsonElement, parsedLogMessage);
        assertEquals("234", parsedJsonElement.toString());
    }

    @Test
    public void shouldParseJsonBoolean() {


        JsonElement jsonElement = JsonParser.parseString("false");
        JsonElement parsedJsonElement = new JsonPrimitiveParser().parse(jsonElement, parsedLogMessage);
        assertEquals("false", parsedJsonElement.toString());
    }


}
