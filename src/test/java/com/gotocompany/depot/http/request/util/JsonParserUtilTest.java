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

/**
 * Unit tests for {@link JsonParserUtils}, the helper that walks a JSON tree and resolves
 * {@code %s,field} placeholders against a parsed protobuf message while leaving non-templated JSON
 * untouched.
 *
 * <p>A real {@link ProtoMessageParser} obtained via {@link MessageParserFactory} parses a
 * {@link com.gotocompany.depot.TestTypesMessage} (with representative string, numeric, boolean,
 * repeated, nested-message and timestamp fields) into the {@link #parsedLogMessage} fixture, and the
 * {@link StatsDReporter} is a Mockito mock. Each test reads a raw JSON snippet with a shared Jackson
 * {@link ObjectMapper}, passes it through {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} and
 * asserts on the serialised result.</p>
 *
 * <p>The scenarios exhaustively cover: untemplated pass-through of every JSON type; typed substitution
 * of placeholders into JSON values (arrays, objects, primitives and timestamps); forced
 * stringification using escaped quotes; placeholders embedded in surrounding text; multiple
 * placeholders in one template; placeholders in object keys, object values and array elements; empty
 * JSON values; and the escaped-comma ({@code /,/}) separator handling.</p>
 */
public class JsonParserUtilTest {

    /**
     * Jackson mapper configured to fail on trailing tokens, used to read the raw JSON snippets into
     * {@link JsonNode} trees.
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    /**
     * Mocked metrics reporter required when constructing the message parser.
     */
    @Mock
    private StatsDReporter statsDReporter;
    /**
     * Timestamp instant embedded in the message and referenced by the timestamp assertions.
     */
    private Instant time;
    /**
     * HTTP sink configuration built once in {@link #setup()} from {@link #configuration}.
     */
    private HttpSinkConfig sinkConfig;
    /**
     * Parsed protobuf log message fixture against which all template placeholders are resolved.
     */
    private ParsedMessage parsedLogMessage;
    /**
     * Mutable configuration map seeded in {@link #setup()} for building the parser configuration.
     */
    private final Map<String, String> configuration = new HashMap<>();

    /**
     * Parses the representative protobuf message into {@link #parsedLogMessage} before each test.
     *
     * <p>Configures the proto key/message classes, {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE}
     * mode and disables default field values, then builds a {@link com.gotocompany.depot.TestTypesMessage}
     * (with string, float, int32, int64, boolean, repeated, nested-message and timestamp fields) and
     * parses it with a real {@link ProtoMessageParser}.</p>
     *
     * @throws IOException if parser construction or message parsing fails
     */
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
                .setInt64Value(299283773722L)
                .setBoolValue(true)
                .addListValues("test-list-1").addListValues("test-list-2").addListValues("test-list-3")
                .addListMessageValues(testMessage).addListMessageValues(testMessage)
                .setMessageValue(testMessage)
                .setTimestampValue(timestamp)
                .build();
        Message message = new Message(testMessage.toByteArray(), testTypesMessage.toByteArray());
        parsedLogMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestTypesMessage");
    }

    /**
     * Verifies that an untemplated floating-point literal is passed through unchanged.
     *
     * <p>Given the raw JSON {@code 23.6677}, when {@link JsonParserUtils#parse(JsonNode, ParsedMessage)}
     * is invoked, then the result serialises back to {@code 23.6677}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonFloatType() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("23.6677");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("23.6677", parsedJsonNode.toString());
    }

    /**
     * Verifies that an untemplated array is passed through unchanged.
     *
     * <p>Given the raw JSON {@code ["ss",23]}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result serialises
     * back to {@code ["ss",23]}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonArrayTypeWithoutTemplate() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[\"ss\",23]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[\"ss\",23]", parsedJsonNode.toString());
    }

    /**
     * Verifies that an untemplated string literal is passed through unchanged.
     *
     * <p>Given the raw JSON {@code "sss"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result serialises
     * back to {@code "sss"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringWithoutTemplate() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"sss\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"sss\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that an untemplated integer literal is passed through unchanged.
     *
     * <p>Given the raw JSON {@code 234}, when {@link JsonParserUtils#parse(JsonNode, ParsedMessage)}
     * is invoked, then the result serialises back to {@code 234}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonIntegerType() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("234");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("234", parsedJsonNode.toString());
    }

    /**
     * Verifies that an untemplated long literal is passed through unchanged.
     *
     * <p>Given the raw JSON {@code 23492992920}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result serialises
     * back to {@code 23492992920}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonLongType() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("23492992920");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("23492992920", parsedJsonNode.toString());
    }

    /**
     * Verifies that an untemplated object is passed through unchanged.
     *
     * <p>Given the raw JSON {@code {"aa":22,"gg":true,"ss":"ee","pp":33.45}}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result serialises
     * back to the same object.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonObjectTypeWithoutTemplate() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"aa\":22,\"gg\":true,\"ss\":\"ee\",\"pp\":33.45}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"aa\":22,\"gg\":true,\"ss\":\"ee\",\"pp\":33.45}", parsedJsonNode.toString());
    }

    /**
     * Verifies that an untemplated boolean literal is passed through unchanged.
     *
     * <p>Given the raw JSON {@code false}, when {@link JsonParserUtils#parse(JsonNode, ParsedMessage)}
     * is invoked, then the result serialises back to {@code false}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonBooleanType() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("false");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("false", parsedJsonNode.toString());
    }

    /**
     * Verifies that a string placeholder referencing a repeated field is substituted as a JSON array.
     *
     * <p>Given the template {@code "%s,list_values"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * array {@code ["test-list-1","test-list-2","test-list-3"]}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithArrayArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,list_values\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[\"test-list-1\",\"test-list-2\",\"test-list-3\"]", parsedJsonNode.toString());
    }

    /**
     * Verifies that a string placeholder referencing a message field is substituted as a JSON object.
     *
     * <p>Given the template {@code "%s,message_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * object {@code {"order_number":"test-order-1","order_details":"ORDER-DETAILS-1"}}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithObjectArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,message_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"}", parsedJsonNode.toString());
    }

    /**
     * Verifies that a placeholder can navigate into a nested message field.
     *
     * <p>Given the template {@code "%s,message_value.order_number"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * string {@code "test-order-1"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithNestedStringArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,message_value.order_number\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"test-order-1\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that a string placeholder referencing a float field is substituted as a JSON number.
     *
     * <p>Given the template {@code "%s,float_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * number {@code 10.0}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithFloatArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,float_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("10.0", parsedJsonNode.toString());
    }

    /**
     * Verifies that a string placeholder referencing a timestamp field is substituted as an ISO-8601
     * string.
     *
     * <p>Given the template {@code "%s,timestamp_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * string {@code "2022-11-22T23:36:47.600Z"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithTimestampArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,timestamp_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"2022-11-22T23:36:47.600Z\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that a string placeholder referencing a boolean field is substituted as a JSON boolean.
     *
     * <p>Given the template {@code "%s,bool_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * boolean {@code true}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithBooleanArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,bool_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("true", parsedJsonNode.toString());
    }

    /**
     * Verifies that a string placeholder referencing an int32 field is substituted as a JSON number.
     *
     * <p>Given the template {@code "%s,int32_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * number {@code 445}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithIntegerArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,int32_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("445", parsedJsonNode.toString());
    }

    /**
     * Verifies that a string placeholder referencing an int64 field is substituted as a JSON number.
     *
     * <p>Given the template {@code "%s,int64_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * number {@code 299283773722}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithLongArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,int64_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("299283773722", parsedJsonNode.toString());
    }

    /**
     * Verifies that a string placeholder referencing a string field is substituted as a JSON string.
     *
     * <p>Given the template {@code "%s,string_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * string {@code "test-string"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithStringArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s,string_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"test-string\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that wrapping a boolean placeholder in escaped quotes forces a stringified result.
     *
     * <p>Given the template {@code "\"%s\",bool_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the boolean is rendered
     * as the JSON string {@code "true"} rather than a boolean.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithBooleanArgumentToString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\\\"%s\\\",bool_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"true\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that wrapping an int32 placeholder in escaped quotes forces a stringified result.
     *
     * <p>Given the template {@code "\"%s\",int32_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the number is rendered as
     * the JSON string {@code "445"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithIntegerArgumentToString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\\\"%s\\\",int32_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"445\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that wrapping an int64 placeholder in escaped quotes forces a stringified result.
     *
     * <p>Given the template {@code "\"%s\",int64_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the number is rendered as
     * the JSON string {@code "299283773722"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithLongArgumentToString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\\\"%s\\\",int64_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"299283773722\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that wrapping a message placeholder in escaped quotes forces a stringified object.
     *
     * <p>Given the template {@code "\"%s\",message_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the object is rendered as
     * a single JSON string containing the escaped object JSON.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithObjectArgumentToString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\\\"%s\\\",message_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"{\\\"order_number\\\":\\\"test-order-1\\\",\\\"order_details\\\":\\\"ORDER-DETAILS-1\\\"}\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that wrapping a repeated-field placeholder in escaped quotes forces a stringified array.
     *
     * <p>Given the template {@code "\"%s\",list_values"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the array is rendered as
     * a single JSON string containing the escaped array JSON.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithArrayArgumentToString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\\\"%s\\\",list_values\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"[\\\"test-list-1\\\",\\\"test-list-2\\\",\\\"test-list-3\\\"]\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that wrapping a float placeholder in escaped quotes forces a stringified result.
     *
     * <p>Given the template {@code "\"%s\",float_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the number is rendered as
     * the JSON string {@code "10.0"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithFloatArgumentToString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\\\"%s\\\",float_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"10.0\"", parsedJsonNode.toString());
    }


    /**
     * Verifies that a repeated-field placeholder embedded in surrounding text yields a single string.
     *
     * <p>Given the template {@code "array = %s,list_values"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * string {@code "array = [\"test-list-1\",\"test-list-2\",\"test-list-3\"]"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithArrayArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"array = %s,list_values\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"array = [\\\"test-list-1\\\",\\\"test-list-2\\\",\\\"test-list-3\\\"]\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that a message placeholder embedded in surrounding text yields a single string.
     *
     * <p>Given the template {@code "object = %s,message_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * string {@code "object = {...}"} containing the inlined object JSON.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithObjectArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"object = %s,message_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"object = {\\\"order_number\\\":\\\"test-order-1\\\",\\\"order_details\\\":\\\"ORDER-DETAILS-1\\\"}\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that a float placeholder embedded in surrounding text yields a single string.
     *
     * <p>Given the template {@code "float = %s,float_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * string {@code "float = 10.0"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithFloatArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"float = %s,float_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"float = 10.0\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that a boolean placeholder embedded in surrounding text yields a single string.
     *
     * <p>Given the template {@code "bool = %s,bool_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * string {@code "bool = true"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithBooleanArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"bool = %s,bool_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"bool = true\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that an int32 placeholder embedded in surrounding text yields a single string.
     *
     * <p>Given the template {@code "int32 = %s,int32_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * string {@code "int32 = 445"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithIntegerArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"int32 = %s,int32_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"int32 = 445\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that an int64 placeholder embedded in surrounding text yields a single string.
     *
     * <p>Given the template {@code "int64 = %s,int64_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * string {@code "int64 = 299283773722"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithLongArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"int64 = %s,int64_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"int64 = 299283773722\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that several placeholders of differing types in one template are all resolved in order.
     *
     * <p>Given a template combining {@code int64_value}, {@code message_value}, {@code bool_value} and
     * {@code list_values} placeholders within surrounding text, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is a single
     * JSON string with each placeholder replaced by its inlined value in order.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithMultipleTypeArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"int64 = %s object = %s bool = %s array = %s,int64_value,message_value,bool_value,list_values\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"int64 = 299283773722 object = {\\\"order_number\\\":\\\"test-order-1\\\",\\\"order_details\\\":\\\"ORDER-DETAILS-1\\\"} bool = true array = [\\\"test-list-1\\\",\\\"test-list-2\\\",\\\"test-list-3\\\"]\"", parsedJsonNode.toString());
    }


    /**
     * Verifies that a string placeholder embedded in surrounding text yields a single string.
     *
     * <p>Given the template {@code "string = %s,string_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * string {@code "string = test-string"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithStringArgumentAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"string = %s,string_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"string = test-string\"", parsedJsonNode.toString());
    }


    /**
     * Verifies that placeholders in object values are substituted as typed primitives.
     *
     * <p>Given the template {@code {"ss":"%s,bool_value","hh":"%s,float_value"}}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is
     * {@code {"ss":true,"hh":10.0}}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonObjectTemplateWithPrimitiveTypeArgumentsInValue() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"ss\":\"%s,bool_value\",\"hh\":\"%s,float_value\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"ss\":true,\"hh\":10.0}", parsedJsonNode.toString());
    }

    /**
     * Verifies that a placeholder in an object value is substituted as a JSON array.
     *
     * <p>Given the template {@code {"ss":"%s,list_values"}}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is
     * {@code {"ss":["test-list-1","test-list-2","test-list-3"]}}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonObjectTemplateWithArrayTypeArgumentsInValue() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"ss\":\"%s,list_values\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"ss\":[\"test-list-1\",\"test-list-2\",\"test-list-3\"]}", parsedJsonNode.toString());
    }

    /**
     * Verifies that a placeholder in an object value is substituted as a nested JSON object.
     *
     * <p>Given the template {@code {"ss":"%s,message_value"}}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is
     * {@code {"ss":{"order_number":"test-order-1","order_details":"ORDER-DETAILS-1"}}}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonObjectTemplateWithObjectTypeArgumentsInValue() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"ss\":\"%s,message_value\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"ss\":{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"}}", parsedJsonNode.toString());
    }

    /**
     * Verifies that placeholders in object keys are substituted with primitive field values.
     *
     * <p>Given the template {@code {"%s,bool_value":"ss","%s,float_value":"hh","%s,string_value":"vv"}},
     * when {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the keys become the
     * resolved values, yielding {@code {"true":"ss","10.0":"hh","test-string":"vv"}}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonObjectTemplateWithPrimitiveTypeArgumentsInKey() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"%s,bool_value\":\"ss\",\"%s,float_value\":\"hh\",\"%s,string_value\":\"vv\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"true\":\"ss\",\"10.0\":\"hh\",\"test-string\":\"vv\"}", parsedJsonNode.toString());
    }

    /**
     * Verifies that an array-typed placeholder used as an object key is stringified.
     *
     * <p>Given the template {@code {"%s,list_values":"ss"}}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the array is rendered as
     * the (escaped) string key, yielding {@code {"[\"test-list-1\",\"test-list-2\",\"test-list-3\"]":"ss"}}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonObjectTemplateWithArrayTypeArgumentsInKey() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"%s,list_values\":\"ss\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"[\\\"test-list-1\\\",\\\"test-list-2\\\",\\\"test-list-3\\\"]\":\"ss\"}", parsedJsonNode.toString());
    }

    /**
     * Verifies that an object-typed placeholder used as an object key is stringified.
     *
     * <p>Given the template {@code {"%s,message_value":"ss"}}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the object is rendered as
     * the (escaped) string key, yielding {@code {"{\"order_number\":...}":"ss"}}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonObjectTemplateWithObjectTypeArgumentsInKey() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"%s,message_value\":\"ss\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"{\\\"order_number\\\":\\\"test-order-1\\\",\\\"order_details\\\":\\\"ORDER-DETAILS-1\\\"}\":\"ss\"}", parsedJsonNode.toString());
    }

    /**
     * Verifies that an untemplated null literal is passed through unchanged.
     *
     * <p>Given the raw JSON {@code null}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result serialises
     * back to {@code null}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonNullStringWithoutTemplate() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("null");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("null", parsedJsonNode.toString());
    }

    /**
     * Verifies that placeholders inside an array are each substituted as typed primitives.
     *
     * <p>Given the template {@code ["%s,string_value","%s,float_value","%s,bool_value"]}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is
     * {@code ["test-string",10.0,true]}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonArrayTemplateWithPrimitiveArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[\"%s,string_value\",\"%s,float_value\",\"%s,bool_value\"]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[\"test-string\",10.0,true]", parsedJsonNode.toString());
    }

    /**
     * Verifies that a repeated-field placeholder inside an array yields a nested array.
     *
     * <p>Given the template {@code ["%s,list_values"]}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is
     * {@code [["test-list-1","test-list-2","test-list-3"]]}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonArrayTemplateWithArrayArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[\"%s,list_values\"]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[[\"test-list-1\",\"test-list-2\",\"test-list-3\"]]", parsedJsonNode.toString());
    }

    /**
     * Verifies that a message placeholder inside an array yields an array of objects.
     *
     * <p>Given the template {@code ["%s,message_value"]}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is
     * {@code [{"order_number":"test-order-1","order_details":"ORDER-DETAILS-1"}]}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonArrayTemplateWithObjectArgument() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[\"%s,message_value\"]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[{\"order_number\":\"test-order-1\",\"order_details\":\"ORDER-DETAILS-1\"}]", parsedJsonNode.toString());
    }

    /**
     * Verifies that an empty array is passed through unchanged.
     *
     * <p>Given the raw JSON {@code []}, when {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is
     * invoked, then the result serialises back to {@code []}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseEmptyJsonArray() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[]", parsedJsonNode.toString());
    }

    /**
     * Verifies that an empty object is passed through unchanged.
     *
     * <p>Given the raw JSON {@code {}}, when {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is
     * invoked, then the result serialises back to {@code {}}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseEmptyJsonObject() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{}", parsedJsonNode.toString());
    }

    /**
     * Verifies that an empty string object value is preserved.
     *
     * <p>Given the raw JSON {@code {"ss":""}}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result serialises
     * back to {@code {"ss":""}}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseEmptyJsonStringInObjectValue() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"ss\":\"\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"ss\":\"\"}", parsedJsonNode.toString());
    }

    /**
     * Verifies that an empty string object key is preserved.
     *
     * <p>Given the raw JSON {@code {"":"ss"}}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result serialises
     * back to {@code {"":"ss"}}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseEmptyJsonStringInObjectKey() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("{\"\":\"ss\"}");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("{\"\":\"ss\"}", parsedJsonNode.toString());
    }

    /**
     * Verifies that an empty string array element is preserved.
     *
     * <p>Given the raw JSON {@code ["ss",""]}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result serialises
     * back to {@code ["ss",""]}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseEmptyJsonStringInArray() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("[\"ss\",\"\"]");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("[\"ss\",\"\"]", parsedJsonNode.toString());
    }

    /**
     * Verifies that a standalone empty string is preserved.
     *
     * <p>Given the raw JSON {@code ""}, when {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is
     * invoked, then the result serialises back to {@code ""}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseEmptyJsonString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that the escaped-comma sequence {@code /,/} emits a literal comma alongside a single
     * argument.
     *
     * <p>Given the template {@code "%s/,/,int32_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the {@code int32_value}
     * is rendered followed by a literal comma, yielding {@code "445,"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithCommaWithSingleArguments() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s/,/,int32_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"445,\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that the escaped-comma sequence {@code /,/} separates two substituted arguments.
     *
     * <p>Given the template {@code "%s/,/%s,int32_value,int32_value"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then both placeholders are
     * rendered around a literal comma, yielding {@code "445,445"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplateWithCommaWithMultipleArguments() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"%s/,/%s,int32_value,int32_value\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"445,445\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that a template consisting solely of the escaped-comma sequence yields a literal comma.
     *
     * <p>Given the template {@code "/,/"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * string {@code ","}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStringTemplaateWithOnlyComma() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"/,/\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\",\"", parsedJsonNode.toString());
    }

    /**
     * Verifies that the escaped-comma sequence emits a literal comma between surrounding literal text.
     *
     * <p>Given the template {@code "ww/,/eee"}, when
     * {@link JsonParserUtils#parse(JsonNode, ParsedMessage)} is invoked, then the result is the JSON
     * string {@code "ww,eee"}.</p>
     *
     * @throws JsonProcessingException if the raw JSON cannot be read or parsed
     */
    @Test
    public void shouldParseJsonStaringTemplateWithCommaAppendedWithString() throws JsonProcessingException {
        JsonNode rawJsonNode = OBJECT_MAPPER.readTree("\"ww/,/eee\"");
        JsonNode parsedJsonNode = JsonParserUtils.parse(rawJsonNode, parsedLogMessage);
        assertEquals("\"ww,eee\"", parsedJsonNode.toString());
    }

}
