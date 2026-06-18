package com.gotocompany.depot.http.request.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.gson.JsonSyntaxException;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.ParsedMessage;

import java.io.IOException;
import java.util.Map;


/**
 * This is a utility class for providing functionality for parsing a templatized Json string
 * using a provided ParsedMessage which contains all the fields of the deserialized Protobuf
 * message. Thus the arguments in the template string will be replaced by the fields from
 * the ParsedMessage
 *
 * <p>This class provides functionality for parsing a templatized JSON string using a provided
 * {@link ParsedMessage} which contains all the fields of the deserialized Protobuf message. The
 * arguments embedded in the template string are replaced by the corresponding fields read from the
 * {@link ParsedMessage}. It underpins the
 * {@link com.gotocompany.depot.http.request.body.TemplatizedJsonBody} request body type.</p>
 *
 * <p>Parsing proceeds in two steps: {@link #createJsonNode(String)} compiles the raw template text
 * into a {@link JsonNode} tree once, and {@link #parse(JsonNode, ParsedMessage)} walks that tree per
 * message, substituting templated strings (in both object keys and values) while preserving the JSON
 * structure. The class exposes only static methods and is not meant to be instantiated.</p>
 *
 * @see com.gotocompany.depot.http.request.body.TemplatizedJsonBody
 * @see ParsedMessage
 * @see Template
 */
public class JsonParserUtils {

    /**
     * Shared Jackson mapper configured to fail when trailing tokens follow a parsed JSON value,
     * guarding against malformed template fragments.
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    /**
     * Creates a raw Json Node object from the provided Json template string.
     *
     * <p>The template text is read into a {@link JsonNode} using the shared, strict object mapper.
     * This is performed once per configured template so that the resulting tree can be reused for
     * every message.</p>
     *
     * @param jsonTemplate the Json template string
     * @return the raw Json node
     * @throws ConfigurationException if {@code jsonTemplate} is empty or is not valid JSON
     */
    public static JsonNode createJsonNode(String jsonTemplate) {
        if (jsonTemplate.isEmpty()) {
            throw new ConfigurationException("Json body template cannot be empty");
        }
        try {
            return OBJECT_MAPPER.readTree(jsonTemplate);

        } catch (JsonSyntaxException | IOException e) {
            throw new ConfigurationException(String.format("Json body template is not a valid json. %s", e.getMessage()));
        }
    }

    /**
     * Parses a raw templatized Json Node using a provided ParsedMessage which contains all
     * the fields of the deserialized Protobuf message. Thus the arguments in the template
     * string will be replaced by the fields from the ParsedMessage Proto object
     *
     * <p>The supplied {@link ParsedMessage} contains all the fields of the deserialized Protobuf
     * message, and the arguments embedded in the template are replaced by those fields. The node type
     * dictates the handling: {@code ARRAY} and {@code OBJECT} nodes are recursed into element by
     * element, {@code STRING} nodes are resolved as templates, and scalar {@code NUMBER},
     * {@code BOOLEAN} and {@code NULL} nodes are returned unchanged.</p>
     *
     * @param rawJsonNode   the raw Json node
     * @param parsedMessage the parsed message
     * @return the parsed Json node
     * @throws IllegalArgumentException if {@code rawJsonNode} is of an unsupported JSON type, or if a
     *     string node holds an invalid template
     * @throws ConfigurationException if a substituted string value cannot be parsed back into JSON
     */
    public static JsonNode parse(JsonNode rawJsonNode, ParsedMessage parsedMessage) {
        switch (rawJsonNode.getNodeType()) {
            case ARRAY:
                return parseInternal((ArrayNode) rawJsonNode, parsedMessage);
            case OBJECT:
                return parseInternal((ObjectNode) rawJsonNode, parsedMessage);
            case STRING:
                return parseInternal((TextNode) rawJsonNode, parsedMessage);
            case NUMBER:
            case BOOLEAN:
            case NULL:
                return rawJsonNode;
            default:
                throw new IllegalArgumentException("The provided Json type is not supported");
        }
    }

    /**
     * Resolves the templates within a JSON object node, including its keys.
     *
     * <p>Each property is processed independently: the key string is itself treated as a template (by
     * wrapping it in a {@link TextNode} and resolving it, then stripping the surrounding quotes when
     * the result is a string), and the value is resolved recursively via
     * {@link #parse(JsonNode, ParsedMessage)}. The resolved key/value pairs are accumulated into a new
     * object node.</p>
     *
     * @param rawJsonObject the raw JSON object node to resolve
     * @param parsedMessage the parsed message supplying values for template substitution
     * @return a new object node with all keys and values resolved
     */
    private static JsonNode parseInternal(ObjectNode rawJsonObject, ParsedMessage parsedMessage) {
        ObjectNode parsedJsonObject = JsonNodeFactory.instance.objectNode();
        for (Map.Entry<String, JsonNode> entry : rawJsonObject.properties()) {
            String rawKeyString = entry.getKey();
            TextNode rawKeyStringNode = new TextNode(rawKeyString);
            JsonNode parsedKeyStringNode = parseInternal(rawKeyStringNode, parsedMessage);
            String parsedKeyString = parsedKeyStringNode.toString();
            if (parsedKeyStringNode.getNodeType().equals(JsonNodeType.STRING)) {
                parsedKeyString = parsedKeyString.substring(1, parsedKeyString.length() - 1);
            }
            JsonNode rawValue = entry.getValue();
            JsonNode parsedValue = parse(rawValue, parsedMessage);
            parsedJsonObject.put(parsedKeyString, parsedValue);
        }
        return parsedJsonObject;
    }

    /**
     * Resolves the templates within a JSON array node element by element.
     *
     * <p>Each element is resolved via {@link #parse(JsonNode, ParsedMessage)} and appended to a new
     * array node, preserving order.</p>
     *
     * @param rawJsonArray the raw JSON array node to resolve
     * @param parsedMessage the parsed message supplying values for template substitution
     * @return a new array node with every element resolved
     */
    private static JsonNode parseInternal(ArrayNode rawJsonArray, ParsedMessage parsedMessage) {
        ArrayNode parsedJsonArray = JsonNodeFactory.instance.arrayNode();
        rawJsonArray.forEach(jsonNode -> parsedJsonArray.add(parse(jsonNode, parsedMessage)));
        return parsedJsonArray;
    }

    /**
     * Resolves a single JSON string node as a template against the parsed message.
     *
     * <p>An empty string node is returned unchanged. Otherwise the text is compiled into a
     * {@link Template} and resolved with type information preserved. When the resolved value is a
     * {@link String}, any surrounding quotes are stripped and a {@link TextNode} is returned so the
     * value stays a JSON string. When the resolved value is not a string, its textual form is parsed
     * back into a JSON node, allowing a template to expand into a number, boolean, object or array.</p>
     *
     * @param rawJsonStringNode the raw JSON string node holding the template text
     * @param parsedMessage the parsed message supplying values for template substitution
     * @return the resolved JSON node, either a text node or a node parsed from the substituted value
     * @throws IllegalArgumentException if the string holds an invalid template
     * @throws ConfigurationException if a non-string substituted value cannot be parsed back into JSON
     */
    private static JsonNode parseInternal(TextNode rawJsonStringNode, ParsedMessage parsedMessage) {
        String rawJsonString = rawJsonStringNode.asText();
        if (rawJsonString.isEmpty()) {
            return rawJsonStringNode;
        }
        Template templateValue;
        try {
            templateValue = new Template(rawJsonString);
        } catch (InvalidTemplateException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        Object parsedValue = templateValue.parseWithType(parsedMessage);
        String parsedJsonString = parsedValue.toString();
        JsonNode parsedJsonNode;
        if (parsedValue instanceof String) {
            if (parsedJsonString.startsWith("\"") && parsedJsonString.endsWith("\"")) {
                parsedJsonString = parsedJsonString.substring(1, parsedJsonString.length() - 1);
            }
            parsedJsonNode = JsonNodeFactory.instance.textNode(parsedJsonString);
            return parsedJsonNode;
        }
        try {
            parsedJsonNode = OBJECT_MAPPER.readTree(parsedJsonString);
        } catch (JsonProcessingException e) {
            throw new ConfigurationException("An error occurred while parsing the template string : " + parsedJsonString + "\nError: " + e.getMessage());
        }
        return parsedJsonNode;
    }
}
