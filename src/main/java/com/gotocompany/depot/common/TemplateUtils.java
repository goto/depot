package com.gotocompany.depot.common;

import com.gotocompany.depot.message.ParsedMessage;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Static helpers for evaluating collections of {@link Template} instances against a message.
 *
 * <p>{@code TemplateUtils} groups utility methods that apply Depot's {@link Template} rendering to
 * structures of templates, such as a map of templated keys to templated values.
 */
public class TemplateUtils {

    /**
     * Renders a map of templates into a map of concrete strings for a single parsed message.
     *
     * <p>Each entry's key and value {@link Template} are evaluated against {@code parsedMessage} via
     * {@link Template#parse(ParsedMessage)}, producing a map whose keys and values are the resulting
     * interpolated strings.
     *
     * @param templateMap   the map whose key and value templates are rendered; each key and value is
     *                      a {@link Template}
     * @param parsedMessage the message supplying the field values referenced by the templates
     * @return a map associating each rendered key string with its corresponding rendered value string
     */
    public static Map<String, String> parseTemplateMap(Map<Template, Template> templateMap, ParsedMessage parsedMessage) {
        return templateMap
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().parse(parsedMessage),
                        entry -> entry.getValue().parse(parsedMessage)
                ));
    }
}
