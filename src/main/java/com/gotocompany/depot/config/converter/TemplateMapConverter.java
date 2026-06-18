package com.gotocompany.depot.config.converter;

import com.google.common.base.Strings;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.exception.InvalidTemplateException;
import org.aeonbits.owner.Converter;
import org.json.JSONObject;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Owner {@link Converter} that parses a JSON object string into a {@code Map<Template, Template>}.
 *
 * <p>Both the keys and the values of the supplied JSON object are compiled into Depot
 * {@link Template} instances, so each side may contain literal text and/or field placeholders that
 * are later resolved against a parsed message. This is used by configuration that maps a templated
 * key to a templated value, such as dynamic routing or parameter definitions.
 *
 * <p>A {@code null} or empty input yields an empty map.
 *
 * @see Template
 * @see Converter
 */
public class TemplateMapConverter implements Converter<Map<Template, Template>> {

    /**
     * Parses the JSON object input into a map of compiled {@link Template} keys and values.
     *
     * <p>If the input is {@code null} or empty an empty map is returned. Otherwise the input is
     * parsed as a {@link org.json.JSONObject} and every entry is converted: a {@link Template} is
     * built from the property name and another {@link Template} is built from the string form of the
     * property value, and the pair is stored in the resulting map. Any {@link InvalidTemplateException}
     * raised while compiling a template is translated into an {@link IllegalArgumentException}
     * carrying the same message.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value, expected to be a JSON object whose keys and values are
     *               template expressions
     * @return a map of compiled {@link Template} keys to compiled {@link Template} values, or an
     *         empty map when {@code input} is {@code null} or empty
     * @throws IllegalArgumentException if any key or value is not a valid template
     * @throws org.json.JSONException   if {@code input} is not a well-formed JSON object
     */
    @Override
    public Map<Template, Template> convert(Method method, String input) {
        if (Strings.isNullOrEmpty(input)) {
            return Collections.emptyMap();
        }
        JSONObject jsonObject = new JSONObject(input);
        Map<Template, Template> templateMap = new HashMap<>();
        Iterator<String> keys = jsonObject.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            try {
                templateMap.put(new Template(key), new Template(jsonObject.get(key).toString()));
            } catch (InvalidTemplateException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }
        return templateMap;
    }
}
