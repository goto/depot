package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.exception.ConfigurationException;
import org.aeonbits.owner.Converter;
import org.json.JSONException;
import org.json.JSONObject;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Owner converter that parses the SINK_KAFKA_PROTO_MAPPING JSON into an ordered field-to-expression map.
 */
public class KafkaProtoMappingConverter implements Converter<Map<String, String>> {

    /**
     * Converts the raw JSON mapping into an ordered map of output field names to CEL expressions.
     *
     * @param method the configuration accessor method being converted
     * @param input  the raw JSON mapping value, which may be null or blank
     * @return the parsed mapping, empty when the input is null or blank
     * @throws ConfigurationException if the input is not a JSON object or a value is not a non empty string
     */
    @Override
    public Map<String, String> convert(Method method, String input) {
        Map<String, String> mapping = new LinkedHashMap<>();
        if (input == null || input.trim().isEmpty()) {
            return mapping;
        }
        try {
            JSONObject jsonObject = new JSONObject(input);
            for (String fieldName : jsonObject.keySet()) {
                Object expression = jsonObject.get(fieldName);
                if (!(expression instanceof String) || ((String) expression).trim().isEmpty()) {
                    throw new ConfigurationException(
                            String.format("SINK_KAFKA_PROTO_MAPPING value for field %s should be a non empty CEL expression string", fieldName));
                }
                mapping.put(fieldName, (String) expression);
            }
        } catch (JSONException e) {
            throw new ConfigurationException("SINK_KAFKA_PROTO_MAPPING is not a valid JSON object: " + e.getMessage(), e);
        }
        return mapping;
    }
}
