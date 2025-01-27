package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeyValuePairsToMapConverter implements Converter<Map<String, String>> {

    private static final String KEY_VALUE_SEPARATOR = "=";
    private static final String ENTRY_SEPARATOR = ",";

    @Override
    public Map<String, String> convert(Method method, String s) {
        if (StringUtils.isEmpty(s)) {
            return Collections.emptyMap();
        }
        return Stream.of(s.split(ENTRY_SEPARATOR))
                .map(entry -> entry.split(KEY_VALUE_SEPARATOR))
                .collect(Collectors.toMap(
                        entry -> entry[0],
                        entry -> entry[1],
                        (entry1, entry2) -> entry2
                ));
    }

}
