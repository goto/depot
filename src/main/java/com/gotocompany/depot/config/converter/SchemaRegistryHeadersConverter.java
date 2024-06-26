package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;
import org.aeonbits.owner.Tokenizer;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.BasicHeader;

import java.lang.reflect.Method;
import java.util.regex.Pattern;

public class SchemaRegistryHeadersConverter implements Converter<Header>, Tokenizer {

    @Override
    public Header convert(Method method, String input) {
        String[] split = input.split(":");
        return new BasicHeader(split[0].trim(), split[1].trim());
    }

    @Override
    public String[] tokens(String values) {
        String[] headers = Pattern.compile(",").splitAsStream(values).map(String::trim)
                .filter(s -> {
                    String[] args = s.split(":");
                    return args.length == 2 && args[0].trim().length() > 0 && args[1].trim().length() > 0;
                }).toArray(String[]::new);
        if (headers.length == 0) {
            throw new IllegalArgumentException(String.format("provided headers %s is not valid", values));
        }

        return headers;
    }

}
