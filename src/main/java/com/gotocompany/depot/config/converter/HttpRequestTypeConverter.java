package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.http.enums.HttpRequestType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class HttpRequestTypeConverter implements Converter<HttpRequestType> {

    @Override
    public HttpRequestType convert(Method method, String input) {
        return HttpRequestType.valueOf(input.toUpperCase());
    }
}
