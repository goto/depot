package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.time.Duration;

public class DurationConverter implements Converter<Duration> {

    @Override
    public Duration convert(Method method, String s) {
        return Duration.parse(s);
    }

}
