package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Owner {@link Converter} that expands a comma-separated list of inclusive integer ranges into a
 * {@code Map<Integer, Boolean>}.
 *
 * <p>Each entry in the input is a range expressed as {@code start-end}; every integer in the closed
 * interval between the two bounds becomes a key in the resulting map and is associated with
 * {@link Boolean#TRUE}. A typical use within Depot is to enumerate the individual HTTP status codes
 * covered by one or more status-code ranges, for example {@code "200-208,400-499"}.
 *
 * <p>The returned map only ever contains {@code true} values; integers that fall outside the
 * configured ranges are simply absent as keys.
 *
 * @see Converter
 */
public class RangeToHashMapConverter implements Converter<Map<Integer, Boolean>> {

    /**
     * Expands the configured ranges into a map of individual integers, each mapped to {@code true}.
     *
     * <p>The input is split on commas into individual range tokens. Each token must contain a
     * {@code "-"} separating its inclusive lower and upper bounds; the token is trimmed, split on
     * {@code "-"}, and both halves are parsed as integers. Every integer in the closed interval
     * between the two bounds is then inserted into the result map with the value {@code true}.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value, a comma-separated list of {@code start-end} ranges
     * @return a map associating every integer covered by the configured ranges with {@code true}
     * @throws IllegalArgumentException  if a range token does not contain the {@code "-"} separator
     * @throws NumberFormatException     if either bound of a range token is not a valid integer
     * @throws IndexOutOfBoundsException if a range token does not supply both a lower and an upper bound
     * @throws NullPointerException      if {@code input} is {@code null}
     */
    @Override
    public Map<Integer, Boolean> convert(Method method, String input) {
        String[] ranges = input.split(",");
        Map<Integer, Boolean> statusMap = new HashMap<Integer, Boolean>();

        Arrays.stream(ranges).forEach(range -> {
            if (!range.contains("-")) {
                throw new IllegalArgumentException("input value '" + range + "' is not a valid range");
            }
            List<Integer> rangeList = Arrays.stream(range.trim().split("-")).map(Integer::parseInt).collect(Collectors.toList());
            IntStream.rangeClosed(rangeList.get(0), rangeList.get(1)).forEach(statusCode -> statusMap.put(statusCode, true));
        });
        return statusMap;
    }
}
