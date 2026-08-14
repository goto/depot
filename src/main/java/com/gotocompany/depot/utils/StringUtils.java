package com.gotocompany.depot.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

/**
 * String helpers for working with Java format strings and character counting.
 *
 * <p>These utilities back Depot's templating, where a configured template is a
 * {@link String#format(String, Object...)} pattern paired with the field names that supply its
 * arguments. {@link #countVariables(String)} reports how many positional arguments a pattern consumes,
 * and {@link #count(String, char)} tallies occurrences of a character, together allowing a template's
 * argument count to be validated against its declared fields.</p>
 */
public class StringUtils {

    /**
     * Regular expression matching a single format specifier within a Java format string.
     *
     * <p>Approximates the grammar accepted by {@link java.util.Formatter}: an optional argument index
     * ({@code n$}), optional flags, width and precision, and a conversion character. It deliberately
     * does not match an escaped {@code %%} and is used by {@link #countVariables(String)} to count the
     * positional arguments a pattern requires.</p>
     */
    private static final Pattern PATTERN = Pattern.compile("(?!<%)%"
            + "(?:(\\d+)\\$)?"
            + "([-#+ 0,(]|<)?"
            + "\\d*"
            + "(?:\\.\\d+)?"
            + "(?:[bBhHsScCdoxXeEfgGaAtT]|"
            + "[tT][HIklMSLNpzZsQBbhAaCYyjmdeRTrDFc])");

    /**
     * Counts the number of arguments a Java format string consumes.
     *
     * <p>Scans {@code fmt} for format specifiers and returns the larger of two quantities: the number
     * of specifiers that consume a fresh positional argument (excluding those that reuse the previous
     * argument), and the highest explicit argument index referenced through the {@code n$} syntax. This
     * yields the count of distinct arguments the pattern needs.</p>
     *
     * @param fmt the format string to analyze
     * @return the number of arguments required to satisfy {@code fmt}
     */
    public static int countVariables(String fmt) {
        Matcher m = PATTERN.matcher(fmt);
        int np = 0;
        int maxref = 0;
        while (m.find()) {
            if (m.group(1) != null) {
                String dec = m.group(1);
                int ref = Integer.parseInt(dec);
                maxref = Math.max(ref, maxref);
            } else if (!(m.group(2) != null && "<".equals(m.group(2)))) {
                np++;
            }
        }
        return Math.max(np, maxref);
    }

    /**
     * Counts how many times a character occurs in a string.
     *
     * @param in the string to scan
     * @param c the character to count
     * @return the number of occurrences of {@code c} in {@code in}
     */
    public static int count(String in, char c) {
        return IntStream.range(0, in.length()).
                reduce(0, (x, y) -> x + (in.charAt(y) == c ? 1 : 0));
    }
}
