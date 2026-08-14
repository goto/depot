package com.gotocompany.depot.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Small helper for rendering timestamps as UTC strings in a fixed ISO-8601-style format.
 *
 * <p>All formatting is performed in the UTC time zone using the pattern
 * {@code yyyy-MM-dd'T'HH:mm'Z'}, so the produced strings carry minute precision and a literal
 * {@code Z} suffix. The shared {@link java.text.DateFormat} is held statically; because
 * {@link java.text.SimpleDateFormat} is not thread-safe, callers must not assume these methods are
 * safe to invoke concurrently from multiple threads.</p>
 */
public class DateUtils {
    /** Fixed UTC time zone applied to every formatted timestamp. */
    private static final TimeZone TZ = TimeZone.getTimeZone("UTC");
    /** Shared formatter producing {@code yyyy-MM-dd'T'HH:mm'Z'} strings; not thread-safe. */
    private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
    static {
        DF.setTimeZone(TZ);
    }

    /**
     * Formats the current instant as a UTC timestamp string.
     *
     * @return the present time rendered with the pattern {@code yyyy-MM-dd'T'HH:mm'Z'} in UTC
     */
    public static String formatCurrentTimeAsUTC() {
        return formatTimeAsUTC(new Date());
    }

    /**
     * Formats the given date as a UTC timestamp string.
     *
     * @param date the instant to format
     * @return {@code date} rendered with the pattern {@code yyyy-MM-dd'T'HH:mm'Z'} in UTC
     * @throws NullPointerException if {@code date} is {@code null}
     */
    public static String formatTimeAsUTC(Date date) {
        return DF.format(date);
    }
}
