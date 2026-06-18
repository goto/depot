package com.gotocompany.depot.config.converter;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Map;

/**
 * Unit tests for {@link MaxComputeOdpsGlobalSettingsConverter}, the Owner converter that parses a
 * comma-separated list of {@code key=value} ODPS global settings into a {@code Map<String, String>}.
 *
 * <p>Each test instantiates a fresh converter and invokes
 * {@link MaxComputeOdpsGlobalSettingsConverter#convert} with a {@code null} accessor method.
 * Assertions are made with JUnit Jupiter's {@link org.junit.jupiter.api.Assertions} even though the
 * cases are driven by the JUnit 4 {@link org.junit.Test} runner; the scenarios cover well-formed
 * input, blank input, {@code null} input and malformed input.
 */
public class MaxComputeOdpsGlobalSettingsConverterTest {

    /**
     * Verifies that {@link MaxComputeOdpsGlobalSettingsConverter#convert} parses entries while
     * trimming surrounding whitespace.
     *
     * <p>Given two whitespace-padded {@code key=value} settings separated by a comma, when
     * {@code convert} is invoked, then the result is a two-entry map mapping
     * {@code odps.schema.evolution.enable} to {@code "true"} and {@code odps.task.major.version} to
     * {@code "sql_flighting_autopt"}, with the leading and trailing spaces removed.
     */
    @Test
    public void shouldParseOdpsGlobalSettings() {
        MaxComputeOdpsGlobalSettingsConverter converter = new MaxComputeOdpsGlobalSettingsConverter();
        String odpsGlobalSettings = "odps.schema.evolution.enable=true ,  odps.task.major.version=  sql_flighting_autopt ";

        Map<String, String> settings = converter.convert(null, odpsGlobalSettings);

        Assertions.assertEquals(2, settings.size());
        Assertions.assertTrue(settings.containsKey("odps.schema.evolution.enable"));
        Assertions.assertEquals("true", settings.get("odps.schema.evolution.enable"));
        Assertions.assertTrue(settings.containsKey("odps.task.major.version"));
        Assertions.assertEquals("sql_flighting_autopt", settings.get("odps.task.major.version"));
    }

    /**
     * Verifies that {@link MaxComputeOdpsGlobalSettingsConverter#convert} returns an empty map for a
     * blank input string.
     *
     * <p>Given a string consisting only of spaces, when {@code convert} is invoked, then the result
     * is an empty map.
     */
    @Test
    public void shouldParseEmptyMapWhenGivenStringIsEmpty() {
        MaxComputeOdpsGlobalSettingsConverter converter = new MaxComputeOdpsGlobalSettingsConverter();
        String odpsGlobalSettings = "         ";

        Map<String, String> settings = converter.convert(null, odpsGlobalSettings);

        Assertions.assertEquals(0, settings.size());
    }

    /**
     * Verifies that {@link MaxComputeOdpsGlobalSettingsConverter#convert} returns an empty map for
     * {@code null} input.
     *
     * <p>Given a {@code null} input, when {@code convert} is invoked, then the result is an empty
     * map.
     */
    @Test
    public void shouldParseEmptyMapWhenGivenStringIsNull() {
        MaxComputeOdpsGlobalSettingsConverter converter = new MaxComputeOdpsGlobalSettingsConverter();

        Map<String, String> settings = converter.convert(null, null);

        Assertions.assertEquals(0, settings.size());
    }

    /**
     * Verifies that {@link MaxComputeOdpsGlobalSettingsConverter#convert} rejects malformed entries.
     *
     * <p>Given an entry that uses {@code +} instead of {@code =} as its key-value separator, when
     * {@code convert} is invoked, then an {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentException() {
        MaxComputeOdpsGlobalSettingsConverter converter = new MaxComputeOdpsGlobalSettingsConverter();
        String odpsGlobalSettings = "odps.schema.evolution.enable+true ,  odps.task.major.version=sql_flighting_autopt";

        converter.convert(null, odpsGlobalSettings);
    }

}
