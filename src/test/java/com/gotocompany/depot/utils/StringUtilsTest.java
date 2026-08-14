package com.gotocompany.depot.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link StringUtils}, the helper that analyzes Java format strings for Depot's
 * templating.
 *
 * <p>The cases exercise {@link StringUtils#countVariables(String)}, which reports how many positional
 * arguments a format pattern consumes, and {@link StringUtils#count(String, char)}, which tallies
 * occurrences of a character. Together these underpin validation of a template's placeholder count
 * against its declared field names.
 */
public class StringUtilsTest {

    /**
     * Verifies that {@link StringUtils#countVariables(String)} counts format placeholders correctly.
     *
     * <p>Asserts that plain and empty strings yield {@code 0}, and that strings containing one, two
     * and five conversion specifiers yield {@code 1}, {@code 2} and {@code 5} respectively, across a
     * mix of {@code %d}, {@code %t}, {@code %s} and {@code %b} conversions.
     */
    @Test
    public void shouldReturnValidArgumentsForStringFormat() {
        Assert.assertEquals(0, StringUtils.countVariables("test"));
        Assert.assertEquals(0, StringUtils.countVariables(""));
        Assert.assertEquals(1, StringUtils.countVariables("test%dtest"));
        Assert.assertEquals(2, StringUtils.countVariables("test%dtest%ttest"));
        Assert.assertEquals(5, StringUtils.countVariables("test%dtest%ttest dskladja %s ds %d sdajk %b"));
    }

    /**
     * Verifies that {@link StringUtils#count(String, char)} counts character occurrences correctly.
     *
     * <p>Asserts the count is {@code 0} for an absent character and for an empty string, and returns
     * the exact number of occurrences for characters present once or multiple times, including
     * confirming that {@code '%'} is absent from {@code "test"}.
     */
    @Test
    public void shouldReturnCharacterCount() {
        Assert.assertEquals(0, StringUtils.count("test", 'i'));
        Assert.assertEquals(0, StringUtils.count("", '5'));
        Assert.assertEquals(2, StringUtils.count("test", 't'));
        Assert.assertEquals(1, StringUtils.count("test", 'e'));
        Assert.assertEquals(1, StringUtils.count("test", 's'));
        Assert.assertEquals(0, StringUtils.count("test", '%'));
    }

    /**
     * Verifies that placeholder and character counts are computed independently on the same string.
     *
     * <p>For {@code "test%s%ddjaklsjd%%%%s%y%d"}, asserts {@link StringUtils#count(String, char)}
     * finds eight {@code '%'} characters while {@link StringUtils#countVariables(String)} finds only
     * four valid conversions, since escaped {@code %} pairs and the invalid {@code %y} conversion are
     * not counted as arguments.
     */
    @Test
    public void shouldReturnValidArgsAndCharacters() {
        String testString = "test%s%ddjaklsjd%%%%s%y%d";
        Assert.assertEquals(8, StringUtils.count(testString, '%'));
        Assert.assertEquals(4, StringUtils.countVariables(testString));
    }
}
