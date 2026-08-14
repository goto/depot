package com.gotocompany.depot.common.client.auth;

import org.joda.time.DateTimeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link OAuth2AccessToken}, covering its expiry detection and remaining-lifetime
 * reporting.
 *
 * <p>Because {@link OAuth2AccessToken} computes expiry against the current time, {@link #setUp()}
 * pins Joda-Time's {@link DateTimeUtils} clock so the tests are deterministic. The cases probe the
 * one-minute safety margin used by {@link OAuth2AccessToken#isExpired()} and confirm that
 * {@link OAuth2AccessToken#getExpiresIn()} reflects the supplied lifetime, defaulting to one hour
 * when none is given.
 */
public class OAuth2AccessTokenTest {

    /**
     * The token instance under test, re-created per scenario with a specific lifetime.
     */
    private OAuth2AccessToken oAuth2AccessToken;
    /**
     * The raw access-token string used to construct the tokens under test.
     */
    private String accessToken;

    /**
     * Pins the clock and prepares the token string before each test.
     *
     * <p>Fixes Joda-Time's clock via {@link DateTimeUtils#setCurrentMillisFixed(long)} so expiry
     * calculations are deterministic and sets the sample access-token string.
     */
    @Before
    public void setUp() {
        DateTimeUtils.setCurrentMillisFixed(System.currentTimeMillis());
        accessToken = "SAMPLE-TOKEN";
    }

    /**
     * Verifies that a token with under one minute of life is reported as expired.
     *
     * <p>Constructs a token with 55 seconds of lifetime and asserts
     * {@link OAuth2AccessToken#isExpired()} returns {@code true}, reflecting the one-minute safety
     * margin.
     */
    @Test
    public void shouldReturnTrueWhenExpireTimeIsLessThan60Sec() {
        oAuth2AccessToken = new OAuth2AccessToken(accessToken, 55);

        Assert.assertTrue(oAuth2AccessToken.isExpired());
    }

    /**
     * Verifies that a token with exactly one minute of life is reported as expired.
     *
     * <p>Constructs a token with 60 seconds of lifetime and asserts
     * {@link OAuth2AccessToken#isExpired()} returns {@code true}, confirming the boundary is treated
     * as expired.
     */
    @Test
    public void shouldReturnTrueWhenExpireTimeIs60Sec() {
        oAuth2AccessToken = new OAuth2AccessToken(accessToken, 60);

        Assert.assertTrue(oAuth2AccessToken.isExpired());
    }

    /**
     * Verifies that a token with more than one minute of life is not reported as expired.
     *
     * <p>Constructs a token with 62 seconds of lifetime and asserts
     * {@link OAuth2AccessToken#isExpired()} returns {@code false}.
     */
    @Test
    public void shouldReturnFalseWhenExpireTimeIsMoreThan60Sec() {
        oAuth2AccessToken = new OAuth2AccessToken(accessToken, 62);

        Assert.assertFalse(oAuth2AccessToken.isExpired());
    }

    /**
     * Verifies that the remaining lifetime mirrors the supplied {@code expires_in} value.
     *
     * <p>Constructs a token with 65 seconds of lifetime and, with the clock pinned, asserts
     * {@link OAuth2AccessToken#getExpiresIn()} returns {@code 65}.
     */
    @Test
    public void shouldReturnExpirationTimeAsPassedInParamaters() {
        Long expiresIn = 65L;
        oAuth2AccessToken = new OAuth2AccessToken(accessToken, 65);

        Assert.assertEquals(expiresIn, oAuth2AccessToken.getExpiresIn());
    }

    /**
     * Verifies that a {@code null} lifetime falls back to the one-hour default.
     *
     * <p>Constructs a token with a {@code null} {@code expires_in} and, with the clock pinned, asserts
     * {@link OAuth2AccessToken#getExpiresIn()} returns the default of {@code 3600} seconds.
     */
    @Test
    public void shouldReturnDefaultExpirationTimeWhenNotPassedInParamaters() {
        Long expiresIn = 3600L;
        oAuth2AccessToken = new OAuth2AccessToken(accessToken, null);

        Assert.assertEquals(expiresIn, oAuth2AccessToken.getExpiresIn());
    }
}
