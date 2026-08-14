package com.gotocompany.depot.common.client.auth;

import org.joda.time.DateTimeUtils;

/**
 * Holder for an OAuth2 bearer token together with its computed expiry.
 *
 * <p>{@code OAuth2AccessToken} stores the raw access-token string and the absolute time, in
 * milliseconds, at which the token expires. The expiry is derived at construction from the
 * {@code expires_in} value (in seconds) returned by the authorization server, defaulting to one hour
 * when that value is absent. Instances are produced by {@link OAuth2Client} and consumed by
 * {@link OAuth2Credential} when attaching the {@code Authorization} header to outbound requests.
 */
public class OAuth2AccessToken {
    /**
     * The raw OAuth2 access-token string presented as a bearer credential.
     */
    private final String accessToken;
    /**
     * The absolute expiry time of the token, in epoch milliseconds.
     */
    private final Long expirationTimeMs;
    /**
     * Default token lifetime, in seconds ({@code 3600}, i.e. one hour), applied when the server omits
     * an {@code expires_in} value.
     */
    private static final int DEFAULT_EXPIRATION_TIME = 3600;
    /**
     * Number of milliseconds in one second, used to convert between seconds and milliseconds.
     */
    private static final long MILLIS = 1000L;

    /**
     * Creates an access token and computes its absolute expiry time.
     *
     * <p>The expiry is calculated as the current time plus {@code expiresIn} seconds. When
     * {@code expiresIn} is {@code null}, a default lifetime of one hour ({@code 3600} seconds) is
     * used instead.
     *
     * @param accessToken the raw access-token string
     * @param expiresIn   the token lifetime in seconds as reported by the authorization server, or
     *                    {@code null} to apply the default lifetime
     */
    public OAuth2AccessToken(String accessToken, Integer expiresIn) {
        this.accessToken = accessToken;
        expiresIn = expiresIn == null ? DEFAULT_EXPIRATION_TIME : expiresIn;
        this.expirationTimeMs = DateTimeUtils.currentTimeMillis() + (expiresIn * MILLIS);
    }

    /**
     * Indicates whether the token is expired or about to expire.
     *
     * <p>To provide a safety margin against clock skew and in-flight requests, the token is treated
     * as expired once one minute or less of validity remains before its actual expiry.
     *
     * @return {@code true} if at most one minute of validity remains, {@code false} otherwise
     */
    public boolean isExpired() {
        final long oneMinute = 60L;
        return this.getExpiresIn() <= oneMinute;
    }

    /**
     * Returns the raw access-token string.
     *
     * <p>This is the value embedded directly into the {@code Authorization: Bearer} header, so the
     * token's string form is its underlying credential rather than a diagnostic description.
     *
     * @return the raw access-token string
     */
    public String toString() {
        return this.accessToken;
    }

    /**
     * Returns the number of whole seconds remaining until the token expires.
     *
     * <p>Computed as the difference between the stored expiry time and the current time, converted
     * from milliseconds to seconds. The result is negative once the token has already expired.
     *
     * @return the remaining validity of the token in seconds, which may be negative if it has expired
     */
    public Long getExpiresIn() {
        return (this.expirationTimeMs - DateTimeUtils.currentTimeMillis()) / MILLIS;
    }
}

