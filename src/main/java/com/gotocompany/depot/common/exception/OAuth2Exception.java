package com.gotocompany.depot.common.exception;

import java.io.IOException;

/**
 * Checked exception signaling a failure during OAuth2 token acquisition.
 *
 * <p>{@code OAuth2Exception} extends {@link IOException} so that callers handling I/O failures of the
 * HTTP token request also handle authorization failures uniformly. It is thrown by
 * {@link com.gotocompany.depot.common.client.auth.OAuth2Client} when the token endpoint returns a
 * non-success (non-2xx) HTTP status, carrying the error message reported by the authorization server.
 */
public class OAuth2Exception extends IOException {
    /**
     * Creates an exception with the supplied detail message.
     *
     * @param message the detail message describing the OAuth2 failure
     */
    public OAuth2Exception(String message) {
        super(message);
    }
}

