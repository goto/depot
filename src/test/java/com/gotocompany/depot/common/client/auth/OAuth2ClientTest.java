package com.gotocompany.depot.common.client.auth;

import com.gotocompany.depot.common.exception.OAuth2Exception;
import org.joda.time.DateTimeUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;

import java.io.IOException;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * Unit tests for {@link OAuth2Client}, verifying the client-credentials token request against a
 * mocked authorization server.
 *
 * <p>A MockServer ({@link ClientAndServer}) on port 1080 stands in for the {@code /oauth2/token}
 * endpoint and is matched on the exact form-encoded body the client posts. The tests assert that a
 * successful (2xx) response is parsed into an {@link OAuth2AccessToken} and that a non-2xx response
 * raises an {@link OAuth2Exception}. Joda-Time's {@link DateTimeUtils} clock is pinned in
 * {@link #setUp()} for deterministic token-expiry calculations.
 */
public class OAuth2ClientTest {

    /**
     * The client under test, configured with fixed credentials, scope and the mock token endpoint.
     */
    private OAuth2Client oAuth2Client;
    /**
     * Shared MockServer, started once on port 1080, that stubs the token endpoint.
     */
    private static ClientAndServer mockServer;

    /**
     * Starts the shared MockServer on port 1080 once before any test in the class runs.
     */
    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer(1080);
    }

    /**
     * Stops the shared MockServer after all tests in the class have completed.
     */
    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    /**
     * Resets the MockServer and recreates the client before each test.
     *
     * <p>Pins Joda-Time's clock via {@link DateTimeUtils#setCurrentMillisFixed(long)} for
     * deterministic expiry math and constructs a fresh {@link OAuth2Client} for fixed client
     * credentials, scope and the mock token endpoint.
     */
    @Before
    public void setUp() {
        DateTimeUtils.setCurrentMillisFixed(System.currentTimeMillis());
        mockServer.reset();
        String clientId = "clientId";
        String clientSecret = "clientSecret";
        String scope = "order:read";
        String accessTokenEndpoint = "http://127.0.0.1:1080/oauth2/token";
        oAuth2Client = new OAuth2Client(clientId, clientSecret, scope, accessTokenEndpoint);

    }

    /**
     * Verifies that a non-2xx token response raises an {@link OAuth2Exception}.
     *
     * <p>Stubs the token endpoint to answer the client-credentials request with a {@code 400} status
     * and asserts that {@link OAuth2Client#requestClientCredentialsGrantAccessToken()} throws an
     * {@link OAuth2Exception} rather than returning a token.
     *
     * @throws IOException if the request fails for a reason other than the expected OAuth error
     */
    @Test(expected = OAuth2Exception.class)
    public void shouldThrowOAuth2ExceptionIfResponseReturnedIsNon2XX() throws IOException {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(400).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));

        oAuth2Client.requestClientCredentialsGrantAccessToken();
    }

    /**
     * Verifies that a {@code 200} token response is parsed into an access token.
     *
     * <p>Stubs the token endpoint to return {@code access_token=ACCESSTOKEN} with
     * {@code expires_in=3599}, then asserts the returned {@link OAuth2AccessToken} stringifies to
     * {@code "ACCESSTOKEN"} and reports {@code 3599} seconds of remaining validity (the clock being
     * pinned).
     *
     * @throws IOException if the token request fails
     */
    @Test
    public void shouldReturnOAuth2AccessTokenIfResponseReturnedIs200() throws IOException {
        Long expiresIn = 3599L;
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));

        OAuth2AccessToken oAuth2AccessToken = oAuth2Client.requestClientCredentialsGrantAccessToken();

        Assert.assertEquals("ACCESSTOKEN", oAuth2AccessToken.toString());
        Assert.assertEquals(expiresIn, oAuth2AccessToken.getExpiresIn());
    }

    /**
     * Verifies that a {@code 201} token response is also treated as success.
     *
     * <p>Identical to the {@code 200} case but with the token endpoint returning {@code 201}; asserts
     * the response is still parsed into an {@link OAuth2AccessToken} that stringifies to
     * {@code "ACCESSTOKEN"} and reports {@code 3599} seconds of remaining validity, confirming the
     * success check matches all 2xx codes.
     *
     * @throws IOException if the token request fails
     */
    @Test
    public void shouldReturnOAuth2AccessTokenIfResponseReturnedIs201() throws IOException {
        Long expiresIn = 3599L;
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(201).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));

        OAuth2AccessToken oAuth2AccessToken = oAuth2Client.requestClientCredentialsGrantAccessToken();

        Assert.assertEquals("ACCESSTOKEN", oAuth2AccessToken.toString());
        Assert.assertEquals(expiresIn, oAuth2AccessToken.getExpiresIn());
    }
}
