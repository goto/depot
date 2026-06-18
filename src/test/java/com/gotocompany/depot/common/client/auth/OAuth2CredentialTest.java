package com.gotocompany.depot.common.client.auth;

import com.google.gson.JsonSyntaxException;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.http.Header;
import org.apache.http.RequestLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.verify.VerificationTimes;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.NottableString.not;
import static org.mockserver.model.NottableString.string;

/**
 * Unit tests for {@link OAuth2Credential}, exercising its token injection and refresh behavior across
 * both the Apache HttpClient and OkHttp request paths.
 *
 * <p>Each test drives the credential through two stacks: an Apache {@link HttpClient} built by
 * registering the credential's interceptors via
 * {@link OAuth2Credential#initialize(org.apache.http.impl.client.HttpClientBuilder)}, and an
 * {@link OkHttpClient} configured with the credential as an OkHttp interceptor. A MockServer
 * ({@link ClientAndServer}) on port 1080 simulates the {@code /oauth2/token} endpoint and the
 * protected {@code /api} resource, and {@link VerificationTimes} assertions confirm how often each is
 * called. Joda-Time's {@link DateTimeUtils} clock is pinned in {@link #setUp()} so token-expiry
 * calculations are deterministic.
 */
public class OAuth2CredentialTest {
    /**
     * MockServer on port 1080 simulating the OAuth2 token endpoint and the protected API.
     */
    private ClientAndServer mockServer;
    /**
     * Apache GET request targeting the mock {@code /api} resource.
     */
    private HttpGet httpRequest;
    /**
     * The credential under test, configured with fixed client credentials and the mock token
     * endpoint.
     */
    private OAuth2Credential oAuth2Credential;
    /**
     * Apache {@link HttpClient} wired with the credential's request and response interceptors.
     */
    private HttpClient httpClient;
    /**
     * OkHttp client configured with the credential registered as an interceptor.
     */
    private OkHttpClient okHttpClient;

    /**
     * Mock metrics reporter backing the {@link Instrumentation} supplied to the credential.
     */
    @Mock
    private StatsDReporter statsDReporter;

    /**
     * Prepares the credential and both HTTP clients before each test.
     *
     * <p>Pins Joda-Time's clock via {@link DateTimeUtils#setCurrentMillisFixed(long)} for
     * deterministic expiry math, starts the MockServer on port 1080, creates an
     * {@link OAuth2Credential} for fixed client credentials, scope and token endpoint, builds an
     * Apache {@link HttpClient} with the credential's interceptors registered, and builds an
     * {@link OkHttpClient} using the credential as an interceptor.
     */
    @Before
    public void setUp() {
        DateTimeUtils.setCurrentMillisFixed(System.currentTimeMillis());
        mockServer = startClientAndServer(1080);
        httpRequest = new HttpGet("http://127.0.0.1:1080/api");
        String clientId = "clientId";
        String clientSecret = "clientSecret";
        String scope = "order:read";
        String accessTokenEndpoint = "http://127.0.0.1:1080/oauth2/token";
        oAuth2Credential = new OAuth2Credential(new Instrumentation(statsDReporter, OAuth2Credential.class), clientId, clientSecret, scope, accessTokenEndpoint);
        httpClient = oAuth2Credential.initialize(HttpClients.custom()).build();
        okHttpClient = new OkHttpClient.Builder().addInterceptor(oAuth2Credential).build();
    }

    /**
     * Stops the MockServer after each test.
     */
    @After
    public void tearDown() {
        mockServer.stop();
    }

    /**
     * Verifies that both HTTP stacks fetch a token and attach it as a bearer header.
     *
     * <p>Stubs the token endpoint and a protected {@code /api} resource that requires
     * {@code Authorization: Bearer ACCESSTOKEN}. Executing the Apache request triggers exactly one
     * token request and one API call; after clearing the cached token, executing the OkHttp request
     * triggers a second token request and API call, confirming both stacks obtain and embed the
     * bearer token.
     *
     * @throws IOException if either client fails to execute its request
     */
    @Test
    public void shouldEmbedBearerToken() throws IOException {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET")
                .withHeader("Authorization", "Bearer ACCESSTOKEN")
                .withHeader("foo", "bar");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(200).withBody("OK"));
        httpRequest.addHeader("foo", "bar");
        httpClient.execute(httpRequest);
        mockServer.verify(oauthRequest, VerificationTimes.exactly(1));
        mockServer.verify(getRequest, VerificationTimes.exactly(1));

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(oauthRequest, VerificationTimes.exactly(2));
        mockServer.verify(getRequest, VerificationTimes.exactly(2));
    }


    /**
     * Verifies that a cached, non-expired token is reused without contacting the token endpoint.
     *
     * <p>Pre-populates the credential with a token valid for 300 seconds, then executes the request on
     * the Apache client and again on the OkHttp client. Only the protected {@code /api} endpoint is
     * stubbed, and it is verified to receive both calls, demonstrating that no fresh token is
     * requested while the cached one remains valid.
     *
     * @throws Exception if either client fails to execute its request
     */
    @Test
    public void shouldReuseTokenWhenTokenExist() throws Exception {
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET")
                .withHeader("Authorization", "Bearer ACCESSTOKEN")
                .withHeader("foo", "bar");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(200).withBody("OK"));
        httpRequest.addHeader("foo", "bar");
        oAuth2Credential.setAccessToken(new OAuth2AccessToken("ACCESSTOKEN", 300));
        httpClient.execute(httpRequest);
        mockServer.verify(getRequest, VerificationTimes.exactly(1));

        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(getRequest, VerificationTimes.exactly(2));
    }

    /**
     * Verifies that an expired token triggers a fresh token request before the call proceeds.
     *
     * <p>After an initial Apache call obtains a token, the cached token is replaced with one whose
     * lifetime has already elapsed, and a second Apache call is shown to re-request it (token endpoint
     * and {@code /api} each hit twice). The same expired-token scenario is then repeated through the
     * OkHttp client, bringing both endpoints to four total invocations.
     *
     * @throws Exception if either client fails to execute its request
     */
    @Test
    public void shouldRequestTokenWhenTokenIsExpired() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET")
                .withHeader("Authorization", "Bearer ACCESSTOKEN")
                .withHeader("foo", "bar");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(200).withBody("OK"));
        httpRequest.addHeader("foo", "bar");
        httpClient.execute(httpRequest);
        oAuth2Credential.setAccessToken(new OAuth2AccessToken("ACCESSTOKEN", -1));
        httpClient.execute(httpRequest);
        mockServer.verify(oauthRequest, VerificationTimes.exactly(2));
        mockServer.verify(getRequest, VerificationTimes.exactly(2));

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        oAuth2Credential.setAccessToken(new OAuth2AccessToken("ACCESSTOKEN", -1));
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(oauthRequest, VerificationTimes.exactly(4));
        mockServer.verify(getRequest, VerificationTimes.exactly(4));
    }

    /**
     * Verifies that a token within the one-minute expiry window is refreshed before use.
     *
     * <p>Mirrors the expired-token scenario but seeds tokens with 60 seconds of remaining lifetime,
     * which {@link OAuth2AccessToken#isExpired()} treats as already expired. Each Apache and OkHttp
     * call is therefore shown to issue a new token request, driving the token endpoint and
     * {@code /api} to four invocations apiece.
     *
     * @throws Exception if either client fails to execute its request
     */
    @Test
    public void shouldRequestTokenWhenTokenIsExpiring() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET")
                .withHeader("Authorization", "Bearer ACCESSTOKEN")
                .withHeader("foo", "bar");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(200).withBody("OK"));
        httpRequest.addHeader("foo", "bar");
        httpClient.execute(httpRequest);
        oAuth2Credential.setAccessToken(new OAuth2AccessToken("ACCESSTOKEN", 60));
        httpClient.execute(httpRequest);
        mockServer.verify(oauthRequest, VerificationTimes.exactly(2));
        mockServer.verify(getRequest, VerificationTimes.exactly(2));

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        oAuth2Credential.setAccessToken(new OAuth2AccessToken("ACCESSTOKEN", 60));
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(oauthRequest, VerificationTimes.exactly(4));
        mockServer.verify(getRequest, VerificationTimes.exactly(4));
    }


    /**
     * Verifies that a token response omitting {@code expires_in} falls back to the default lifetime.
     *
     * <p>Stubs the token endpoint to return a body without an {@code expires_in} field. After an Apache
     * call and, separately, an OkHttp call, the cached token's remaining lifetime is asserted to be the
     * default of {@code 3600} seconds, confirming the fallback is applied on both stacks.
     *
     * @throws Exception if either client fails to execute its request
     */
    @Test
    public void shouldUseDefaultExpirationWhenAccessTokenIsIncomplete() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET")
                .withHeader("Authorization", "Bearer ACCESSTOKEN")
                .withHeader("foo", "bar");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(200).withBody("OK"));
        httpRequest.addHeader("foo", "bar");
        httpClient.execute(httpRequest);

        mockServer.verify(oauthRequest, VerificationTimes.exactly(1));
        mockServer.verify(getRequest, VerificationTimes.exactly(1));
        assertEquals(3600, (long) oAuth2Credential.getAccessToken().getExpiresIn());

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(oauthRequest, VerificationTimes.exactly(2));
        mockServer.verify(getRequest, VerificationTimes.exactly(2));
        assertEquals(3600, (long) oAuth2Credential.getAccessToken().getExpiresIn());
    }

    /**
     * Verifies that an HTTP 401 from the protected resource clears the cached token.
     *
     * <p>The token endpoint returns a valid token while {@code /api} responds with {@code 401}. After
     * the Apache call the cached token is asserted to be {@code null}, and the same is confirmed after
     * the OkHttp call, demonstrating that an unauthorized response invalidates the stored token on both
     * stacks so the next request re-authenticates.
     *
     * @throws Exception if either client fails to execute its request
     */
    @Test
    public void shouldClearTokenWhenServerReturnedHTTP401() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET")
                .withHeader("foo", "bar");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(401).withBody("{\n"
                        + "  \"error\": \"invalid_request\",\n"
                        + "  \"error_description\": \"Some Description\",\n"
                        + "  \"error_uri\": \"See the full API docs at https://authorization-server.com/docs/access_token\"\n"
                        + "}"));
        httpRequest.addHeader("foo", "bar");
        httpClient.execute(httpRequest);
        mockServer.verify(oauthRequest, VerificationTimes.exactly(1));
        mockServer.verify(getRequest, VerificationTimes.exactly(1));
        assertNull(oAuth2Credential.getAccessToken());

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(oauthRequest, VerificationTimes.exactly(2));
        mockServer.verify(getRequest, VerificationTimes.exactly(2));
        assertNull(oAuth2Credential.getAccessToken());
    }

    /**
     * Verifies that a non-JSON token response surfaces a {@link JsonSyntaxException} on the Apache
     * path.
     *
     * <p>The token endpoint returns a {@code 503} with a plain-text body. When the Apache client
     * executes the request, parsing that body as the token JSON fails, and because the error is
     * unchecked it propagates out of {@code execute} rather than being swallowed by the credential.
     *
     * @throws Exception if the request fails for a reason other than the expected JSON parse error
     */
    @Test(expected = JsonSyntaxException.class)
    public void shouldThrowExceptionWhenServerHasJsonException() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(503).withBody("Gateway not available"));
        httpRequest.addHeader("foo", "bar");
        httpClient.execute(httpRequest);
    }

    /**
     * Verifies that a non-JSON token response surfaces a {@link JsonSyntaxException} on the OkHttp
     * path.
     *
     * <p>Mirrors {@link #shouldThrowExceptionWhenServerHasJsonException()} but drives the request
     * through the OkHttp client, asserting the unchecked JSON parse error propagates from the OkHttp
     * call.
     *
     * @throws Exception if the request fails for a reason other than the expected JSON parse error
     */
    @Test(expected = JsonSyntaxException.class)
    public void shouldThrowExceptionWhenServerHasJsonExceptionOkHttp() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(503).withBody("Gateway not available"));
        httpRequest.addHeader("foo", "bar");
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
    }

    /**
     * Verifies that an OAuth error response from the token endpoint does not fail the request.
     *
     * <p>The token endpoint returns a {@code 400} OAuth error body, which surfaces as an
     * {@link com.gotocompany.depot.common.exception.OAuth2Exception}; because that type extends
     * {@link IOException}, the credential catches and logs it and lets the request proceed
     * unauthenticated to {@code /api} (stubbed to return {@code 401}). The token endpoint and
     * {@code /api} are verified to be called once on the Apache path and, after clearing the token, a
     * second time on the OkHttp path.
     *
     * @throws Exception if either client fails for a reason other than the handled OAuth error
     */
    @Test
    public void shouldNotThrowExceptionWhenServerHasOAuthException() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(400).withBody("{\n"
                        + "  \"error\": \"invalid_request\",\n"
                        + "  \"error_description\": \"Request was missing the 'redirect_uri' parameter.\",\n"
                        + "  \"error_uri\": \"See the full API docs at https://authorization-server.com/docs/access_token\"\n"
                        + "}"));
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(401).withBody("Authentication error"));
        httpClient.execute(httpRequest);
        mockServer.verify(oauthRequest, VerificationTimes.exactly(1));
        mockServer.verify(getRequest, VerificationTimes.exactly(1));

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(oauthRequest, VerificationTimes.exactly(2));
        mockServer.verify(getRequest, VerificationTimes.exactly(2));
    }

    /**
     * Verifies that a slow token endpoint does not fail the request.
     *
     * <p>The token endpoint is stubbed to respond only after a six-second delay, exceeding the client's
     * five-second timeout, while {@code /api} is stubbed to match requests that carry no
     * {@code Authorization} header. The timeout is handled internally so the request proceeds
     * unauthenticated; {@code /api} is verified to receive one Apache call and, after clearing the
     * token, a second OkHttp call.
     *
     * @throws Exception if either client fails for a reason other than the handled timeout
     */
    @Test
    public void shouldNotThrowExceptionWhenServerHasOAuthTimeout() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200)
                        .withDelay(TimeUnit.SECONDS, 6)
                        .withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        HttpRequest getRequest = request().withPath("/api")
                .withHeader(not("Authorization"), string(".*"))
                .withMethod("GET");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(200).withBody("No error"));

        httpClient.execute(httpRequest);
        mockServer.verify(getRequest, VerificationTimes.exactly(1));

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(getRequest, VerificationTimes.exactly(2));
    }

    /**
     * Converts an Apache HTTP request into an equivalent OkHttp {@link Request}.
     *
     * <p>Copies the request line's URI and all headers onto a new OkHttp request builder, additionally
     * setting fixed {@code User-Agent} and {@code Accept-Encoding} headers, so the same logical request
     * can be replayed through the {@link OkHttpClient}.
     *
     * @param request the Apache HTTP request to translate
     * @return an OkHttp {@link Request} mirroring the given request's URI and headers
     */
    private Request transformRequest(org.apache.http.HttpRequest request) {
        Request.Builder builder = new Request.Builder();
        RequestLine requestLine = request.getRequestLine();
        Header[] headers = request.getAllHeaders();
        for (Header header : headers) {
            builder = builder.header(header.getName(), header.getValue());
        }
        builder.header("User-Agent", "Apache-HttpClient/4.5.2 (Java/1.8.0_252)");
        builder.header("Accept-Encoding", "gzip,deflate");
        return builder.url(requestLine.getUri()).build();
    }
}
