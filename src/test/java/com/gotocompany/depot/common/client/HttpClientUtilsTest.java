package com.gotocompany.depot.common.client;

import com.gotocompany.depot.config.HttpClientConfig;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gradle.internal.impldep.org.junit.Before;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.verify.VerificationTimes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * Unit tests for {@link HttpClientUtils#newHttpClient(HttpClientConfig, StatsDReporter)}.
 *
 * <p>The suite verifies how the factory wires OAuth2 support into the Apache HTTP client it builds. A
 * MockServer ({@link ClientAndServer}) listening on port 1080 stands in for both the protected
 * {@code /api} resource and the {@code /oauth2/token} endpoint, and the tests assert whether a token
 * request is issued based on the {@code SINK_HTTPV2_OAUTH2_ENABLE} configuration flag.
 *
 * <p>The MockServer is started once for the class via {@link #startServer()} and torn down via
 * {@link #stopServer()}, while {@link #startMockServer()} resets and re-primes its expectations
 * before each test. Note that {@link #startMockServer()} carries JUnit's {@code @org.junit.Before}
 * annotation whereas {@link #setup()} is annotated with the Gradle-shaded
 * {@code org.gradle.internal.impldep.org.junit.Before}.
 */
public class HttpClientUtilsTest {

    /**
     * Shared MockServer, started once on port 1080, that stubs the OAuth2 token and API endpoints.
     */
    private static ClientAndServer mockServer;
    /**
     * Mock metrics reporter passed to the factory and used to instrument the OAuth2 credential when
     * OAuth2 is enabled.
     */
    @Mock
    private StatsDReporter statsDReporter;
    /**
     * The HTTP GET request issued against the mock {@code /api} endpoint in each test.
     */
    private HttpGet httpRequest;

    /**
     * The Owner-backed HTTP client configuration assembled per test from a property map.
     */
    private HttpClientConfig clientConfig;

    /**
     * Initializes this instance's Mockito-annotated fields.
     *
     * <p>Invokes {@code initMocks(this)} to populate {@link #statsDReporter}. This method is annotated
     * with the Gradle-shaded {@code org.gradle.internal.impldep.org.junit.Before} rather than JUnit's
     * own {@code Before}.
     */
    @Before
    public void setup() {
        initMocks(this);
    }

    /**
     * Starts the shared MockServer once before any test in the class runs.
     *
     * <p>Binds the {@link ClientAndServer} instance to port 1080 so it can later be primed with stub
     * responses.
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
     * Resets the MockServer and primes its stub responses before each test.
     *
     * <p>Clears any previously registered expectations, then stubs {@code /oauth2/token} to return a
     * bearer-token JSON body and {@code /api} to return {@code 200 OK}. This method carries JUnit's
     * {@code @org.junit.Before} annotation and therefore runs as a JUnit lifecycle hook.
     */
    @org.junit.Before
    public void startMockServer() {
        mockServer.reset();
        mockServer.when(request().withPath("/oauth2/token"))
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        mockServer.when(request().withPath("/api"))
                .respond(response().withStatusCode(200).withBody("OK"));
    }

    /**
     * Verifies that no token request is made when OAuth2 is disabled.
     *
     * <p>Builds a client from configuration with {@code SINK_HTTPV2_OAUTH2_ENABLE=false}, executes a
     * GET against the mock {@code /api} endpoint, and asserts the {@code /oauth2/token} endpoint is
     * never called. The {@code expected = Test.None.class} declaration documents that the request must
     * complete without throwing.
     *
     * @throws IOException if executing the HTTP request fails
     */
    @Test(expected = Test.None.class)
    public void shouldNotEmbedAccessTokenIfGoAuthDisabled() throws IOException {
        httpRequest = new HttpGet("http://127.0.0.1:1080/api");
        httpRequest.addHeader("foo", "bar");

        Map<String, String> configuration = new HashMap<>();
        configuration.put("SINK_HTTPV2_OAUTH2_ENABLE", "false");
        configuration.put("SINK_HTTPV2_OAUTH2_ACCESS_TOKEN_URL", "http://127.0.0.1:1080/oauth2/token");
        clientConfig = ConfigFactory.create(HttpClientConfig.class, configuration);
        CloseableHttpClient closeableHttpClient = HttpClientUtils.newHttpClient(clientConfig, statsDReporter);

        closeableHttpClient.execute(httpRequest);

        mockServer.verify(request().withPath("/oauth2/token"), VerificationTimes.exactly(0));
    }

    /**
     * Verifies that a token is fetched once when OAuth2 is enabled.
     *
     * <p>Builds a client from configuration with {@code SINK_HTTPV2_OAUTH2_ENABLE=true}, executes a
     * GET against the mock {@code /api} endpoint, and asserts the {@code /oauth2/token} endpoint is
     * called exactly once to obtain the bearer token embedded on the outbound request. The
     * {@code expected = Test.None.class} declaration documents that the request must complete without
     * throwing.
     *
     * @throws IOException if executing the HTTP request fails
     */
    @Test(expected = Test.None.class)
    public void shouldEmbedAccessTokenIfGoAuthEnabled() throws IOException {
        httpRequest = new HttpGet("http://127.0.0.1:1080/api");
        httpRequest.addHeader("foo", "bar");

        Map<String, String> configuration = new HashMap<>();
        configuration.put("SINK_HTTPV2_OAUTH2_ENABLE", "true");
        configuration.put("SINK_HTTPV2_OAUTH2_ACCESS_TOKEN_URL", "http://127.0.0.1:1080/oauth2/token");

        clientConfig = ConfigFactory.create(HttpClientConfig.class, configuration);
        CloseableHttpClient closeableHttpClient = HttpClientUtils.newHttpClient(clientConfig, statsDReporter);

        closeableHttpClient.execute(httpRequest);

        mockServer.verify(request().withPath("/oauth2/token"), VerificationTimes.exactly(1));
    }
}
