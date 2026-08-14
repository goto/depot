package com.gotocompany.depot.common.client.auth;

import com.gotocompany.depot.metrics.Instrumentation;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;

/**
 * Interceptor to add oauth token in http request.
 *
 * <p>{@code OAuth2Credential} wraps an {@link OAuth2Client} and caches the most recently obtained
 * {@link OAuth2AccessToken}. It serves two HTTP stacks: it implements the OkHttp {@link Interceptor}
 * contract through {@link #intercept(Interceptor.Chain)}, and it exposes Apache HttpClient
 * {@link HttpRequestInterceptor} and {@link HttpResponseInterceptor} instances that can be registered
 * on an {@link HttpClientBuilder} via {@link #initialize(HttpClientBuilder)}.
 *
 * <p>In both stacks the behavior is the same: before a request is sent the token is fetched (or
 * refreshed when missing or near expiry) and added as an {@code Authorization: Bearer} header, and
 * when a response reports {@link HttpStatus#SC_UNAUTHORIZED} the cached token is discarded so the next
 * request obtains a fresh one. Token-acquisition failures are logged through the supplied
 * {@link Instrumentation} rather than propagated.
 */
public class OAuth2Credential implements Interceptor {

    /**
     * Client used to obtain access tokens from the OAuth2 token endpoint.
     */
    private final OAuth2Client client;
    /**
     * Instrumentation used to log token-acquisition progress and failures.
     */
    private final Instrumentation instrumentation;
    /**
     * The most recently obtained access token, or {@code null} when none has been fetched or the
     * cached token has been invalidated.
     */
    private OAuth2AccessToken accessToken;

    /**
     * Creates a credential backed by a new {@link OAuth2Client} built from the given settings.
     *
     * @param instrumentation     the instrumentation used for logging token activity
     * @param clientId            the OAuth2 client identifier
     * @param clientSecret        the OAuth2 client secret
     * @param scope               the OAuth2 scope to request
     * @param accessTokenEndpoint the token-endpoint URL from which tokens are requested
     */
    public OAuth2Credential(Instrumentation instrumentation, String clientId, String clientSecret, String scope, String accessTokenEndpoint) {
        this.instrumentation = instrumentation;
        this.client = new OAuth2Client(clientId, clientSecret, scope, accessTokenEndpoint);
    }

    /**
     * Fetches a fresh access token from the token endpoint and caches it.
     *
     * <p>Logs the remaining validity of the currently cached token (or {@code <none>} when absent),
     * requests a new token via the client-credentials grant, and stores the result.
     *
     * @throws IOException if the underlying token request fails
     */
    public void requestAccessToken() throws IOException {
        instrumentation.logInfo("Requesting Access Token, expires in: {0}",
                (this.accessToken == null ? "<none>" : this.accessToken.getExpiresIn()));
        OAuth2AccessToken token = client.requestClientCredentialsGrantAccessToken();
        setAccessToken(token);
    }

    /**
     * Returns an Apache HttpClient request interceptor that attaches a bearer token to each request.
     *
     * <p>The returned interceptor refreshes the cached token when it is missing or expired, then adds
     * an {@code Authorization: Bearer} header. If token acquisition fails, the failure is logged as a
     * warning and the request proceeds without the header.
     *
     * @return a request interceptor that injects the OAuth2 bearer token
     */
    public HttpRequestInterceptor requestInterceptor() {
        return (request, context) -> {
            try {
                if (getAccessToken() == null || getAccessToken().isExpired()) {
                    requestAccessToken();
                }
                request.addHeader("Authorization", "Bearer " + getAccessToken().toString());
            } catch (IOException e) {
                instrumentation.logWarn("OAuth2 request access token failed: {0}", e.getMessage());
            }
        };
    }

    /**
     * Returns an Apache HttpClient response interceptor that invalidates the token on authorization
     * failures.
     *
     * <p>When a response carries an HTTP 401 (unauthorized) status, the cached token is cleared so the
     * next request obtains a fresh one.
     *
     * @return a response interceptor that clears the cached token on a 401 response
     */
    public HttpResponseInterceptor responseInterceptor() {
        return (response, context) -> {
            boolean isTokenExpired = response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED;
            if (isTokenExpired) {
                setAccessToken(null);
            }
        };
    }

    /**
     * Registers this credential's request and response interceptors on an Apache HTTP client builder.
     *
     * <p>The request interceptor is added first and the response interceptor last within their
     * respective chains.
     *
     * @param builder the Apache {@link HttpClientBuilder} to augment
     * @return the same {@code builder}, with the OAuth2 interceptors registered, for chaining
     */
    public HttpClientBuilder initialize(HttpClientBuilder builder) {
        return builder.addInterceptorFirst(this.requestInterceptor()).addInterceptorLast(this.responseInterceptor());
    }

    /**
     * Returns the currently cached access token.
     *
     * @return the cached {@link OAuth2AccessToken}, or {@code null} if none is currently held
     */
    public OAuth2AccessToken getAccessToken() {
        return accessToken;
    }

    /**
     * Replaces the cached access token.
     *
     * @param accessToken the token to cache, or {@code null} to invalidate the current token
     */
    public void setAccessToken(OAuth2AccessToken accessToken) {
        this.accessToken = accessToken;
    }

    /**
     * Intercepts an OkHttp request to attach a valid OAuth2 bearer token and handle token expiry.
     *
     * <p>{@inheritDoc}
     *
     * <p>Before proceeding, the cached token is refreshed when missing or expired and added as an
     * {@code Authorization: Bearer} header on a copy of the request; acquisition failures are logged
     * as warnings and the request proceeds without the header. After the call, a 401 (unauthorized)
     * response clears the cached token so the next request re-authenticates.
     *
     * @param chain the OkHttp interceptor chain providing the request and used to proceed
     * @return the response produced by proceeding along the chain
     * @throws IOException if proceeding with the request fails
     */
    @Override
    public Response intercept(Interceptor.Chain chain) throws IOException {
        Request request = chain.request();
        try {
            if (getAccessToken() == null || getAccessToken().isExpired()) {
                requestAccessToken();
            }
            request = request.newBuilder().header("Authorization", "Bearer " + getAccessToken().toString()).build();
        } catch (IOException e) {
            instrumentation.logWarn("OAuth2 request access token failed: {0}", e.getMessage());
        }

        Response response = chain.proceed(request);
        boolean isTokenExpired = response.code() == HttpStatus.SC_UNAUTHORIZED;
        if (isTokenExpired) {
            setAccessToken(null);
        }
        return response;
    }
}

