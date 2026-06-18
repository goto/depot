package com.gotocompany.depot.common.client.auth;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.gotocompany.depot.common.exception.OAuth2Exception;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Minimal OAuth2 client that obtains access tokens using the client-credentials grant.
 *
 * <p>{@code OAuth2Client} encapsulates the token-endpoint interaction for Depot's HTTP sink. It holds
 * the client credentials, requested scope and token endpoint, and uses a dedicated Apache HTTP client
 * (configured with a fixed five-second timeout) to perform a {@code grant_type=client_credentials}
 * request. The JSON response is parsed into an {@link OAuth2AccessToken}. It is created and driven by
 * {@link OAuth2Credential}, which caches and refreshes the returned tokens.
 */
public class OAuth2Client {
    /**
     * Apache HTTP client used to call the OAuth2 token endpoint.
     */
    private final HttpClient client;
    /**
     * OAuth2 client identifier sent as the {@code client_id} form parameter.
     */
    private final String clientId;
    /**
     * OAuth2 client secret sent as the {@code client_secret} form parameter.
     */
    private final String clientSecret;
    /**
     * Requested OAuth2 scope sent as the {@code scope} form parameter.
     */
    private final String scope;
    /**
     * URL of the OAuth2 token endpoint to which access-token requests are posted.
     */
    private final String accessTokenEndpoint;
    /**
     * Connect, connection-request and socket timeout, in milliseconds, applied to token requests.
     */
    private final int timeoutMs = 5000;
    /**
     * Regular expression ({@code "^2.*"}) matching HTTP 2xx status codes that denote a successful
     * token response.
     */
    private static final String SUCCESS_CODE_PATTERN = "^2.*";

    /**
     * Creates a client that requests tokens from the given endpoint with the given credentials.
     *
     * <p>An internal Apache HTTP client is created immediately, configured with the fixed request
     * timeout.
     *
     * @param clientId            the OAuth2 client identifier
     * @param clientSecret        the OAuth2 client secret
     * @param scope               the OAuth2 scope to request
     * @param accessTokenEndpoint the token-endpoint URL to which requests are posted
     */
    public OAuth2Client(String clientId, String clientSecret, String scope, String accessTokenEndpoint) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.accessTokenEndpoint = accessTokenEndpoint;
        this.client = this.httpClient();
    }

    /**
     * Builds the internal Apache HTTP client used for token requests.
     *
     * <p>The connect, connection-request and socket timeouts are all set to {@code timeoutMs}.
     *
     * @return a configured {@link CloseableHttpClient} for calling the token endpoint
     */
    private CloseableHttpClient httpClient() {
        RequestConfig config = RequestConfig.custom().setConnectTimeout(timeoutMs).setConnectionRequestTimeout(timeoutMs).setSocketTimeout(timeoutMs).build();
        return HttpClientBuilder.create().setDefaultRequestConfig(config).build();
    }

    /**
     * Requests a new access token from the OAuth2 token endpoint using the client-credentials grant.
     *
     * <p>A form-encoded POST carrying {@code client_id}, {@code client_secret}, {@code scope} and
     * {@code grant_type=client_credentials} is sent to the configured endpoint. The JSON response body
     * is parsed into a map; on a 2xx status the {@code access_token} and optional {@code expires_in}
     * values are used to build an {@link OAuth2AccessToken}, while on any other status an
     * {@link OAuth2Exception} carrying the response's {@code error} value is thrown.
     *
     * @return the access token obtained from the authorization server
     * @throws OAuth2Exception if the token endpoint responds with a non-2xx status code
     * @throws IOException     if the HTTP request fails or the response body cannot be read
     */
    public OAuth2AccessToken requestClientCredentialsGrantAccessToken() throws IOException {
        HttpPost req = new HttpPost(this.accessTokenEndpoint);
        req.setHeader("Content-Type", "application/x-www-form-urlencoded");
        List<NameValuePair> kv = new ArrayList<>();
        kv.add(new BasicNameValuePair("client_id", this.clientId));
        kv.add(new BasicNameValuePair("client_secret", this.clientSecret));
        kv.add(new BasicNameValuePair("scope", this.scope));
        kv.add(new BasicNameValuePair("grant_type", "client_credentials"));
        req.setEntity(new UrlEncodedFormEntity(kv, "UTF-8"));
        HttpResponse response = this.client.execute(req);
        String body = EntityUtils.toString(response.getEntity());
        Type responseMapType = (new TypeToken<Map<String, String>>() {
        }).getType();
        Map<String, String> map = new Gson().fromJson(body, responseMapType);

        if (!Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches()) {
            throw new OAuth2Exception("OAuthException: " + map.get("error"));
        } else {
            String accessToken = map.get("access_token");
            String expiresInRaw = map.get("expires_in");
            Integer expiresIn = expiresInRaw == null ? null : Integer.valueOf(expiresInRaw);
            return new OAuth2AccessToken(accessToken, expiresIn);
        }
    }
}
