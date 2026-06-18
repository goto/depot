package com.gotocompany.depot.http.request;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.http.enums.HttpRequestType;
import com.gotocompany.depot.http.request.builder.HeaderBuilder;
import com.gotocompany.depot.http.request.builder.QueryParamBuilder;
import com.gotocompany.depot.http.request.builder.UriBuilder;
import com.gotocompany.depot.message.MessageParser;

/**
 * Factory that selects and assembles the appropriate {@link Request} strategy from configuration.
 *
 * <p>It constructs the shared header, query-parameter and URI builders and then, based on the
 * configured {@link HttpRequestType}, returns either a {@link SingleRequest} (one request per message)
 * or a {@link BatchRequest} (one request per batch). This is the single place where the request-mode
 * decision is made for the HTTP sink.</p>
 *
 * @see Request
 * @see SingleRequest
 * @see BatchRequest
 */
public class RequestFactory {

    /**
     * Creates the request strategy matching the configured request mode.
     *
     * <p>Builds a {@link HeaderBuilder}, {@link QueryParamBuilder} and {@link UriBuilder} from the
     * configuration and wires them into a {@link SingleRequest} when the request type is
     * {@link HttpRequestType#SINGLE}, or a {@link BatchRequest} otherwise. Constructing the
     * {@link UriBuilder} compiles the service-URL template, which may fail validation.</p>
     *
     * @param config the HTTP sink configuration
     * @param messageParser the parser used by the strategy to decode messages
     * @return a {@link SingleRequest} or {@link BatchRequest} according to the configured request mode
     * @throws InvalidTemplateException if the configured service URL is not a valid template
     */
    public static Request create(HttpSinkConfig config, MessageParser messageParser) throws InvalidTemplateException {
        HeaderBuilder headerBuilder = new HeaderBuilder(config);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(config);
        UriBuilder uriBuilder = new UriBuilder(config);

        if (config.getRequestType().equals(HttpRequestType.SINGLE)) {
            return new SingleRequest(headerBuilder, queryParamBuilder, uriBuilder, config, messageParser);
        } else {
            return new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder, config, messageParser);
        }
    }
}
