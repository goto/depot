package com.gotocompany.depot.http.request;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.net.URI;
import java.util.Map;

/**
 * Static helpers shared by the {@link Request} strategies for building requests and error records.
 *
 * <p>This class centralizes two concerns of the request-building stage: turning a construction
 * failure into an invalid {@link HttpRequestRecord} that carries diagnostic
 * {@link com.gotocompany.depot.error.ErrorInfo}, and assembling a concrete, body-capable
 * {@link HttpEntityEnclosingRequestBase} from the resolved method, headers, URI and optional body.
 * It is not meant to be instantiated.</p>
 *
 * @see SingleRequest
 * @see BatchRequest
 * @see RequestMethodFactory
 */
@Slf4j
public class RequestUtils {

    /**
     * Builds an invalid request record describing a per-message construction failure.
     *
     * <p>Wraps the triggering exception and error type in an
     * {@link com.gotocompany.depot.error.ErrorInfo}, logs the failure together with the message
     * metadata, and returns an invalid {@link HttpRequestRecord} tagged with the offending message
     * index.</p>
     *
     * @param e the exception that prevented the request from being built
     * @param type the classification of the failure
     * @param index the index of the message (within the batch) that failed
     * @param metadata the metadata of the failing message, included in the log line
     * @return an invalid record carrying the error and the message index
     */
    protected static HttpRequestRecord createErrorRecord(Exception e, ErrorType type, Integer index, Map<String, Object> metadata) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        log.error("Error while parsing record for message. Metadata : {}, Error: {}", metadata, errorInfo);
        HttpRequestRecord record = new HttpRequestRecord(errorInfo);
        record.addIndex(index);
        return record;
    }

    /**
     * Wraps an object's string form in a JSON-typed HTTP string entity.
     *
     * <p>The entity is created with {@link ContentType#APPLICATION_JSON} so the request advertises a
     * JSON body, using {@code input.toString()} as the payload.</p>
     *
     * @param input the value whose string representation becomes the entity body
     * @return a {@link StringEntity} carrying the JSON content type
     */
    private static StringEntity buildStringEntity(Object input) {
        return new StringEntity(input.toString(), ContentType.APPLICATION_JSON);
    }

    /**
     * Assembles a body-capable HTTP request from the resolved method, headers, URI and body.
     *
     * <p>The request object is created by {@link RequestMethodFactory#create(URI, com.gotocompany.depot.http.enums.HttpRequestMethodType)}
     * for the configured method, every header is added to it, and, when {@code requestBody} is
     * non-{@code null}, a JSON {@link StringEntity} produced by {@link #buildStringEntity(Object)} is
     * attached. A {@code null} body yields a request with no entity.</p>
     *
     * @param config the sink configuration supplying the HTTP method
     * @param headers the headers to add to the request
     * @param uri the target URI of the request
     * @param requestBody the body to serialize and attach, or {@code null} for a bodyless request
     * @return the assembled HTTP request, ready to be executed
     */
    public static HttpEntityEnclosingRequestBase buildRequest(HttpSinkConfig config, Map<String, String> headers, URI uri, Object requestBody) {
        HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(uri, config.getSinkHttpRequestMethod());
        headers.forEach(request::addHeader);
        if (requestBody != null) {
            request.setEntity(buildStringEntity(requestBody));
        }
        return request;
    }
}
