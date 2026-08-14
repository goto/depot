package com.gotocompany.depot.http.request;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.http.request.body.RequestBody;
import com.gotocompany.depot.http.request.body.RequestBodyFactory;
import com.gotocompany.depot.http.request.builder.HeaderBuilder;
import com.gotocompany.depot.http.request.builder.QueryParamBuilder;
import com.gotocompany.depot.http.request.builder.UriBuilder;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * {@link Request} strategy that emits one HTTP request per message.
 *
 * <p>Selected when the sink runs in {@link com.gotocompany.depot.http.enums.HttpRequestType#SINGLE}
 * mode. For every message it independently resolves the request headers, query parameters, target
 * URI and body from that message's parsed content, which means each component may use per-message
 * templates. The resulting {@link HttpRequestRecord} carries the single originating message index.</p>
 *
 * <p>Construction failures are isolated per message: depending on the exception raised while building
 * a record, an invalid record carrying the appropriate {@link ErrorType} is produced (via
 * {@link RequestUtils#createErrorRecord}) instead of failing the entire batch.</p>
 *
 * @see Request
 * @see BatchRequest
 * @see RequestFactory
 */
@Slf4j
public class SingleRequest implements Request {

    /**
     * Builder that resolves per-message request headers from templates and base headers.
     */
    private final HeaderBuilder headerBuilder;
    /**
     * Builder that resolves per-message query parameters from templates.
     */
    private final QueryParamBuilder queryParamBuilder;
    /**
     * Builder that resolves the per-message target URI, including query parameters.
     */
    private final UriBuilder uriBuilder;
    /**
     * Serializer that turns a message into the request body, chosen by the configured body type.
     */
    private final RequestBody requestBody;
    /**
     * Sink configuration governing request method, delete-body behavior and schema classes.
     */
    private final HttpSinkConfig config;
    /**
     * Parser used to decode each message's key and value into parsed form.
     */
    private final MessageParser messageParser;

    /**
     * Creates a single-request strategy from its component builders and configuration.
     *
     * <p>The request body serializer is resolved from {@code config} via
     * {@link RequestBodyFactory#create(HttpSinkConfig)} at construction time.</p>
     *
     * @param headerBuilder the builder for per-message request headers
     * @param queryParamBuilder the builder for per-message query parameters
     * @param uriBuilder the builder for the per-message target URI
     * @param config the HTTP sink configuration
     * @param messageParser the parser used to decode messages
     */
    public SingleRequest(HeaderBuilder headerBuilder,
                         QueryParamBuilder queryParamBuilder,
                         UriBuilder uriBuilder,
                         HttpSinkConfig config,
                         MessageParser messageParser) {
        this.headerBuilder = headerBuilder;
        this.queryParamBuilder = queryParamBuilder;
        this.uriBuilder = uriBuilder;
        this.requestBody = RequestBodyFactory.create(config);
        this.config = config;
        this.messageParser = messageParser;

    }

    /**
     * Builds one HTTP request record per message, preserving the original order.
     *
     * <p>Each message is converted by {@link #createRecord(Message, int)} using its position in the
     * list as the record index, so the returned list has exactly one record per input message. Both
     * valid and invalid records may be present.</p>
     *
     * @param messages the batch of messages to convert
     * @return a list containing one record per message, in the original order
     */
    @Override
    public List<HttpRequestRecord> createRecords(List<Message> messages) {
        ArrayList<HttpRequestRecord> records = new ArrayList<>();
        IntStream.range(0, messages.size()).forEach(index -> {
            Message message = messages.get(index);
            HttpRequestRecord record = createRecord(message, index);
            records.add(record);
        });
        return records;
    }

    /**
     * Resolves the request body for a message, honoring delete-method body rules.
     *
     * <p>For any method other than {@link HttpRequestMethodType#DELETE} the body is always built. For
     * {@code DELETE} a body is built only when {@code SINK_HTTPV2_DELETE_BODY_ENABLE} is set;
     * otherwise {@code null} is returned so the request is sent without an entity.</p>
     *
     * @param messageContainer the lazily-parsed view of the message
     * @return the serialized request body, or {@code null} when a delete request must carry no body
     * @throws IOException if the body serializer fails to read or parse the message
     */
    private Object getPayload(MessageContainer messageContainer) throws IOException {
        // Other http methods than delete.
        if (config.getSinkHttpRequestMethod() != HttpRequestMethodType.DELETE) {
            return requestBody.build(messageContainer);
        }
        // create payload for delete if enabled
        if (config.isSinkHttpDeleteBodyEnable()) {
            return requestBody.build(messageContainer);
        }
        return null;
    }

    /**
     * Builds a single HTTP request record for one message, capturing any failure as an invalid record.
     *
     * <p>The message is wrapped in a {@link MessageContainer} and used to resolve the request headers,
     * query parameters, URI and payload, which are assembled into an
     * {@link org.apache.http.client.methods.HttpEntityEnclosingRequestBase} by
     * {@link RequestUtils#buildRequest}. The resulting valid record is tagged with the message index.
     * If building fails, the exception is translated into an invalid record whose
     * {@link ErrorType} reflects the failure category:</p>
     * <ul>
     *   <li>{@link EmptyMessageException} maps to {@link ErrorType#INVALID_MESSAGE_ERROR};</li>
     *   <li>{@link ConfigurationException} and {@link IllegalArgumentException} map to
     *       {@link ErrorType#UNKNOWN_FIELDS_ERROR};</li>
     *   <li>{@link DeserializerException} and {@link IOException} map to
     *       {@link ErrorType#DESERIALIZATION_ERROR}.</li>
     * </ul>
     *
     * @param message the message to convert into a request
     * @param index the position of the message within the batch, used as the record index
     * @return a valid record wrapping the built request, or an invalid record describing the failure
     */
    private HttpRequestRecord createRecord(Message message, int index) {
        try {
            MessageContainer messageContainer = new MessageContainer(message, messageParser);
            Map<String, String> requestHeaders = headerBuilder.build(messageContainer);
            Map<String, String> queryParam = queryParamBuilder.build(messageContainer);
            URI requestUrl = uriBuilder.build(messageContainer, queryParam);
            HttpEntityEnclosingRequestBase request;
            Object payload = getPayload(messageContainer);
            request = RequestUtils.buildRequest(config, requestHeaders, requestUrl, payload);
            HttpRequestRecord record = new HttpRequestRecord(request);
            record.addIndex(index);
            return record;
        } catch (EmptyMessageException e) {
            return RequestUtils.createErrorRecord(e, ErrorType.INVALID_MESSAGE_ERROR, index, message.getMetadata());
        } catch (ConfigurationException | IllegalArgumentException e) {
            return RequestUtils.createErrorRecord(e, ErrorType.UNKNOWN_FIELDS_ERROR, index, message.getMetadata());
        } catch (DeserializerException | IOException e) {
            return RequestUtils.createErrorRecord(e, ErrorType.DESERIALIZATION_ERROR, index, message.getMetadata());
        }
    }
}
