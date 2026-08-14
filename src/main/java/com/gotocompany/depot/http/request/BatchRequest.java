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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link Request} strategy that aggregates an entire batch of messages into a single HTTP request.
 *
 * <p>Selected when the sink runs in {@link com.gotocompany.depot.http.enums.HttpRequestType#BATCH}
 * mode. Because one request serves many messages, the headers, query parameters and target URI must
 * be constant and are therefore resolved once at construction time (the underlying builders reject
 * per-message templates in batch mode). At publish time the serialized bodies of all valid messages
 * are collected and sent together, with the resulting valid {@link HttpRequestRecord} carrying the
 * indexes of every message it includes.</p>
 *
 * <p>Per-message serialization failures are recorded as individual invalid records (via
 * {@link RequestUtils#createErrorRecord}) so that a single bad message does not prevent the rest of
 * the batch from being sent.</p>
 *
 * @see Request
 * @see SingleRequest
 * @see RequestFactory
 */
@Slf4j
public class BatchRequest implements Request {

    /**
     * Constant request headers shared by every message in the batch, resolved once at construction.
     */
    private final Map<String, String> requestHeaders;
    /**
     * Constant target URI (including query parameters) shared by the batch, resolved at construction.
     */
    private final URI requestUrl;
    /**
     * Serializer that turns each message into its body fragment within the batch payload.
     */
    private final RequestBody requestBody;
    /**
     * Sink configuration governing request method and delete-body behavior.
     */
    private final HttpSinkConfig config;
    /**
     * Parser used to decode each message's key and value into parsed form.
     */
    private final MessageParser parser;

    /**
     * Creates a batch-request strategy, eagerly resolving the constant headers and URI.
     *
     * <p>The headers are obtained from {@link HeaderBuilder#build()} and the URI from
     * {@link UriBuilder#build(Map)} using {@link QueryParamBuilder#build()}; all three reject
     * per-message templates, so a {@link ConfigurationException} is raised here if the configuration
     * declares templated headers, query parameters or service URL. The body serializer is resolved
     * from {@code config} via {@link RequestBodyFactory#create(HttpSinkConfig)}.</p>
     *
     * @param headerBuilder the builder supplying the constant request headers
     * @param queryParamBuilder the builder supplying the constant query parameters
     * @param uriBuilder the builder supplying the constant target URI
     * @param config the HTTP sink configuration
     * @param parser the parser used to decode messages
     */
    public BatchRequest(HeaderBuilder headerBuilder,
                        QueryParamBuilder queryParamBuilder,
                        UriBuilder uriBuilder,
                        HttpSinkConfig config,
                        MessageParser parser) {
        this.requestHeaders = headerBuilder.build();
        this.requestUrl = uriBuilder.build(queryParamBuilder.build());
        this.requestBody = RequestBodyFactory.create(config);
        this.config = config;
        this.parser = parser;
    }

    /**
     * Builds the request records representing the whole batch.
     *
     * <p>When the configured method is {@link HttpRequestMethodType#DELETE} and delete bodies are
     * disabled, a single bodyless request covering the constant headers and URI is produced.
     * Otherwise the work is delegated to {@link #createRecordsWithBody(List, ArrayList)}, which builds
     * one aggregated request from all valid message bodies plus any per-message error records.</p>
     *
     * @param messages the batch of messages to convert
     * @return the resulting records: typically one valid aggregated record plus zero or more invalid
     *     records, or a single bodyless record for the delete-without-body case
     */
    @Override
    public List<HttpRequestRecord> createRecords(List<Message> messages) {
        ArrayList<HttpRequestRecord> records = new ArrayList<>();
        if (!(config.getSinkHttpRequestMethod() == HttpRequestMethodType.DELETE && !config.isSinkHttpDeleteBodyEnable())) {
            createRecordsWithBody(messages, records);
        } else {
            HttpEntityEnclosingRequestBase request = RequestUtils.buildRequest(config, requestHeaders, requestUrl, null);
            records.add(new HttpRequestRecord(request));
        }
        return records;
    }

    /**
     * Serializes each message and aggregates the valid bodies into a single request record.
     *
     * <p>Every message is parsed and serialized in turn. Successfully serialized bodies are collected
     * into a map keyed by message index; any message that fails serialization is appended to
     * {@code records} as an invalid record whose {@link ErrorType} reflects the failure:</p>
     * <ul>
     *   <li>{@link EmptyMessageException} maps to {@link ErrorType#INVALID_MESSAGE_ERROR};</li>
     *   <li>{@link ConfigurationException} and {@link IllegalArgumentException} map to
     *       {@link ErrorType#UNKNOWN_FIELDS_ERROR};</li>
     *   <li>{@link DeserializerException} and {@link IOException} map to
     *       {@link ErrorType#DESERIALIZATION_ERROR}.</li>
     * </ul>
     * <p>If at least one body was built, the collected bodies are sent as one request and the
     * resulting valid record is tagged with all the corresponding message indexes.</p>
     *
     * @param messages the batch of messages to serialize
     * @param records the accumulator to which invalid records and the aggregated valid record are
     *     added
     */
    private void createRecordsWithBody(List<Message> messages, ArrayList<HttpRequestRecord> records) {
        Map<Integer, String> validBodies = new HashMap<>();
        for (int index = 0; index < messages.size(); index++) {
            Message message = messages.get(index);
            MessageContainer messageContainer = new MessageContainer(message, parser);
            try {
                String body = requestBody.build(messageContainer);
                validBodies.put(index, body);
            } catch (EmptyMessageException e) {
                records.add(RequestUtils.createErrorRecord(e, ErrorType.INVALID_MESSAGE_ERROR, index, message.getMetadata()));
            } catch (ConfigurationException | IllegalArgumentException e) {
                records.add(RequestUtils.createErrorRecord(e, ErrorType.UNKNOWN_FIELDS_ERROR, index, message.getMetadata()));
            } catch (DeserializerException | IOException e) {
                records.add(RequestUtils.createErrorRecord(e, ErrorType.DESERIALIZATION_ERROR, index, message.getMetadata()));
            }
        }
        if (validBodies.size() != 0) {
            HttpEntityEnclosingRequestBase request = RequestUtils.buildRequest(config, requestHeaders, requestUrl, validBodies.values());
            HttpRequestRecord validRecord = new HttpRequestRecord(request);
            validRecord.addAllIndexes(validBodies.keySet());
            records.add(validRecord);
        }
    }
}
