package com.gotocompany.depot.bigquery;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageClient;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageResponseParser;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.message.Message;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * {@link Sink} implementation that writes records to BigQuery using the Storage Write API.
 *
 * <p>Delegates conversion and appending of rows to a
 * {@link com.gotocompany.depot.bigquery.storage.BigQueryStorageClient} and uses a
 * {@link com.gotocompany.depot.bigquery.storage.BigQueryStorageResponseParser} to translate
 * invalid payloads, append responses and append failures into the sink response. This is
 * the higher-throughput alternative to {@link BigQuerySink}, selected when the Storage API
 * is enabled.</p>
 *
 * @see BigQuerySinkFactory
 */
public class BigQueryStorageAPISink implements Sink {
    /** Client that converts messages to a payload and appends rows via the Storage API. */
    private final BigQueryStorageClient bigQueryStorageClient;
    /** Parser that maps payloads, append responses and exceptions onto the sink response. */
    private final BigQueryStorageResponseParser responseParser;

    /**
     * Creates a Storage API sink from its client and response parser.
     *
     * @param bigQueryStorageClient the client used to convert messages and append rows
     * @param responseParser        the parser used to populate the sink response
     */
    public BigQueryStorageAPISink(
            BigQueryStorageClient bigQueryStorageClient,
            BigQueryStorageResponseParser responseParser) {
        this.bigQueryStorageClient = bigQueryStorageClient;
        this.responseParser = responseParser;
    }

    /**
     * Converts a batch of messages and appends the valid rows via the Storage Write API.
     *
     * <p>Converts the messages to a
     * {@link com.gotocompany.depot.bigquery.storage.BigQueryPayload}, records invalid
     * messages in the response, and, when there are rows to write, appends them and parses
     * the append response for per-row errors. An
     * {@link java.util.concurrent.ExecutionException} during the append is unwrapped and
     * reported through the response parser, while an {@link InterruptedException} is
     * rethrown as a {@link SinkException}.</p>
     *
     * @param messages the messages to push to the sink
     * @return a {@link SinkResponse} containing an entry for every message that failed
     * @throws SinkException if the append is interrupted
     */
    @Override
    public SinkResponse pushToSink(List<Message> messages) throws SinkException {
        SinkResponse sinkResponse = new SinkResponse();
        BigQueryPayload payload = bigQueryStorageClient.convert(messages);
        responseParser.setSinkResponseForInvalidMessages(payload, messages, sinkResponse);
        if (!payload.getPayloadIndexes().isEmpty()) {
            try {
                AppendRowsResponse appendRowsResponse = bigQueryStorageClient.appendAndGet(payload);
                responseParser.setSinkResponseForErrors(payload, appendRowsResponse, messages, sinkResponse);
            } catch (ExecutionException e) {
                e.printStackTrace();
                Throwable cause = e.getCause();
                responseParser.setSinkResponseForException(cause, payload, messages, sinkResponse);
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new SinkException("Interrupted exception occurred", e);
            }
        }
        return sinkResponse;
    }

    /**
     * Closes the underlying Storage API client.
     *
     * @throws IOException if closing the underlying
     *                     {@link com.gotocompany.depot.bigquery.storage.BigQueryStorageClient}
     *                     fails
     */
    @Override
    public void close() throws IOException {
        bigQueryStorageClient.close();
    }
}
