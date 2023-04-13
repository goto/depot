package com.gotocompany.depot.bigquery;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageClient;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageResponseParser;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BigQueryStorageAPISink implements Sink {
    private final BigQueryStorageClient bigQueryStorageClient;
    private final Instrumentation instrumentation;

    public BigQueryStorageAPISink(
            BigQueryStorageClient bigQueryStorageClient,
            BigQueryMetrics bigQueryMetrics,
            Instrumentation instrumentation) {
        this.bigQueryStorageClient = bigQueryStorageClient;
        this.instrumentation = instrumentation;
    }

    @Override
    public SinkResponse pushToSink(List<Message> messages) throws SinkException {
        SinkResponse sinkResponse = new SinkResponse();
        BigQueryPayload payload = bigQueryStorageClient.convert(messages);
        BigQueryStorageResponseParser.setSinkResponseForInvalidMessages(payload, messages, sinkResponse, instrumentation);
        try {
            AppendRowsResponse appendRowsResponse = bigQueryStorageClient.appendAndGet(payload);
            BigQueryStorageResponseParser.setSinkResponseForErrors(payload, appendRowsResponse, messages, sinkResponse, instrumentation);
        } catch (ExecutionException e) {
            e.printStackTrace();
            Throwable cause = e.getCause();
            BigQueryStorageResponseParser.setSinkResponseFromException(cause, payload, messages, sinkResponse, instrumentation);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sinkResponse;
    }

    @Override
    public void close() throws IOException {
        bigQueryStorageClient.close();
    }
}
