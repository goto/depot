package com.gotocompany.depot.bigquery;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.rpc.Status;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageClient;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;

public class BigQueryStorageAPISink implements Sink {
    private final BigQueryStorageClient bigQueryStorageClient;

    public BigQueryStorageAPISink(
            BigQueryStorageClient bigQueryStorageClient,
            BigQueryMetrics bigQueryMetrics,
            Instrumentation instrumentation) {
        this.bigQueryStorageClient = bigQueryStorageClient;
    }

    @Override
    public SinkResponse pushToSink(List<Message> messages) throws SinkException {
        BigQueryPayload payload = bigQueryStorageClient.convert(messages);
        try {
            AppendRowsResponse appendRowsResponse = bigQueryStorageClient.appendAndGet(payload, null);
            Status error = appendRowsResponse.getError();
            List<RowError> rowErrorsList = appendRowsResponse.getRowErrorsList();
            rowErrorsList.get(0).getIndex();
        } catch (Exception e) {

        }
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
