package com.gotocompany.depot.bigquery.storage;

import com.google.api.core.ApiFutureCallback;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.gotocompany.depot.message.Message;

import java.util.List;

public interface BigQueryStorageClient {
    BigQueryPayload convert(List<Message> messages);
    AppendRowsResponse appendAndGet(BigQueryPayload payload, ApiFutureCallback<AppendRowsResponse> callback) throws Exception;
}
