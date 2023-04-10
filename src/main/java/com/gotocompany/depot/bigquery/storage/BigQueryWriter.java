package com.gotocompany.depot.bigquery.storage;

import com.google.api.core.ApiFutureCallback;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;

public interface BigQueryWriter extends AutoCloseable {

    void init();

    AppendRowsResponse appendAndGet(BigQueryPayload payload, ApiFutureCallback<AppendRowsResponse> callback) throws Exception;
}
