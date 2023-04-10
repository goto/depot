package com.gotocompany.depot.bigquery.storage.json;

import com.google.api.core.ApiFutureCallback;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryWriter;
import com.gotocompany.depot.config.BigQuerySinkConfig;

public class BigQueryJsonWriter implements BigQueryWriter {

    public BigQueryJsonWriter(BigQuerySinkConfig config) {

    }


    @Override
    public void init() {

    }

    @Override
    public AppendRowsResponse appendAndGet(BigQueryPayload rows, ApiFutureCallback<AppendRowsResponse> callback) {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
