package com.gotocompany.depot.bigquery.storage.json;

import com.google.api.core.ApiFutureCallback;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryStream;
import com.gotocompany.depot.bigquery.storage.BigQueryWriter;
import com.gotocompany.depot.common.Function3;
import com.gotocompany.depot.config.BigQuerySinkConfig;

import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class BigQueryJsonWriter implements BigQueryWriter {

    public BigQueryJsonWriter(BigQuerySinkConfig config) {

    }


    @Override
    public void init(Function<BigQuerySinkConfig, BigQueryWriteClient> bqWriterCreator,
                     Function<BigQuerySinkConfig, CredentialsProvider> credCreator,
                     Function3<BigQuerySinkConfig, CredentialsProvider, ProtoSchema, BigQueryStream> streamCreator) {

    }

    @Override
    public AppendRowsResponse appendAndGet(BigQueryPayload rows, ApiFutureCallback<AppendRowsResponse> callback)
            throws ExecutionException, InterruptedException {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
