package com.gotocompany.depot.bigquery.storage;

import com.google.api.core.ApiFutureCallback;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.gotocompany.depot.common.Function3;
import com.gotocompany.depot.config.BigQuerySinkConfig;

import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public interface BigQueryWriter {

    void init(Function<BigQuerySinkConfig, BigQueryWriteClient> bqWriterCreator,
              Function<BigQuerySinkConfig, CredentialsProvider> credCreator,
              Function3<BigQuerySinkConfig, CredentialsProvider, ProtoSchema, BigQueryStream> streamCreator);

    AppendRowsResponse appendAndGet(BigQueryPayload payload, ApiFutureCallback<AppendRowsResponse> callback)
            throws ExecutionException, InterruptedException;
}
