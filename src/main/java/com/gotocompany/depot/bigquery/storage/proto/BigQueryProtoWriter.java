package com.gotocompany.depot.bigquery.storage.proto;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryStream;
import com.gotocompany.depot.bigquery.storage.BigQueryWriter;
import com.gotocompany.depot.bigquery.storage.BigQueryWriterUtils;
import com.gotocompany.depot.common.Function3;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import lombok.Getter;

import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class BigQueryProtoWriter implements AutoCloseable, BigQueryWriter {

    private final BigQuerySinkConfig config;
    @Getter
    private StreamWriter streamWriter;
    @Getter
    private Descriptors.Descriptor descriptor;

    public BigQueryProtoWriter(BigQuerySinkConfig config) {
        this.config = config;
    }

    @Override
    public void init(Function<BigQuerySinkConfig, BigQueryWriteClient> bqWriterCreator,
                     Function<BigQuerySinkConfig, CredentialsProvider> credCreator,
                     Function3<BigQuerySinkConfig, CredentialsProvider, ProtoSchema, BigQueryStream> streamCreator) {
        try {
            String streamName = BigQueryWriterUtils.getDefaultStreamName(config);
            GetWriteStreamRequest writeStreamRequest =
                    GetWriteStreamRequest.newBuilder()
                            .setName(streamName)
                            .setView(WriteStreamView.FULL)
                            .build();
            try (BigQueryWriteClient bigQueryInstance = bqWriterCreator.apply(config)) {
                // This WriteStream is to get the schema of the table.
                WriteStream writeStream = bigQueryInstance.getWriteStream(writeStreamRequest);
                TableSchema writeStreamTableSchema = writeStream.getTableSchema();
                // saving the descriptor for conversion
                descriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(writeStreamTableSchema);
                BigQueryStream bigQueryStream = streamCreator.apply(config, credCreator.apply(config), ProtoSchemaConverter.convert(descriptor));
                assert (bigQueryStream instanceof BigQueryProtoStream);
                // Actual object to write data.
                streamWriter = ((BigQueryProtoStream) bigQueryStream).getStreamWriter();
            }
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalArgumentException("Could not initialise the bigquery writer", e);
        }
    }

    @Override
    public void close() throws Exception {
        this.streamWriter.close();
    }

    // In the callback one can have the container and set the errors and/or log the response errors
    @Override
    public AppendRowsResponse appendAndGet(BigQueryPayload rows, ApiFutureCallback<AppendRowsResponse> callback)
            throws ExecutionException, InterruptedException {
        assert (rows instanceof BigQueryProtoPayload);
        ApiFuture<AppendRowsResponse> future = streamWriter.append((ProtoRows) rows.getPayload());
        ApiFutures.addCallback(future, callback, MoreExecutors.directExecutor());
        return future.get();
    }
}
