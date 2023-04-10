package com.gotocompany.depot.bigquery.storage.proto;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.GetWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.bigquery.storage.v1.WriteStreamView;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryStream;
import com.gotocompany.depot.bigquery.storage.BigQueryWriter;
import com.gotocompany.depot.bigquery.storage.BigQueryWriterUtils;
import com.gotocompany.depot.common.Function3;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;
import java.util.function.Function;

@Slf4j
public class BigQueryProtoWriter implements BigQueryWriter {

    private final BigQuerySinkConfig config;
    private final Function<BigQuerySinkConfig, BigQueryWriteClient> bqWriterCreator;
    private final Function<BigQuerySinkConfig, CredentialsProvider> credCreator;
    private final Function3<BigQuerySinkConfig, CredentialsProvider, ProtoSchema, BigQueryStream> streamCreator;
    @Getter
    private StreamWriter streamWriter;
    @Getter
    private Descriptors.Descriptor descriptor;

    public BigQueryProtoWriter(BigQuerySinkConfig config,
                               Function<BigQuerySinkConfig, BigQueryWriteClient> bqWriterCreator,
                               Function<BigQuerySinkConfig, CredentialsProvider> credCreator,
                               Function3<BigQuerySinkConfig, CredentialsProvider, ProtoSchema, BigQueryStream> streamCreator) {
        this.config = config;
        this.bqWriterCreator = bqWriterCreator;
        this.credCreator = credCreator;
        this.streamCreator = streamCreator;
    }

    @Override
    public void init() {
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
                // saving the descriptor for conversion
                descriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(writeStream.getTableSchema());
                BigQueryStream bigQueryStream = streamCreator.apply(config,
                        credCreator.apply(config),
                        ProtoSchemaConverter.convert(descriptor));
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
            throws ExecutionException, InterruptedException, Descriptors.DescriptorValidationException {
        ApiFuture<AppendRowsResponse> future;
        // need to synchronize
        synchronized (this) {
            TableSchema updatedSchema = this.streamWriter.getUpdatedSchema();
            if (updatedSchema != null) {
                log.info("Updated table schema detected, recreating stream writer");
                // Close the StreamWriter
                this.streamWriter.close();
                this.descriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(updatedSchema);
                BigQueryStream bigQueryStream = streamCreator.apply(config,
                        credCreator.apply(config),
                        ProtoSchemaConverter.convert(descriptor));
                assert (bigQueryStream instanceof BigQueryProtoStream);
                // Recreate stream writer
                streamWriter = ((BigQueryProtoStream) bigQueryStream).getStreamWriter();
            }
            future = streamWriter.append((ProtoRows) rows.getPayload());
        }
        ApiFutures.addCallback(future, callback, MoreExecutors.directExecutor());
        return future.get();
    }
}
