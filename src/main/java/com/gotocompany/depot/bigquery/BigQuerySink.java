package com.gotocompany.depot.bigquery;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.gotocompany.depot.bigquery.client.BigQueryClient;
import com.gotocompany.depot.bigquery.client.BigQueryResponseParser;
import com.gotocompany.depot.bigquery.client.BigQueryRow;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverterCache;
import com.gotocompany.depot.bigquery.handler.ErrorHandler;
import com.gotocompany.depot.bigquery.models.Record;
import com.gotocompany.depot.bigquery.models.Records;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * {@link Sink} implementation that writes records to BigQuery using the legacy {@code insertAll} streaming API.
 *
 * <p>For each batch of messages the sink converts them to {@link Record} instances via the
 * cached converter, reports invalid records as errors, and inserts the valid records into
 * the destination table. Insertion errors returned by BigQuery are parsed into the sink
 * response and forwarded to the configured {@link ErrorHandler}, which may, for example,
 * repair an inferred schema before the next attempt.</p>
 *
 * @see BigQuerySinkFactory
 * @see BigQueryStorageAPISink
 */
public class BigQuerySink implements Sink {

    /** Client used to obtain the table id and perform {@code insertAll} requests. */
    private final BigQueryClient bigQueryClient;
    /** Builds a BigQuery row (with or without an insert id) from each record. */
    private final BigQueryRow rowCreator;
    /** Cache supplying the converter that turns messages into records. */
    private final MessageRecordConverterCache messageRecordConverterCache;
    /** Instrumentation used to log batch insert outcomes. */
    private final Instrumentation instrumentation;
    /** Metrics used when parsing BigQuery insertion errors. */
    private final BigQueryMetrics bigQueryMetrics;
    /** Handler invoked with the per-row errors returned by a batch insert. */
    private final ErrorHandler errorHandler;

    /**
     * Creates a BigQuery sink from its collaborators.
     *
     * @param client          the BigQuery client used to resolve the table and insert rows
     * @param converterCache  the cache providing the message-to-record converter
     * @param rowCreator      the strategy that turns a record into a BigQuery row
     * @param bigQueryMetrics the metrics used while parsing insertion errors
     * @param instrumentation the instrumentation used to log insert outcomes
     * @param errorHandler    the handler invoked with the errors returned by a batch insert
     */
    public BigQuerySink(BigQueryClient client,
                        MessageRecordConverterCache converterCache,
                        BigQueryRow rowCreator,
                        BigQueryMetrics bigQueryMetrics,
                        Instrumentation instrumentation,
                        ErrorHandler errorHandler) {
        this.bigQueryClient = client;
        this.messageRecordConverterCache = converterCache;
        this.rowCreator = rowCreator;
        this.instrumentation = instrumentation;
        this.bigQueryMetrics = bigQueryMetrics;
        this.errorHandler = errorHandler;
    }

    /**
     * Closes the sink.
     *
     * <p>This implementation holds no resources that require releasing, so the method does
     * nothing.</p>
     *
     * @throws IOException declared by {@link Sink}; never thrown by this implementation
     */
    @Override
    public void close() throws IOException {
    }

    /**
     * Inserts the given records into the destination table in a single request.
     *
     * <p>Builds an {@code insertAll} request for the client's table id, adding one row per
     * record via the configured row creator, and executes it.</p>
     *
     * @param records the valid records to insert
     * @return the {@link InsertAllResponse} returned by BigQuery, which may report per-row
     *         errors
     */
    private InsertAllResponse insertIntoBQ(List<Record> records) {
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(bigQueryClient.getTableID());
        records.forEach((Record m) -> builder.addRow(rowCreator.of(m)));
        return bigQueryClient.insertAll(builder.build());
    }

    /**
     * Converts a batch of messages and writes the valid ones to BigQuery.
     *
     * <p>Converts the messages to records, records every invalid record as an error in the
     * response, and, when there are valid records, inserts them. If BigQuery reports
     * insertion errors, they are parsed into the response and the configured
     * {@link ErrorHandler} is invoked with the raw errors and the inserted records.</p>
     *
     * @param messageList the messages to push to the sink
     * @return a {@link SinkResponse} containing an entry for every message that failed,
     *         keyed by its index in the batch
     */
    @Override
    public SinkResponse pushToSink(List<Message> messageList) {
        Records records = messageRecordConverterCache.getMessageRecordConverter().convert(messageList);
        SinkResponse sinkResponse = new SinkResponse();
        records.getInvalidRecords().forEach(invalidRecord -> sinkResponse.addErrors(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));
        if (!records.getValidRecords().isEmpty()) {
            InsertAllResponse response = insertIntoBQ(records.getValidRecords());
            instrumentation.logInfo("Pushed a batch of {} records to BQ. Insert success?: {}", records.getValidRecords().size(), !response.hasErrors());
            if (response.hasErrors()) {
                Map<Long, ErrorInfo> errorInfoMap = BigQueryResponseParser.getErrorsFromBQResponse(records.getValidRecords(), response, bigQueryMetrics, instrumentation);
                errorInfoMap.forEach(sinkResponse::addErrors);
                errorHandler.handle(response.getInsertErrors(), records.getValidRecords());
            }
        }
        return sinkResponse;
    }
}
