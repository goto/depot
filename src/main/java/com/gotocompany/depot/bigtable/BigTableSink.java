package com.gotocompany.depot.bigtable;

import com.gotocompany.depot.bigtable.client.BigTableClient;
import com.gotocompany.depot.bigtable.model.BigTableRecord;
import com.gotocompany.depot.bigtable.parser.BigTableRecordParser;
import com.gotocompany.depot.bigtable.parser.BigTableResponseParser;
import com.gotocompany.depot.bigtable.response.BigTableResponse;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.BigTableMetrics;
import com.gotocompany.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Sink implementation that writes records to a Google Cloud Bigtable table.
 *
 * <p>{@code BigTableSink} is the Bigtable-specific realization of the {@link Sink} contract. For each
 * batch it converts the incoming {@link Message}s into {@link BigTableRecord}s with a
 * {@link BigTableRecordParser}, separates records that failed conversion from valid ones, and sends
 * the valid records to Bigtable through a {@link BigTableClient}. Both conversion failures and write
 * failures are reported per record in the returned {@link SinkResponse}; write failures are first
 * translated into classified {@link ErrorInfo}s by {@link BigTableResponseParser}.</p>
 *
 * <p>Instances are assembled by {@link BigTableSinkFactory}. Per-operation latency, throughput and
 * error metrics are emitted through the supplied {@link BigTableMetrics} and {@link Instrumentation}.</p>
 *
 * @see Sink
 * @see BigTableClient
 * @see BigTableRecordParser
 * @see BigTableResponseParser
 */
public class BigTableSink implements Sink {
    /** Client that applies record mutations to Bigtable and reports failed mutations. */
    private final BigTableClient bigTableClient;
    /** Parser that converts incoming messages into {@link BigTableRecord} mutations. */
    private final BigTableRecordParser bigTableRecordParser;
    /** Metric names and tags for Bigtable operations and errors. */
    private final BigTableMetrics bigtableMetrics;
    /** Logging and metrics facade for this sink. */
    private final Instrumentation instrumentation;

    /**
     * Creates a Bigtable sink from its collaborators.
     *
     * @param bigTableClient the client used to send record mutations to Bigtable
     * @param bigTableRecordParser the parser that converts messages into {@link BigTableRecord}s
     * @param bigtableMetrics the Bigtable metric names and tags used when reporting errors
     * @param instrumentation the logging and metrics facade for this sink
     */
    public BigTableSink(BigTableClient bigTableClient, BigTableRecordParser bigTableRecordParser, BigTableMetrics bigtableMetrics, Instrumentation instrumentation) {
        this.bigTableClient = bigTableClient;
        this.bigTableRecordParser = bigTableRecordParser;
        this.bigtableMetrics = bigtableMetrics;
        this.instrumentation = instrumentation;
    }

    /**
     * Converts a batch of messages to Bigtable mutations, writes the valid ones, and reports failures.
     *
     * <p>Processing proceeds in three steps. First every {@link Message} is converted to a
     * {@link BigTableRecord} and the results are partitioned by {@link BigTableRecord#isValid()};
     * records that failed conversion contribute their {@link ErrorInfo} to the response immediately,
     * keyed by the originating message index. Second, when any valid records remain they are sent to
     * Bigtable via {@link BigTableClient#send(java.util.List)}. Third, if the client reports failed
     * mutations, those are classified by
     * {@link BigTableResponseParser#getErrorsFromSinkResponse(java.util.List, BigTableResponse, BigTableMetrics, Instrumentation)}
     * and merged into the response. When no valid records exist the client is not called.</p>
     *
     * @param messages the batch of records to write to Bigtable
     * @return a {@link SinkResponse} holding an error for every record that failed conversion or whose
     *     Bigtable mutation failed; empty of errors when every record was written
     */
    @Override
    public SinkResponse pushToSink(List<Message> messages) {
        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        Map<Boolean, List<BigTableRecord>> splitterRecords = records.stream().collect(Collectors.partitioningBy(BigTableRecord::isValid));
        List<BigTableRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<BigTableRecord> validRecords = splitterRecords.get(Boolean.TRUE);

        SinkResponse sinkResponse = new SinkResponse();
        invalidRecords.forEach(invalidRecord -> sinkResponse.addErrors(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));

        if (validRecords.size() > 0) {
            BigTableResponse bigTableResponse = bigTableClient.send(validRecords);
            if (bigTableResponse != null && bigTableResponse.hasErrors()) {
                instrumentation.logInfo("Found {} Error records in response", bigTableResponse.getErrorCount());
                Map<Long, ErrorInfo> errorInfoMap = BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigTableResponse, bigtableMetrics, instrumentation);
                errorInfoMap.forEach(sinkResponse::addErrors);
            }
        }

        return sinkResponse;
    }

    /**
     * Releases resources held by this sink.
     *
     * <p>The lifecycle of the underlying Bigtable clients is managed by the {@link BigTableSinkFactory}
     * that created them, so this implementation performs no work.</p>
     *
     * @throws IOException declared by {@link java.io.Closeable#close()}; never actually thrown by this
     *     implementation
     */
    @Override
    public void close() throws IOException {
    }
}
