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

public class BigTableSink implements Sink {
    private final BigTableClient bigTableClient;
    private final BigTableRecordParser bigTableRecordParser;
    private final BigTableMetrics bigtableMetrics;
    private final Instrumentation instrumentation;

    public BigTableSink(BigTableClient bigTableClient, BigTableRecordParser bigTableRecordParser, BigTableMetrics bigtableMetrics, Instrumentation instrumentation) {
        this.bigTableClient = bigTableClient;
        this.bigTableRecordParser = bigTableRecordParser;
        this.bigtableMetrics = bigtableMetrics;
        this.instrumentation = instrumentation;
    }

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

    @Override
    public void close() throws IOException {
    }
}
