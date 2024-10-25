package com.gotocompany.depot.maxcompute;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.record.RecordConverter;
import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.message.Message;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.List;

@RequiredArgsConstructor
public class MaxComputeSink implements Sink {

    private final MaxComputeClient maxComputeClient;
    private final RecordConverter recordConverter;

    @Override
    public SinkResponse pushToSink(List<Message> messages) throws SinkException {
        SinkResponse sinkResponse = new SinkResponse();
        RecordWrappers recordWrappers = recordConverter.convert(messages);
        recordWrappers.getInvalidRecords()
                .forEach(invalidRecord -> sinkResponse.getErrors().put(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));
        try {
            maxComputeClient.insert(recordWrappers.getValidRecords());
        } catch (Exception e) {
            recordWrappers.getValidRecords()
                    .forEach(validRecord -> sinkResponse.getErrors().put(validRecord.getIndex(), new ErrorInfo(e, ErrorType.DEFAULT_ERROR)));
        }
        return sinkResponse;
    }

    @Override
    public void close() throws IOException {}

}