package com.gotocompany.depot.maxcompute.converter.record;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.UnknownFieldsException;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.record.RecordDecorator;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.message.Message;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RequiredArgsConstructor
public class RecordConverter implements MessageRecordConverter {

    private final RecordDecorator recordDecorator;
    private final MaxComputeSchemaCache maxComputeSchemaCache;

    @Override
    public List<RecordWrapper> convert(List<Message> messages) {
        MaxComputeSchema maxComputeSchema = maxComputeSchemaCache.getMaxComputeSchema();
        return IntStream.range(0, messages.size())
                .mapToObj(index -> {
                    Record record = new ArrayRecord(maxComputeSchema.getColumns());
                    RecordWrapper recordWrapper = new RecordWrapper(record, index, null, null);
                    try {
                        recordDecorator.decorate(recordWrapper, messages.get(index));
                    } catch (IOException e) {
                        recordWrapper.setRecord(null);
                        recordWrapper.setErrorInfo(new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
                    } catch (UnknownFieldsException e) {
                        recordWrapper.setRecord(null);
                        recordWrapper.setErrorInfo(new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR));
                    }
                    return recordWrapper;
                }).collect(Collectors.toList());
    }
}
