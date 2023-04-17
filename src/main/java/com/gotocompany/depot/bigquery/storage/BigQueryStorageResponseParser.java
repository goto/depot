package com.gotocompany.depot.bigquery.storage;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import io.grpc.Status.Code;
import com.google.rpc.Status;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

public class BigQueryStorageResponseParser {
    private static final int STATUS_4XX = 400;
    private static final int STATUS_5XX = 500;
    private static final int STATUS_6XX = 600;

    private static final Set<Code> RETRYABLE_ERROR_CODES =
            new HashSet<Code>() {{
                add(Code.INTERNAL);
                add(Code.ABORTED);
                add(Code.CANCELLED);
                add(Code.FAILED_PRECONDITION);
                add(Code.DEADLINE_EXCEEDED);
                add(Code.UNAVAILABLE);
            }};


    public static ErrorInfo getError(Status error) {
        if (error.getCode() >= STATUS_4XX && error.getCode() < STATUS_5XX) {
            return new ErrorInfo(new Exception(error.getMessage()), ErrorType.SINK_4XX_ERROR);
        }
        if (error.getCode() >= STATUS_5XX && error.getCode() < STATUS_6XX) {
            return new ErrorInfo(new Exception(error.getMessage()), ErrorType.SINK_5XX_ERROR);
        } else {
            return new ErrorInfo(new Exception(error.getMessage()), ErrorType.SINK_UNKNOWN_ERROR);
        }
    }

    public static boolean shouldRetry(io.grpc.Status status) {
        return BigQueryStorageResponseParser.RETRYABLE_ERROR_CODES.contains(status.getCode());
    }

    public static ErrorInfo get4xxError(RowError rowError) {
        return new ErrorInfo(new Exception(rowError.getMessage()), ErrorType.SINK_4XX_ERROR);
    }

    public static void setSinkResponseForInvalidMessages(
            BigQueryPayload payload,
            List<Message> messages,
            SinkResponse sinkResponse,
            Instrumentation instrumentation) {

        payload.forEach(meta -> {
            if (!meta.isValid()) {
                sinkResponse.addErrors(meta.getInputIndex(), meta.getErrorInfo());
                instrumentation.logError(
                        "Error {} occurred while converting to payload for record {}",
                        meta.getErrorInfo(),
                        messages.get((int) meta.getInputIndex()).getMetadataString());
            }
        });
    }

    public static void setSinkResponseForErrors(
            BigQueryPayload payload,
            AppendRowsResponse appendRowsResponse,
            List<Message> messages,
            SinkResponse sinkResponse,
            Instrumentation instrumentation) {

        if (appendRowsResponse.hasError()) {
            instrumentation.logError("received an error in stream :{} ", appendRowsResponse.getError());
            com.google.rpc.Status error = appendRowsResponse.getError();
            ErrorInfo errorInfo = BigQueryStorageResponseParser.getError(error);
            Set<Long> payloadIndexes = payload.getPayloadIndexes();
            payloadIndexes.forEach(index -> {
                long inputIndex = payload.getInputIndex(index);
                sinkResponse.addErrors(inputIndex, errorInfo);
            });
        }

        //per message error
        List<RowError> rowErrorsList = appendRowsResponse.getRowErrorsList();
        rowErrorsList.forEach(rowError -> {
            ErrorInfo errorInfo = BigQueryStorageResponseParser.get4xxError(rowError);
            long inputIndex = payload.getInputIndex(rowError.getIndex());
            sinkResponse.addErrors(inputIndex, errorInfo);
            String metadataString = messages.get((int) inputIndex).getMetadataString();
            instrumentation.logError(
                    "Error {} occurred while sending the payload for record {} with RowError {}",
                    errorInfo,
                    metadataString,
                    rowError);
        });
    }

    public static void setSinkResponseForException(
            Throwable cause,
            BigQueryPayload payload,
            List<Message> messages,
            SinkResponse sinkResponse,
            Instrumentation instrumentation) {
        io.grpc.Status status = io.grpc.Status.fromThrowable(cause);
        instrumentation.logError("Error from exception: {} ", status.getDescription());
        if (BigQueryStorageResponseParser.shouldRetry(status)) {
            IntStream.range(0, messages.size())
                    .forEach(index -> sinkResponse.addErrors(index, new ErrorInfo(new Exception(cause), ErrorType.SINK_5XX_ERROR)));
        } else {
            IntStream.range(0, messages.size())
                    .forEach(index -> sinkResponse.addErrors(index, new ErrorInfo(new Exception(cause), ErrorType.SINK_4XX_ERROR)));
        }
        if (cause instanceof Exceptions.AppendSerializationError) {
            Exceptions.AppendSerializationError ase = (Exceptions.AppendSerializationError) cause;
            Map<Integer, String> rowIndexToErrorMessage = ase.getRowIndexToErrorMessage();
            rowIndexToErrorMessage.forEach((index, err) -> {
                long inputIndex = payload.getInputIndex(index);
                String metadataString = messages.get((int) inputIndex).getMetadataString();
                ErrorInfo errorInfo = new ErrorInfo(new Exception(err), ErrorType.SINK_4XX_ERROR);
                instrumentation.logError(
                        "Error {} occurred while sending the payload for record {}",
                        errorInfo,
                        metadataString);
                sinkResponse.addErrors(inputIndex, errorInfo);
            });
        }
    }

    public static AppendRowsResponse get4xxErrorResponse() {
        return AppendRowsResponse.newBuilder().setError(Status.newBuilder().setCode(STATUS_4XX).build()).build();
    }
}
