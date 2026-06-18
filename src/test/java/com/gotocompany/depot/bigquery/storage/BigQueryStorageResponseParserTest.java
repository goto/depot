package com.gotocompany.depot.bigquery.storage;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.rpc.Code;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.bigquery.storage.proto.BigQueryRecordMeta;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link BigQueryStorageResponseParser}, which interprets BigQuery Storage Write API
 * responses and exceptions and records the resulting per-message errors on a {@link SinkResponse}.
 *
 * <p>The tests cover the static classification helpers ({@code getError}, {@code shouldRetry} and
 * {@code get4xxError}) as well as the instance methods that translate conversion failures,
 * {@link AppendRowsResponse} errors and row errors, and thrown exceptions (including gRPC
 * {@link StatusRuntimeException} and {@link Exceptions.AppendSerializationError}) into
 * {@link ErrorInfo} entries keyed by input index. Index remapping from valid-payload positions back
 * to original input positions is asserted throughout.</p>
 */
public class BigQueryStorageResponseParserTest {

    /** Mocked instrumentation used to verify error logging. */
    private Instrumentation instrumentation;
    /** Parser under test, wired with mocked config, instrumentation and BigQuery metrics. */
    private BigQueryStorageResponseParser responseParser;

    /**
     * Builds the parser under test with mocked configuration, instrumentation and metrics.
     */
    @Before
    public void setup() {
        instrumentation = Mockito.mock(Instrumentation.class);
        BigQuerySinkConfig sinkConfig = Mockito.mock(BigQuerySinkConfig.class);
        BigQueryMetrics bigQueryMetrics = new BigQueryMetrics(sinkConfig);
        responseParser = new BigQueryStorageResponseParser(sinkConfig, instrumentation, bigQueryMetrics);
    }

    /**
     * Verifies that a gRPC status code is mapped to the correct error type and message.
     *
     * <p>Given a {@code PERMISSION_DENIED} status, when {@code getError} runs, then it yields a
     * {@link ErrorType#SINK_4XX_ERROR} carrying the status message; given an {@code INTERNAL} status,
     * it yields a {@link ErrorType#SINK_5XX_ERROR} with its message.</p>
     */
    @Test
    public void shouldReturnErrorFromStatus() {
        com.google.rpc.Status status = com.google.rpc.Status.newBuilder().setCode(Code.PERMISSION_DENIED_VALUE).setMessage("test error").build();
        ErrorInfo error = BigQueryStorageResponseParser.getError(status);
        assert error != null;
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, error.getErrorType());
        Assert.assertEquals("test error", error.getException().getMessage());

        status = com.google.rpc.Status.newBuilder().setCode(Code.INTERNAL_VALUE).setMessage("test 5xx error").build();
        error = BigQueryStorageResponseParser.getError(status);
        assert error != null;
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, error.getErrorType());
        Assert.assertEquals("test 5xx error", error.getException().getMessage());
    }

    /**
     * Verifies the retryability classification of gRPC statuses.
     *
     * <p>Asserts that {@code shouldRetry} returns {@code true} for {@code ABORTED}, {@code INTERNAL},
     * {@code CANCELLED}, {@code FAILED_PRECONDITION}, {@code DEADLINE_EXCEEDED} and
     * {@code UNAVAILABLE}, and {@code false} for the remaining statuses such as {@code OK},
     * {@code INVALID_ARGUMENT}, {@code NOT_FOUND} and {@code RESOURCE_EXHAUSTED}.</p>
     */
    @Test
    public void shouldReturnRetryBoolean() {
        Assert.assertTrue(BigQueryStorageResponseParser.shouldRetry(Status.ABORTED));
        Assert.assertTrue(BigQueryStorageResponseParser.shouldRetry(Status.INTERNAL));
        Assert.assertTrue(BigQueryStorageResponseParser.shouldRetry(Status.CANCELLED));
        Assert.assertTrue(BigQueryStorageResponseParser.shouldRetry(Status.FAILED_PRECONDITION));
        Assert.assertTrue(BigQueryStorageResponseParser.shouldRetry(Status.DEADLINE_EXCEEDED));
        Assert.assertTrue(BigQueryStorageResponseParser.shouldRetry(Status.UNAVAILABLE));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.OK));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.UNKNOWN));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.INVALID_ARGUMENT));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.NOT_FOUND));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.ALREADY_EXISTS));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.PERMISSION_DENIED));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.RESOURCE_EXHAUSTED));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.OUT_OF_RANGE));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.UNIMPLEMENTED));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.DATA_LOSS));
        Assert.assertFalse(BigQueryStorageResponseParser.shouldRetry(Status.UNAUTHENTICATED));
    }

    /**
     * Verifies that a BigQuery {@link RowError} is mapped to a 4xx error.
     *
     * <p>Given a row error with a message, when {@code get4xxError} runs, then it returns an
     * {@link ErrorInfo} of type {@link ErrorType#SINK_4XX_ERROR} carrying that message.</p>
     */
    @Test
    public void shouldReturn4xx() {
        RowError rowError = Mockito.mock(RowError.class);
        Mockito.when(rowError.getMessage()).thenReturn("row error");
        ErrorInfo error = BigQueryStorageResponseParser.get4xxError(rowError);
        Assert.assertEquals("row error", error.getException().getMessage());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, error.getErrorType());
    }

    /**
     * Verifies that records flagged invalid during payload conversion are reported as errors.
     *
     * <p>Given a {@link BigQueryPayload} whose metadata records mix valid entries with three invalid
     * ones (deserialization, unknown-fields and invalid-message), when
     * {@code setSinkResponseForInvalidMessages} runs, then the {@link SinkResponse} contains exactly
     * those three errors with their original types and messages, and a conversion error is logged for
     * each invalid record's metadata string.</p>
     */
    @Test
    public void shouldSetErrorResponse() {
        BigQueryPayload payload = new BigQueryPayload();
        payload.addMetadataRecord(new BigQueryRecordMeta(0, null, true));
        payload.addMetadataRecord(new BigQueryRecordMeta(1, new ErrorInfo(new Exception("error1"), ErrorType.DESERIALIZATION_ERROR), false));
        payload.addMetadataRecord(new BigQueryRecordMeta(2, null, true));
        payload.addMetadataRecord(new BigQueryRecordMeta(3, new ErrorInfo(new Exception("error2"), ErrorType.UNKNOWN_FIELDS_ERROR), false));
        payload.addMetadataRecord(new BigQueryRecordMeta(4, new ErrorInfo(new Exception("error3"), ErrorType.INVALID_MESSAGE_ERROR), false));
        List<Message> messages = createMockMessages();
        Mockito.when(messages.get(1).getMetadataString()).thenReturn("meta1");
        Mockito.when(messages.get(3).getMetadataString()).thenReturn("meta2");
        Mockito.when(messages.get(4).getMetadataString()).thenReturn("meta3");
        SinkResponse response = new SinkResponse();
        responseParser.setSinkResponseForInvalidMessages(payload, messages, response);
        Assert.assertEquals(3, response.getErrors().size());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, response.getErrors().get(1L).getErrorType());
        Assert.assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, response.getErrors().get(3L).getErrorType());
        Assert.assertEquals(ErrorType.INVALID_MESSAGE_ERROR, response.getErrors().get(4L).getErrorType());
        Assert.assertEquals("error1", response.getErrors().get(1L).getException().getMessage());
        Assert.assertEquals("error2", response.getErrors().get(3L).getException().getMessage());
        Assert.assertEquals("error3", response.getErrors().get(4L).getException().getMessage());

        List<BigQueryRecordMeta> metaList = new ArrayList<>();
        payload.forEach(metaList::add);

        Mockito.verify(instrumentation, Mockito.times(1)).logError("Error {} occurred while converting to payload for record {}",
                metaList.get(1).getErrorInfo(), "meta1");
        Mockito.verify(instrumentation, Mockito.times(1)).logError("Error {} occurred while converting to payload for record {}",
                metaList.get(3).getErrorInfo(), "meta2");
        Mockito.verify(instrumentation, Mockito.times(1)).logError("Error {} occurred while converting to payload for record {}",
                metaList.get(4).getErrorInfo(), "meta3");

    }

    /**
     * Verifies that a request-level append error is applied to all mapped rows.
     *
     * <p>Given a payload mapping valid indexes {@code 0,1,2} to input indexes {@code 0,3,4} and an
     * {@link AppendRowsResponse} carrying an {@code UNAVAILABLE} status, when
     * {@code setSinkResponseForErrors} runs, then each input index is recorded as a
     * {@link ErrorType#SINK_5XX_ERROR} with the response's error message.</p>
     */
    @Test
    public void shouldSetResponseForError() {
        BigQueryPayload payload = new BigQueryPayload();
        payload.putValidIndexToInputIndex(0L, 0L);
        payload.putValidIndexToInputIndex(1L, 3L);
        payload.putValidIndexToInputIndex(2L, 4L);
        List<Message> messages = createMockMessages();
        AppendRowsResponse appendRowsResponse = AppendRowsResponse.newBuilder().setError(com.google.rpc.Status.newBuilder().setMessage("test error").setCode(Code.UNAVAILABLE_VALUE).build()).build();
        SinkResponse sinkResponse = new SinkResponse();
        responseParser.setSinkResponseForErrors(payload, appendRowsResponse, messages, sinkResponse);
        Assert.assertEquals(3, sinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrors().get(0L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrors().get(3L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrors().get(4L).getErrorType());

        Assert.assertEquals("test error", sinkResponse.getErrors().get(0L).getException().getMessage());
        Assert.assertEquals("test error", sinkResponse.getErrors().get(3L).getException().getMessage());
        Assert.assertEquals("test error", sinkResponse.getErrors().get(4L).getException().getMessage());
    }

    /**
     * Verifies that per-row append errors override the request-level error for their rows.
     *
     * <p>Given the same index mapping plus row errors at valid indexes {@code 1} and {@code 2}, when
     * {@code setSinkResponseForErrors} runs, then input index {@code 0} keeps the request-level
     * {@link ErrorType#SINK_5XX_ERROR} while input indexes {@code 3} and {@code 4} become
     * {@link ErrorType#SINK_4XX_ERROR} with their respective row-error messages.</p>
     */
    @Test
    public void shouldSetResponseForRowError() {
        BigQueryPayload payload = new BigQueryPayload();
        payload.putValidIndexToInputIndex(0L, 0L);
        payload.putValidIndexToInputIndex(1L, 3L);
        payload.putValidIndexToInputIndex(2L, 4L);
        List<Message> messages = createMockMessages();
        AppendRowsResponse appendRowsResponse = AppendRowsResponse.newBuilder()
                .setError(com.google.rpc.Status.newBuilder().setMessage("test error").setCode(Code.UNAVAILABLE_VALUE).build())
                .addRowErrors(RowError.newBuilder().setIndex(1L).setMessage("row error1").build())
                .addRowErrors(RowError.newBuilder().setIndex(2L).setMessage("row error2").build())
                .build();
        SinkResponse sinkResponse = new SinkResponse();
        responseParser.setSinkResponseForErrors(payload, appendRowsResponse, messages, sinkResponse);
        Assert.assertEquals(3, sinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrors().get(0L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, sinkResponse.getErrors().get(3L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, sinkResponse.getErrors().get(4L).getErrorType());

        Assert.assertEquals("test error", sinkResponse.getErrors().get(0L).getException().getMessage());
        Assert.assertEquals("row error1", sinkResponse.getErrors().get(3L).getException().getMessage());
        Assert.assertEquals("row error2", sinkResponse.getErrors().get(4L).getException().getMessage());
    }

    /**
     * Builds a list of five mocked {@link Message} instances for use as the batch under test.
     *
     * @return a mutable list of five Mockito-mocked messages
     */
    private List<Message> createMockMessages() {
        List<Message> messages = new ArrayList<>();
        Message m1 = Mockito.mock(Message.class);
        Message m2 = Mockito.mock(Message.class);
        Message m3 = Mockito.mock(Message.class);
        Message m4 = Mockito.mock(Message.class);
        Message m5 = Mockito.mock(Message.class);
        messages.add(m1);
        messages.add(m2);
        messages.add(m3);
        messages.add(m4);
        messages.add(m5);
        return messages;
    }

    /**
     * Verifies that a retryable gRPC exception marks every mapped row as a 5xx error.
     *
     * <p>Given a {@link StatusRuntimeException} with {@code INTERNAL} status and five mapped rows, when
     * {@code setSinkResponseForException} runs, then all five input indexes are recorded as
     * {@link ErrorType#SINK_5XX_ERROR} carrying the exception's message.</p>
     */
    @Test
    public void shouldSetSinkResponseForException() {
        Throwable cause = new StatusRuntimeException(Status.INTERNAL);
        BigQueryPayload payload = new BigQueryPayload();
        payload.putValidIndexToInputIndex(0L, 0L);
        payload.putValidIndexToInputIndex(1L, 1L);
        payload.putValidIndexToInputIndex(2L, 2L);
        payload.putValidIndexToInputIndex(3L, 3L);
        payload.putValidIndexToInputIndex(4L, 4L);
        List<Message> messages = createMockMessages();
        SinkResponse response = new SinkResponse();
        responseParser.setSinkResponseForException(cause, payload, messages, response);
        Assert.assertEquals(5, response.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, response.getErrors().get(0L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, response.getErrors().get(1L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, response.getErrors().get(2L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, response.getErrors().get(3L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, response.getErrors().get(4L).getErrorType());
        Assert.assertEquals("io.grpc.StatusRuntimeException: INTERNAL", response.getErrors().get(0L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: INTERNAL", response.getErrors().get(1L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: INTERNAL", response.getErrors().get(2L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: INTERNAL", response.getErrors().get(3L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: INTERNAL", response.getErrors().get(4L).getException().getMessage());
    }

    /**
     * Verifies that a non-retryable gRPC exception marks every mapped row as a 4xx error.
     *
     * <p>Given a {@link StatusRuntimeException} with {@code RESOURCE_EXHAUSTED} status and five mapped
     * rows, when {@code setSinkResponseForException} runs, then all five input indexes are recorded as
     * {@link ErrorType#SINK_4XX_ERROR} carrying the exception's message.</p>
     */
    @Test
    public void shouldSetSinkResponseForExceptionWithNonRetry() {
        Throwable cause = new StatusRuntimeException(Status.RESOURCE_EXHAUSTED);
        BigQueryPayload payload = new BigQueryPayload();
        payload.putValidIndexToInputIndex(0L, 0L);
        payload.putValidIndexToInputIndex(1L, 1L);
        payload.putValidIndexToInputIndex(2L, 2L);
        payload.putValidIndexToInputIndex(3L, 3L);
        payload.putValidIndexToInputIndex(4L, 4L);
        List<Message> messages = createMockMessages();
        SinkResponse response = new SinkResponse();
        responseParser.setSinkResponseForException(cause, payload, messages, response);
        Assert.assertEquals(5, response.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(0L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(1L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(2L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(3L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(4L).getErrorType());
        Assert.assertEquals("io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED", response.getErrors().get(0L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED", response.getErrors().get(1L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED", response.getErrors().get(2L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED", response.getErrors().get(3L).getException().getMessage());
        Assert.assertEquals("io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED", response.getErrors().get(4L).getException().getMessage());
    }

    /**
     * Verifies that an append serialization error is applied per row by its row index.
     *
     * <p>Given an {@link Exceptions.AppendSerializationError} carrying per-row messages for rows
     * {@code 0} and {@code 2} and a payload mapping valid indexes {@code 0,1,2} to input indexes
     * {@code 0,3,4}, when {@code setSinkResponseForException} runs, then the rows with a specific
     * message become {@link ErrorType#SINK_4XX_ERROR} with that message, while the unmapped row
     * (input index {@code 3}) falls back to a {@link ErrorType#SINK_5XX_ERROR} carrying the
     * serialization error's own message.</p>
     */
    @Test
    public void shouldSetSinkResponseForExceptionWithAppendError() {
        Map<Integer, String> rowsToErrorMessages = new HashMap<>();
        rowsToErrorMessages.put(0, "message1");
        rowsToErrorMessages.put(2, "message2");
        Throwable cause = new Exceptions.AppendSerializationError(404, "test error", "default", rowsToErrorMessages);
        BigQueryPayload payload = new BigQueryPayload();
        payload.putValidIndexToInputIndex(0L, 0L);
        payload.putValidIndexToInputIndex(1L, 3L);
        payload.putValidIndexToInputIndex(2L, 4L);
        List<Message> messages = createMockMessages();
        SinkResponse response = new SinkResponse();
        responseParser.setSinkResponseForException(cause, payload, messages, response);
        Assert.assertEquals(3, response.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(0L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, response.getErrors().get(3L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(4L).getErrorType());
        Assert.assertEquals("message1", response.getErrors().get(0L).getException().getMessage());
        Assert.assertEquals("com.google.cloud.bigquery.storage.v1.Exceptions$AppendSerializationError: UNKNOWN: test error", response.getErrors().get(3L).getException().getMessage());
        Assert.assertEquals("message2", response.getErrors().get(4L).getException().getMessage());
    }
}
