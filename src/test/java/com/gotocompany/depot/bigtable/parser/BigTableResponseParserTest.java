package com.gotocompany.depot.bigtable.parser;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ErrorDetails;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.rpc.BadRequest;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.QuotaFailure;
import com.gotocompany.depot.TestBookingLogKey;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestServiceType;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.bigtable.model.BigTableRecord;
import com.gotocompany.depot.bigtable.response.BigTableResponse;
import com.gotocompany.depot.metrics.BigTableMetrics;
import org.aeonbits.owner.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link BigTableResponseParser}, which converts a failed {@link BigTableResponse} into
 * a map of record index to {@link ErrorInfo} and emits per-failure error metrics.
 *
 * <p>The {@link ApiException} and its {@link StatusCode}, {@link StatusCode.Code} and
 * {@link ErrorDetails} are Mockito mocks wired together in {@link #setUp()}, alongside mocked
 * {@link BigTableMetrics} and {@link Instrumentation} and two valid {@link BigTableRecord}s. Each test
 * builds a {@link MutateRowsException} with a single failed mutation, varies the HTTP status code,
 * retryability and error-detail kind, and then asserts the {@link ErrorType} mapped by
 * {@link BigTableResponseParser#getErrorsFromSinkResponse(java.util.List, BigTableResponse, BigTableMetrics, Instrumentation)}
 * as well as the error-type counters and the error logging.</p>
 */
public class BigTableResponseParserTest {
    /** Mock metrics providing the error counter names and tags. */
    @Mock
    private BigTableMetrics bigtableMetrics;
    /** Mock instrumentation whose counter increments and error logging are verified. */
    @Mock
    private Instrumentation instrumentation;
    /** Mock API exception describing a single failed mutation. */
    @Mock
    private ApiException apiException;
    /** Mock status code returned by the API exception. */
    @Mock
    private StatusCode statusCode;
    /** Mock status code enum whose HTTP status drives the mapped error type. */
    @Mock
    private StatusCode.Code code;
    /** Mock error details used to select the Bigtable error-type metric. */
    @Mock
    private ErrorDetails errorDetails;
    /** Two valid records (indices {@code 0} and {@code 1}) whose failures are looked up by index. */
    private List<BigTableRecord> validRecords;

    /**
     * Wires the mocked API exception graph and builds the record fixtures before each test.
     *
     * <p>Stubs the {@link ApiException} to expose the mocked {@link StatusCode},
     * {@link StatusCode.Code} and {@link ErrorDetails}, a reason string, a non-retryable flag and
     * itself as the cause, with all error-detail kinds defaulting to {@code null}. Also builds two
     * booking-log {@link Message}s and the matching valid {@link BigTableRecord}s at indices
     * {@code 0} and {@code 1}.</p>
     */
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.when(apiException.getStatusCode()).thenReturn(statusCode);
        Mockito.when(statusCode.getCode()).thenReturn(code);
        Mockito.when(apiException.getErrorDetails()).thenReturn(errorDetails);
        Mockito.when(apiException.getReason()).thenReturn("REASON_STRING");
        Mockito.when(apiException.isRetryable()).thenReturn(Boolean.FALSE);
        Mockito.when(apiException.getCause()).thenReturn(apiException);
        Mockito.when(errorDetails.getBadRequest()).thenReturn(null);
        Mockito.when(errorDetails.getQuotaFailure()).thenReturn(null);
        Mockito.when(errorDetails.getPreconditionFailure()).thenReturn(null);

        TestBookingLogKey bookingLogKey1 = TestBookingLogKey.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").build();
        TestBookingLogMessage bookingLogMessage1 = TestBookingLogMessage.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").setServiceType(TestServiceType.Enum.GO_SEND).build();
        TestBookingLogKey bookingLogKey2 = TestBookingLogKey.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2").build();
        TestBookingLogMessage bookingLogMessage2 = TestBookingLogMessage.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2").setServiceType(TestServiceType.Enum.GO_SHOP).build();

        Message message1 = new Message(bookingLogKey1.toByteArray(), bookingLogMessage1.toByteArray());
        Message message2 = new Message(bookingLogKey2.toByteArray(), bookingLogMessage2.toByteArray());

        RowMutationEntry rowMutationEntry1 = RowMutationEntry.create("rowKey1").setCell("family1", "qualifier1", "value1");
        RowMutationEntry rowMutationEntry2 = RowMutationEntry.create("rowKey2").setCell("family2", "qualifier2", "value2");
        BigTableRecord bigTableRecord1 = new BigTableRecord(rowMutationEntry1, 0, null, message1.getMetadata());
        BigTableRecord bigTableRecord2 = new BigTableRecord(rowMutationEntry2, 1, null, message2.getMetadata());
        validRecords = Collections.list(bigTableRecord1, bigTableRecord2);
    }

    /**
     * Verifies that a retryable failure maps to a retryable sink error.
     *
     * <p>Given a single failed mutation at index {@code 1} with HTTP status {@code 400} and the API
     * exception marked retryable, when the response is parsed, then the entry for index {@code 1} has
     * error type {@link ErrorType#SINK_RETRYABLE_ERROR} and retains the originating exception.</p>
     */
    @Test
    public void shouldReturnErrorInfoMapWithRetryableError() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(400);
        Mockito.when(apiException.isRetryable()).thenReturn(Boolean.TRUE);

        Map<Long, ErrorInfo> errorsFromSinkResponse = BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Assertions.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, errorsFromSinkResponse.get(1L).getErrorType());
        Assertions.assertEquals(apiException, errorsFromSinkResponse.get(1L).getException());
    }

    /**
     * Verifies that a non-retryable 4xx failure maps to a 4xx sink error.
     *
     * <p>Given a failed mutation with HTTP status {@code 400} and a non-retryable exception, when the
     * response is parsed, then the entry has error type {@link ErrorType#SINK_4XX_ERROR} and retains
     * the originating exception.</p>
     */
    @Test
    public void shouldReturnErrorInfoMapWith4XXError() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(400);

        Map<Long, ErrorInfo> errorsFromSinkResponse = BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Assertions.assertEquals(ErrorType.SINK_4XX_ERROR, errorsFromSinkResponse.get(1L).getErrorType());
        Assertions.assertEquals(apiException, errorsFromSinkResponse.get(1L).getException());
    }

    /**
     * Verifies that a 5xx failure maps to a 5xx sink error.
     *
     * <p>Given a failed mutation with HTTP status {@code 500}, when the response is parsed, then the
     * entry has error type {@link ErrorType#SINK_5XX_ERROR} and retains the originating exception.</p>
     */
    @Test
    public void shouldReturnErrorInfoMapWith5XXError() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(500);

        Map<Long, ErrorInfo> errorsFromSinkResponse = BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Assertions.assertEquals(ErrorType.SINK_5XX_ERROR, errorsFromSinkResponse.get(1L).getErrorType());
        Assertions.assertEquals(apiException, errorsFromSinkResponse.get(1L).getException());
    }

    /**
     * Verifies that a failure with no recognised HTTP status maps to an unknown sink error.
     *
     * <p>Given a failed mutation with HTTP status {@code 0}, when the response is parsed, then the
     * entry has error type {@link ErrorType#SINK_UNKNOWN_ERROR} and retains the originating
     * exception.</p>
     */
    @Test
    public void shouldReturnErrorInfoMapWithUnknownError() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);

        Map<Long, ErrorInfo> errorsFromSinkResponse = BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Assertions.assertEquals(ErrorType.SINK_UNKNOWN_ERROR, errorsFromSinkResponse.get(1L).getErrorType());
        Assertions.assertEquals(apiException, errorsFromSinkResponse.get(1L).getException());
    }

    /**
     * Verifies that a bad-request error detail increments the bad-request error counter.
     *
     * <p>Given error details exposing a {@code BadRequest}, when the response is parsed, then the
     * instrumentation increments the total-errors counter once with the
     * {@code BigTableErrorType.BAD_REQUEST} tag.</p>
     */
    @Test
    public void shouldCaptureMetricBigtableErrorTypeBadRequest() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);
        Mockito.when(errorDetails.getBadRequest()).thenReturn(BadRequest.getDefaultInstance());

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.BAD_REQUEST));
    }

    /**
     * Verifies that a quota-failure error detail increments the quota-failure error counter.
     *
     * <p>Given error details exposing a {@code QuotaFailure}, when the response is parsed, then the
     * instrumentation increments the total-errors counter once with the
     * {@code BigTableErrorType.QUOTA_FAILURE} tag.</p>
     */
    @Test
    public void shouldCaptureMetricBigtableErrorTypeQuotaFailure() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);
        Mockito.when(errorDetails.getQuotaFailure()).thenReturn(QuotaFailure.getDefaultInstance());

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.QUOTA_FAILURE));
    }

    /**
     * Verifies that a precondition-failure error detail increments the precondition-failure counter.
     *
     * <p>Given error details exposing a {@code PreconditionFailure}, when the response is parsed, then
     * the instrumentation increments the total-errors counter once with the
     * {@code BigTableErrorType.PRECONDITION_FAILURE} tag.</p>
     */
    @Test
    public void shouldCaptureMetricBigtableErrorTypePreconditionFailure() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);
        Mockito.when(errorDetails.getPreconditionFailure()).thenReturn(PreconditionFailure.getDefaultInstance());

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.PRECONDITION_FAILURE));
    }

    /**
     * Verifies that error details with no specific failure default to the RPC-failure counter.
     *
     * <p>Given error details exposing none of the known failure kinds, when the response is parsed,
     * then the instrumentation increments the total-errors counter once with the
     * {@code BigTableErrorType.RPC_FAILURE} tag.</p>
     */
    @Test
    public void shouldCaptureMetricBigtableErrorTypeRpcFailureByDefault() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.RPC_FAILURE));
    }

    /**
     * Verifies that null error details fall back to the RPC-failure counter.
     *
     * <p>Given an API exception whose error details are {@code null}, when the response is parsed,
     * then the instrumentation increments the total-errors counter once with the
     * {@code BigTableErrorType.RPC_FAILURE} tag.</p>
     */
    @Test
    public void shouldCaptureMetricBigtableErrorTypeRpcFailureIfErrorDetailsIsNull() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(apiException.getErrorDetails()).thenReturn(null);
        Mockito.when(code.getHttpStatusCode()).thenReturn(0);

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.RPC_FAILURE));
    }

    /**
     * Verifies that each failed record is logged with its metadata, cause, reason and status codes.
     *
     * <p>Given a failed mutation at index {@code 1}, when the response is parsed, then the
     * {@link Instrumentation} logs the error once with the failing record's metadata, the failure
     * cause, the reason, the status code and the HTTP status code.</p>
     */
    @Test
    public void shouldLogErrorRecordWithReasonAndStatusCode() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);
        BigTableResponse bigtableResponse = new BigTableResponse(mutateRowsException);

        Mockito.when(code.getHttpStatusCode()).thenReturn(0);
        Mockito.when(errorDetails.getPreconditionFailure()).thenReturn(PreconditionFailure.getDefaultInstance());

        BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigtableResponse, bigtableMetrics, instrumentation);

        Mockito.verify(instrumentation, Mockito.times(1)).logError("Error while inserting to Bigtable. Record Metadata: {}, Cause: {}, Reason: {}, StatusCode: {}, HttpCode: {}",
                validRecords.get(1).getMetadata(),
                failedMutations.get(0).getError().getCause(),
                failedMutations.get(0).getError().getReason(),
                failedMutations.get(0).getError().getStatusCode().getCode(),
                failedMutations.get(0).getError().getStatusCode().getCode().getHttpStatusCode());
    }
}
