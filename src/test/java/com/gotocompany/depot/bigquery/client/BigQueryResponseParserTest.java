package com.gotocompany.depot.bigquery.client;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;
import com.gotocompany.depot.bigquery.TestMetadata;
import com.gotocompany.depot.bigquery.TestMessageBuilder;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.bigquery.exception.BigQuerySinkException;
import com.gotocompany.depot.bigquery.models.Record;
import org.aeonbits.owner.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link BigQueryResponseParser}, which maps a BigQuery {@link InsertAllResponse}'s
 * per-row insert errors back to {@link ErrorInfo} entries keyed by record index.
 *
 * <p>The test builds a batch of {@link Record}s, stubs the {@link InsertAllResponse} to report a mix
 * of unknown, invalid-schema, out-of-bounds and stopped errors, and asserts both the resulting
 * {@link ErrorType} classification per row and that the matching BigQuery error metrics are
 * incremented on the {@link Instrumentation}.</p>
 */
public class BigQueryResponseParserTest {

    /** Mocked BigQuery insert-all response whose insert errors drive the parser. */
    @Mock
    private InsertAllResponse response;

    /** Mocked instrumentation used to verify that error metrics are emitted. */
    @Mock
    private Instrumentation instrumentation;

    /** Mocked metrics provider supplying the BigQuery error metric names. */
    @Mock
    private BigQueryMetrics metrics;

    /**
     * Initializes the Mockito-annotated mocks before each test.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Verifies that BigQuery insert errors are classified and counted per record.
     *
     * <p>Given six records and an {@link InsertAllResponse} reporting errors for the first four rows
     * (an empty reason, a {@code "no such field"} schema error, an out-of-bounds partition error and a
     * {@code "stopped"} error), when {@code getErrorsFromBQResponse} runs, then rows {@code 0}-{@code 3}
     * are mapped to {@link ErrorType#SINK_UNKNOWN_ERROR}, {@link ErrorType#SINK_4XX_ERROR},
     * {@link ErrorType#SINK_4XX_ERROR} and {@link ErrorType#SINK_5XX_ERROR} respectively, and the
     * unknown, invalid-schema, out-of-bounds and stopped error counters are each incremented
     * once.</p>
     */
    @Test
    public void shouldParseResponse() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record3Offset = new TestMetadata("topic1", 3, 103, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record4Offset = new TestMetadata("topic1", 4, 104, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record5Offset = new TestMetadata("topic1", 5, 104, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record6Offset = new TestMetadata("topic1", 6, 104, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        Record record1 = new Record(TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1").getMetadata(), new HashMap<>(), 0, null);
        Record record2 = new Record(TestMessageBuilder.withMetadata(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2").getMetadata(), new HashMap<>(), 1, null);
        Record record3 = new Record(TestMessageBuilder.withMetadata(record3Offset).createConsumerRecord("order-3", "order-url-3", "order-details-3").getMetadata(), new HashMap<>(), 2, null);
        Record record4 = new Record(TestMessageBuilder.withMetadata(record4Offset).createConsumerRecord("order-4", "order-url-4", "order-details-4").getMetadata(), new HashMap<>(), 3, null);
        Record record5 = new Record(TestMessageBuilder.withMetadata(record5Offset).createConsumerRecord("order-5", "order-url-5", "order-details-5").getMetadata(), new HashMap<>(), 4, null);
        Record record6 = new Record(TestMessageBuilder.withMetadata(record6Offset).createConsumerRecord("order-6", "order-url-6", "order-details-6").getMetadata(), new HashMap<>(), 5, null);
        List<Record> records = Collections.list(record1, record2, record3, record4, record5, record6);
        BigQueryError error1 = new BigQueryError("", "US", "");
        BigQueryError error2 = new BigQueryError("invalid", "US", "no such field");
        BigQueryError error3 = new BigQueryError("invalid", "", "The destination table's partition tmp$20160101 is outside the allowed bounds. You can only stream to partitions within 1825 days in the past and 366 days in the future relative to the current date");
        BigQueryError error4 = new BigQueryError("stopped", "", "");

        Map<Long, List<BigQueryError>> insertErrorsMap = new HashMap<Long, List<BigQueryError>>() {{
            put(0L, Collections.list(error1));
            put(1L, Collections.list(error2));
            put(2L, Collections.list(error3));
            put(3L, Collections.list(error4));
        }};
        Mockito.when(response.hasErrors()).thenReturn(true);
        Mockito.when(response.getInsertErrors()).thenReturn(insertErrorsMap);
        Mockito.when(metrics.getBigqueryTotalErrorsMetrics()).thenReturn("test");
        Map<Long, ErrorInfo> errorInfoMap = BigQueryResponseParser.getErrorsFromBQResponse(records, response, metrics, instrumentation);

        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_UNKNOWN_ERROR), errorInfoMap.get(0L));
        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_4XX_ERROR), errorInfoMap.get(1L));
        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_4XX_ERROR), errorInfoMap.get(2L));
        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_5XX_ERROR), errorInfoMap.get(3L));

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter("test", String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.UNKNOWN_ERROR));
        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter("test", String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.INVALID_SCHEMA_ERROR));
        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter("test", String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.OOB_ERROR));
        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter("test", String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.STOPPED_ERROR));
    }
}
