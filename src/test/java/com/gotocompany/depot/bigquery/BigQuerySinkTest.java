package com.gotocompany.depot.bigquery;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.gotocompany.depot.bigquery.client.BigQueryClient;
import com.gotocompany.depot.bigquery.client.BigQueryRow;
import com.gotocompany.depot.bigquery.client.BigQueryRowWithInsertId;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverter;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverterCache;
import com.gotocompany.depot.bigquery.handler.ErrorHandler;
import com.gotocompany.depot.bigquery.models.Record;
import com.gotocompany.depot.bigquery.models.Records;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.message.Message;
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
 * Unit tests for {@link BigQuerySink}, the legacy streaming-insert sink that converts messages,
 * inserts the valid rows through a {@link BigQueryClient} and aggregates the per-record errors into a
 * {@link SinkResponse}.
 *
 * <p>The sink is assembled from mocked collaborators (client, converter, metrics, instrumentation and
 * {@link ErrorHandler}) together with a real {@link MessageRecordConverterCache} and a
 * {@link BigQueryRowWithInsertId} row creator. Each test stubs the converter to return a fixed set of
 * valid and invalid {@link Record}s and the client to report insert successes or failures, then
 * asserts on the errors surfaced in the {@link SinkResponse} and on the interactions with the client
 * and error handler.</p>
 */
public class BigQuerySinkTest {

    /** Identifier of the destination table returned by the mocked client. */
    private final TableId tableId = TableId.of("test_dataset", "test_table");
    /** Real converter cache wired with the mocked {@link MessageRecordConverter}. */
    private final MessageRecordConverterCache converterCache = new MessageRecordConverterCache();
    /** Row creator that derives a deterministic insert id from each record's metadata. */
    private final BigQueryRow rowCreator = new BigQueryRowWithInsertId(
            metadata -> metadata.get("topic") + "_" + metadata.get("partition") + "_" + metadata.get("offset") + "_" + metadata.get("timestamp"));
    /** Mocked BigQuery client used to insert rows and resolve the table id. */
    @Mock
    private BigQueryClient client;
    /** Mocked instrumentation collaborator. */
    @Mock
    private Instrumentation instrumentation;
    /** Mocked converter stubbed to return predetermined valid and invalid records. */
    @Mock
    private MessageRecordConverter converter;
    /** Mocked metrics collaborator. */
    @Mock
    private BigQueryMetrics metrics;
    /** Sink under test, assembled in {@link #setup()}. */
    private BigQuerySink sink;
    /** Mocked BigQuery insert-all response stubbed per test. */
    @Mock
    private InsertAllResponse insertAllResponse;

    /** Mocked error handler whose invocation on insert failures is verified. */
    @Mock
    private ErrorHandler errorHandler;

    /**
     * Initializes the mocks, registers the converter in the cache and builds the sink under test.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.converterCache.setMessageRecordConverter(converter);
        this.sink = new BigQuerySink(client, converterCache, rowCreator, metrics, instrumentation, errorHandler);
        Mockito.when(client.getTableID()).thenReturn(tableId);
    }

    /**
     * Verifies the happy path where all records are inserted without errors.
     *
     * <p>Given six valid records converted from the input messages and a client whose insert reports
     * no errors, when {@code pushToSink} runs, then the returned {@link SinkResponse} has no errors and
     * the client's {@code insertAll} is invoked exactly once with the built rows.</p>
     */
    @Test
    public void shouldPushToBigQuerySink() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record3Offset = new TestMetadata("topic1", 3, 103, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record4Offset = new TestMetadata("topic1", 4, 104, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record5Offset = new TestMetadata("topic1", 5, 104, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record6Offset = new TestMetadata("topic1", 6, 104, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        Message message1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message message2 = TestMessageBuilder.withMetadata(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2");
        Message message3 = TestMessageBuilder.withMetadata(record3Offset).createConsumerRecord("order-3", "order-url-3", "order-details-3");
        Message message4 = TestMessageBuilder.withMetadata(record4Offset).createConsumerRecord("order-4", "order-url-4", "order-details-4");
        Message message5 = TestMessageBuilder.withMetadata(record5Offset).createConsumerRecord("order-5", "order-url-5", "order-details-5");
        Message message6 = TestMessageBuilder.withMetadata(record6Offset).createConsumerRecord("order-6", "order-url-6", "order-details-6");
        List<Message> messages = Collections.list(message1, message2, message3, message4, message5, message6);
        Record record1 = new Record(message1.getMetadata(), new HashMap<>(), 0, null);
        Record record2 = new Record(message2.getMetadata(), new HashMap<>(), 1, null);
        Record record3 = new Record(message3.getMetadata(), new HashMap<>(), 2, null);
        Record record4 = new Record(message4.getMetadata(), new HashMap<>(), 3, null);
        Record record5 = new Record(message5.getMetadata(), new HashMap<>(), 4, null);
        Record record6 = new Record(message6.getMetadata(), new HashMap<>(), 5, null);
        Records records = new Records(Collections.list(record1, record2, record3, record4, record5, record6), java.util.Collections.emptyList());

        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(client.getTableID());
        records.getValidRecords().forEach((Record m) -> builder.addRow(rowCreator.of(m)));
        InsertAllRequest rows = builder.build();
        Mockito.when(converter.convert(Mockito.eq(messages))).thenReturn(records);
        Mockito.when(client.insertAll(rows)).thenReturn(insertAllResponse);
        Mockito.when(insertAllResponse.hasErrors()).thenReturn(false);
        SinkResponse response = sink.pushToSink(messages);
        Assert.assertEquals(0, response.getErrors().size());
        Mockito.verify(client, Mockito.times(1)).insertAll(rows);
    }

    /**
     * Verifies that conversion-stage invalid records are surfaced as errors.
     *
     * <p>Given four valid and two invalid records (a default error at input index {@code 1} and an
     * invalid-message error at index {@code 3}) with a client reporting no insert errors, when
     * {@code pushToSink} runs, then the {@link SinkResponse} contains the two errors keyed by their
     * original indexes with the expected {@link ErrorType}s, and {@code insertAll} is invoked
     * once.</p>
     */
    @Test
    public void shouldReturnInvalidMessages() throws Exception {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record3Offset = new TestMetadata("topic1", 3, 103, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record4Offset = new TestMetadata("topic1", 4, 104, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record5Offset = new TestMetadata("topic1", 5, 104, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record6Offset = new TestMetadata("topic1", 6, 104, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        Message message1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message message2 = TestMessageBuilder.withMetadata(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2");
        Message message3 = TestMessageBuilder.withMetadata(record3Offset).createConsumerRecord("order-3", "order-url-3", "order-details-3");
        Message message4 = TestMessageBuilder.withMetadata(record4Offset).createConsumerRecord("order-4", "order-url-4", "order-details-4");
        Message message5 = TestMessageBuilder.withMetadata(record5Offset).createConsumerRecord("order-5", "order-url-5", "order-details-5");
        Message message6 = TestMessageBuilder.withMetadata(record6Offset).createConsumerRecord("order-6", "order-url-6", "order-details-6");
        List<Message> messages = Collections.list(message1, message2, message3, message4, message5, message6);
        Record record1 = new Record(message1.getMetadata(), new HashMap<>(), 0, null);
        Record record2 = new Record(message2.getMetadata(), new HashMap<>(), 1, new ErrorInfo(new RuntimeException(), ErrorType.DEFAULT_ERROR));
        Record record3 = new Record(message3.getMetadata(), new HashMap<>(), 2, null);
        Record record4 = new Record(message4.getMetadata(), new HashMap<>(), 3, new ErrorInfo(new RuntimeException(), ErrorType.INVALID_MESSAGE_ERROR));
        Record record5 = new Record(message5.getMetadata(), new HashMap<>(), 4, null);
        Record record6 = new Record(message6.getMetadata(), new HashMap<>(), 5, null);
        Records records = new Records(Collections.list(record1, record3, record5, record6), Collections.list(record2, record4));

        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(client.getTableID());
        records.getValidRecords().forEach((Record m) -> builder.addRow(rowCreator.of(m)));
        InsertAllRequest rows = builder.build();
        Mockito.when(converter.convert(Mockito.eq(messages))).thenReturn(records);
        Mockito.when(client.insertAll(rows)).thenReturn(insertAllResponse);
        Mockito.when(insertAllResponse.hasErrors()).thenReturn(false);
        SinkResponse response = sink.pushToSink(messages);
        Assert.assertEquals(2, response.getErrors().size());
        Mockito.verify(client, Mockito.times(1)).insertAll(rows);

        Assert.assertEquals(ErrorType.DEFAULT_ERROR, response.getErrors().get(1L).getErrorType());
        Assert.assertEquals(ErrorType.INVALID_MESSAGE_ERROR, response.getErrors().get(3L).getErrorType());
    }

    /**
     * Verifies that conversion errors and insert-time errors are aggregated together.
     *
     * <p>Given two conversion-stage invalid records and a client whose insert reports errors for valid
     * rows {@code 0} (unknown) and {@code 2} (out-of-bounds), when {@code pushToSink} runs, then the
     * {@link ErrorHandler} is invoked once with the insert-error map and the {@link SinkResponse}
     * contains four errors: the unknown and 4xx insert errors plus the default and invalid-message
     * conversion errors, each keyed by its original index.</p>
     */
    @Test
    public void shouldReturnInvalidMessagesWithFailedInsertMessages() throws Exception {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record3Offset = new TestMetadata("topic1", 3, 103, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record4Offset = new TestMetadata("topic1", 4, 104, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record5Offset = new TestMetadata("topic1", 5, 104, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        TestMetadata record6Offset = new TestMetadata("topic1", 6, 104, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        Message message1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message message2 = TestMessageBuilder.withMetadata(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2");
        Message message3 = TestMessageBuilder.withMetadata(record3Offset).createConsumerRecord("order-3", "order-url-3", "order-details-3");
        Message message4 = TestMessageBuilder.withMetadata(record4Offset).createConsumerRecord("order-4", "order-url-4", "order-details-4");
        Message message5 = TestMessageBuilder.withMetadata(record5Offset).createConsumerRecord("order-5", "order-url-5", "order-details-5");
        Message message6 = TestMessageBuilder.withMetadata(record6Offset).createConsumerRecord("order-6", "order-url-6", "order-details-6");
        List<Message> messages = Collections.list(message1, message2, message3, message4, message5, message6);
        Record record1 = new Record(message1.getMetadata(), new HashMap<>(), 0, null);
        Record record2 = new Record(message2.getMetadata(), new HashMap<>(), 1, new ErrorInfo(new RuntimeException(), ErrorType.DEFAULT_ERROR));
        Record record3 = new Record(message3.getMetadata(), new HashMap<>(), 2, null);
        Record record4 = new Record(message4.getMetadata(), new HashMap<>(), 3, new ErrorInfo(new RuntimeException(), ErrorType.INVALID_MESSAGE_ERROR));
        Record record5 = new Record(message5.getMetadata(), new HashMap<>(), 4, null);
        Record record6 = new Record(message6.getMetadata(), new HashMap<>(), 5, null);
        Records records = new Records(Collections.list(record1, record3, record5, record6), Collections.list(record2, record4));

        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(client.getTableID());
        records.getValidRecords().forEach((Record m) -> builder.addRow(rowCreator.of(m)));
        InsertAllRequest rows = builder.build();
        Mockito.when(converter.convert(Mockito.eq(messages))).thenReturn(records);
        Mockito.when(client.insertAll(rows)).thenReturn(insertAllResponse);
        Mockito.when(insertAllResponse.hasErrors()).thenReturn(true);

        BigQueryError error1 = new BigQueryError("", "US", "");
        BigQueryError error3 = new BigQueryError("invalid", "", "The destination table's partition tmp$20160101 is outside the allowed bounds. You can only stream to partitions within 1825 days in the past and 366 days in the future relative to the current date");

        Map<Long, List<BigQueryError>> insertErrorsMap = new HashMap<Long, List<BigQueryError>>() {{
            put(0L, Collections.list(error1));
            put(2L, Collections.list(error3));
        }};
        Mockito.when(insertAllResponse.getInsertErrors()).thenReturn(insertErrorsMap);

        SinkResponse response = sink.pushToSink(messages);
        Mockito.verify(client, Mockito.times(1)).insertAll(rows);
        Mockito.verify(errorHandler, Mockito.times(1)).handle(Mockito.eq(insertErrorsMap), Mockito.any());
        Assert.assertEquals(4, response.getErrors().size());

        Assert.assertEquals(ErrorType.SINK_UNKNOWN_ERROR, response.getErrors().get(0L).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, response.getErrors().get(1L).getErrorType());
        Assert.assertEquals(ErrorType.INVALID_MESSAGE_ERROR, response.getErrors().get(3L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(4L).getErrorType());
    }
}
