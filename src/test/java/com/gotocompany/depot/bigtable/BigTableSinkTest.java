package com.gotocompany.depot.bigtable;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.gotocompany.depot.bigtable.client.BigTableClient;
import com.gotocompany.depot.bigtable.model.BigTableRecord;
import com.gotocompany.depot.bigtable.parser.BigTableRecordParser;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.TestBookingLogKey;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestServiceType;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.metrics.BigTableMetrics;
import org.aeonbits.owner.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;

/**
 * Unit tests for {@link BigTableSink}, the sink that converts messages into Bigtable mutations and
 * sends them through a {@link BigTableClient}.
 *
 * <p>The collaborators ({@link BigTableRecordParser}, {@link BigTableClient} and
 * {@link BigTableMetrics}) are Mockito mocks, while a real {@link Instrumentation} wraps a mocked
 * {@link StatsDReporter}. The {@link #setUp()} fixture builds two booking-log {@link Message}
 * instances together with the matching lists of valid and invalid {@link BigTableRecord}s, so the
 * parser can be stubbed to return either set. Tests exercise
 * {@link BigTableSink#pushToSink(java.util.List)} and assert both the returned {@link SinkResponse}
 * and the interactions with the client.</p>
 */
public class BigTableSinkTest {

    /** Mock parser stubbed to convert the input batch into valid or invalid records. */
    @Mock
    private BigTableRecordParser bigTableRecordParser;
    /** Mock Bigtable client whose send behaviour and invocation count are verified. */
    @Mock
    private BigTableClient bigTableClient;
    /** Mock StatsD reporter wrapped by the real {@link Instrumentation} given to the sink. */
    @Mock
    private StatsDReporter statsDReporter;
    /** Mock Bigtable metrics passed to the sink under test. */
    @Mock
    private BigTableMetrics bigtableMetrics;

    /** Sink under test, constructed from the mocked collaborators. */
    private BigTableSink bigTableSink;
    /** Two booking-log messages forming the input batch. */
    private List<Message> messages;
    /** Records reporting no error, used to stub a successful conversion. */
    private List<BigTableRecord> validRecords;
    /** Records carrying {@link #errorInfo}, used to stub a failed conversion. */
    private List<BigTableRecord> invalidRecords;
    /** Shared error attached to the invalid records and expected in the response. */
    private ErrorInfo errorInfo;

    /**
     * Builds the message batch, record fixtures and the sink under test before each test.
     *
     * <p>Initialises the {@code @Mock} collaborators, constructs two booking-log {@link Message}s and
     * derives two valid {@link BigTableRecord}s (with no error) plus two invalid ones (carrying
     * {@link #errorInfo}). The {@link BigTableSink} is created with the mocked client, parser and
     * metrics and a real {@link Instrumentation} over the mocked {@link StatsDReporter}.</p>
     */
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        TestBookingLogKey bookingLogKey1 = TestBookingLogKey.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").build();
        TestBookingLogMessage bookingLogMessage1 = TestBookingLogMessage.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").setServiceType(TestServiceType.Enum.GO_SEND).build();
        TestBookingLogKey bookingLogKey2 = TestBookingLogKey.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2").build();
        TestBookingLogMessage bookingLogMessage2 = TestBookingLogMessage.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2").setServiceType(TestServiceType.Enum.GO_SHOP).build();

        Message message1 = new Message(bookingLogKey1.toByteArray(), bookingLogMessage1.toByteArray());
        Message message2 = new Message(bookingLogKey2.toByteArray(), bookingLogMessage2.toByteArray());
        messages = Collections.list(message1, message2);

        RowMutationEntry rowMutationEntry = RowMutationEntry.create("rowKey").setCell("family", "qualifier", "value");
        BigTableRecord bigTableRecord1 = new BigTableRecord(rowMutationEntry, 1, null, message1.getMetadata());
        BigTableRecord bigTableRecord2 = new BigTableRecord(rowMutationEntry, 2, null, message2.getMetadata());
        validRecords = Collections.list(bigTableRecord1, bigTableRecord2);

        errorInfo = new ErrorInfo(new Exception("test-exception-message"), ErrorType.DEFAULT_ERROR);
        BigTableRecord bigTableRecord3 = new BigTableRecord(null, 3, errorInfo, message1.getMetadata());
        BigTableRecord bigTableRecord4 = new BigTableRecord(null, 4, errorInfo, message2.getMetadata());
        invalidRecords = Collections.list(bigTableRecord3, bigTableRecord4);

        bigTableSink = new BigTableSink(bigTableClient, bigTableRecordParser, bigtableMetrics, new Instrumentation(statsDReporter, BigTableSink.class));
    }

    /**
     * Verifies that valid records are forwarded to the client and yield an error-free response.
     *
     * <p>Given the parser stubbed to convert the batch into {@link #validRecords} and the client
     * stubbed to return {@code null} (no failures), when the batch is pushed, then the client's
     * {@code send} is invoked exactly once with the valid records and the {@link SinkResponse} reports
     * no errors.</p>
     */
    @Test
    public void shouldSendValidBigTableRecordsToBigTableSink() {
        Mockito.when(bigTableRecordParser.convert(messages)).thenReturn(validRecords);
        Mockito.when(bigTableClient.send(validRecords)).thenReturn(null);

        SinkResponse response = bigTableSink.pushToSink(messages);

        Mockito.verify(bigTableClient, Mockito.times(1)).send(validRecords);
        Assert.assertEquals(0, response.getErrors().size());
    }

    /**
     * Verifies that invalid records are reported as errors and never sent to the client.
     *
     * <p>Given the parser stubbed to convert the batch into {@link #invalidRecords}, when the batch is
     * pushed, then the client is never asked to send, and the {@link SinkResponse} reports errors for
     * the two failing entries (indices {@code 3} and {@code 4}), each carrying the shared
     * {@link #errorInfo}.</p>
     */
    @Test
    public void shouldAddErrorsFromInvalidRecordsToResponse() {
        Mockito.when(bigTableRecordParser.convert(messages)).thenReturn(invalidRecords);

        SinkResponse response = bigTableSink.pushToSink(messages);

        Mockito.verify(bigTableClient, Mockito.times(0)).send(validRecords);
        Assert.assertTrue(response.hasErrors());
        Assert.assertEquals(2, response.getErrors().size());
        Assert.assertEquals(errorInfo, response.getErrorsFor(3));
        Assert.assertEquals(errorInfo, response.getErrorsFor(4));
    }
}
