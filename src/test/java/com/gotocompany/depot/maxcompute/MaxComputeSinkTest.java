package com.gotocompany.depot.maxcompute;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.NonRetryableException;
import com.gotocompany.depot.maxcompute.client.insert.InsertManager;
import com.gotocompany.depot.maxcompute.converter.record.MessageRecordConverter;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MaxComputeSink}.
 *
 * <p>These tests verify how the sink converts a batch of {@link Message} objects and streams the valid records
 * into MaxCompute, and how it classifies failures into the {@link SinkResponse} error map. The
 * {@link MessageRecordConverter} is mocked to return a fixed split of valid and invalid {@link RecordWrapper}
 * instances, and the {@link InsertManager} is mocked to either succeed or throw a specific exception, allowing
 * each error-classification branch to be exercised. The metrics collaborators ({@link StatsDReporter} and
 * {@link MaxComputeMetrics}) are passed as mocks.</p>
 *
 * <p>The scenarios cover a successful insert that preserves conversion errors, and insertion failures raised as
 * a {@link TunnelException}, an {@link IOException}, a {@link NonRetryableException}, and a generic
 * {@link RuntimeException}, each mapping to the corresponding {@link ErrorType}; a final test confirms
 * {@link MaxComputeSink#close()} is a no-op.</p>
 *
 * @see MaxComputeSink
 */
public class MaxComputeSinkTest {

    /**
     * Verifies that valid records are inserted while conversion failures are reported as errors.
     *
     * <p>Given a converter that returns one valid record and one invalid record (a
     * {@link ErrorType#DESERIALIZATION_ERROR} at index {@code 1}) and an {@link InsertManager} that inserts
     * successfully, when {@link MaxComputeSink#pushToSink(List)} is called, then the insert manager is invoked
     * exactly once with the valid records and the response contains a single error entry at index {@code 1}
     * carrying the {@code "Invalid Schema"} message and the deserialization error type.</p>
     *
     * @throws IOException     never in this test; declared because the insert path may throw it
     * @throws TunnelException never in this test; declared because the insert path may throw it
     */
    @Test
    public void shouldInsertMaxComputeSinkTest() throws IOException, TunnelException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeAccessId())
                .thenReturn("accessId");
        when(maxComputeSinkConfig.getMaxComputeAccessKey())
                .thenReturn("accessKey");
        when(maxComputeSinkConfig.getMaxComputeOdpsUrl())
                .thenReturn("odpsUrl");
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("projectId");
        when(maxComputeSinkConfig.getMaxComputeSchema())
                .thenReturn("schema");
        when(maxComputeSinkConfig.getMaxComputeTunnelUrl())
                .thenReturn("tunnelUrl");
        when(maxComputeSinkConfig.getTableValidatorNameRegex())
                .thenReturn("^[A-Za-z][A-Za-z0-9_]{0,127}$");
        when(maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable())
                .thenReturn(1200);
        when(maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable())
                .thenReturn(1);
        InsertManager insertManager = Mockito.mock(InsertManager.class);
        Mockito.doNothing()
                .when(insertManager)
                .insert(Mockito.anyList());
        MessageRecordConverter messageRecordConverter = Mockito.mock(MessageRecordConverter.class);
        MaxComputeSink maxComputeSink = new MaxComputeSink(insertManager, messageRecordConverter, Mockito.mock(StatsDReporter.class), Mockito.mock(MaxComputeMetrics.class));
        List<Message> messages = Arrays.asList(
                new Message("key1".getBytes(StandardCharsets.UTF_8), "message1".getBytes(StandardCharsets.UTF_8)),
                new Message("key2".getBytes(StandardCharsets.UTF_8), "invalidMessage2".getBytes(StandardCharsets.UTF_8))
        );
        List<RecordWrapper> validRecords = Collections.singletonList(new RecordWrapper(Mockito.mock(Record.class), 0, null, null));
        List<RecordWrapper> invalidRecords = Collections.singletonList(new RecordWrapper(Mockito.mock(Record.class), 1,
                new ErrorInfo(new RuntimeException("Invalid Schema"), ErrorType.DESERIALIZATION_ERROR), null));
        when(messageRecordConverter.convert(messages)).thenReturn(new RecordWrappers(validRecords, invalidRecords));

        SinkResponse sinkResponse = maxComputeSink.pushToSink(messages);

        Mockito.verify(insertManager, Mockito.times(1)).insert(validRecords);
        Assertions.assertEquals(1, sinkResponse.getErrors().size());
        Assertions.assertEquals(sinkResponse.getErrors().get(1L).getException().getMessage(), "Invalid Schema");
        Assertions.assertEquals(sinkResponse.getErrors().get(1L).getErrorType(), ErrorType.DESERIALIZATION_ERROR);
    }

    /**
     * Verifies that a {@link TunnelException} during insert marks every valid record as retryable.
     *
     * <p>Given two valid records and an {@link InsertManager} stubbed to throw a {@link TunnelException}, when
     * {@link MaxComputeSink#pushToSink(List)} is called, then the response contains two error entries and every
     * error is of type {@link ErrorType#SINK_RETRYABLE_ERROR}.</p>
     *
     * @throws IOException     never in this test; declared because the insert path may throw it
     * @throws TunnelException never in this test; declared because the insert path may throw it
     */
    @Test
    public void shouldMarkAllMessageAsFailedWhenInsertThrowTunnelExceptionError() throws IOException, TunnelException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeAccessId())
                .thenReturn("accessId");
        when(maxComputeSinkConfig.getMaxComputeAccessKey())
                .thenReturn("accessKey");
        when(maxComputeSinkConfig.getMaxComputeOdpsUrl())
                .thenReturn("odpsUrl");
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("projectId");
        when(maxComputeSinkConfig.getMaxComputeSchema())
                .thenReturn("schema");
        when(maxComputeSinkConfig.getMaxComputeTunnelUrl())
                .thenReturn("tunnelUrl");
        when(maxComputeSinkConfig.getTableValidatorNameRegex())
                .thenReturn("^[A-Za-z][A-Za-z0-9_]{0,127}$");
        when(maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable())
                .thenReturn(1200);
        when(maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable())
                .thenReturn(1);
        InsertManager insertManager = Mockito.mock(InsertManager.class);
        Mockito.doThrow(new TunnelException("Failed establishing connection"))
                .when(insertManager)
                .insert(Mockito.anyList());
        MessageRecordConverter messageRecordConverter = Mockito.mock(MessageRecordConverter.class);
        MaxComputeSink maxComputeSink = new MaxComputeSink(insertManager, messageRecordConverter, Mockito.mock(StatsDReporter.class), Mockito.mock(MaxComputeMetrics.class));
        List<Message> messages = Arrays.asList(
                new Message("key1".getBytes(StandardCharsets.UTF_8), "message1".getBytes(StandardCharsets.UTF_8)),
                new Message("key2".getBytes(StandardCharsets.UTF_8), "invalidMessage2".getBytes(StandardCharsets.UTF_8))
        );
        List<RecordWrapper> validRecords = Arrays.asList(
                new RecordWrapper(Mockito.mock(Record.class), 0, null, null),
                new RecordWrapper(Mockito.mock(Record.class), 1, null, null)
        );
        when(messageRecordConverter.convert(messages)).thenReturn(new RecordWrappers(validRecords, new ArrayList<>()));

        SinkResponse sinkResponse = maxComputeSink.pushToSink(messages);

        Assertions.assertEquals(2, sinkResponse.getErrors().size());
        Assert.assertTrue(sinkResponse.getErrors()
                .values()
                .stream()
                .allMatch(s -> ErrorType.SINK_RETRYABLE_ERROR.equals(s.getErrorType())));
    }

    /**
     * Verifies that an {@link IOException} during insert marks every valid record as retryable.
     *
     * <p>Given two valid records and an {@link InsertManager} stubbed to throw an {@link IOException}, when
     * {@link MaxComputeSink#pushToSink(List)} is called, then the response contains two error entries and every
     * error is of type {@link ErrorType#SINK_RETRYABLE_ERROR}.</p>
     *
     * @throws IOException     never in this test; declared because the insert path may throw it
     * @throws TunnelException never in this test; declared because the insert path may throw it
     */
    @Test
    public void shouldMarkAllMessageAsFailedWhenInsertThrowIOExceptionError() throws IOException, TunnelException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeAccessId())
                .thenReturn("accessId");
        when(maxComputeSinkConfig.getMaxComputeAccessKey())
                .thenReturn("accessKey");
        when(maxComputeSinkConfig.getMaxComputeOdpsUrl())
                .thenReturn("odpsUrl");
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("projectId");
        when(maxComputeSinkConfig.getMaxComputeSchema())
                .thenReturn("schema");
        when(maxComputeSinkConfig.getMaxComputeTunnelUrl())
                .thenReturn("tunnelUrl");
        when(maxComputeSinkConfig.getTableValidatorNameRegex())
                .thenReturn("^[A-Za-z][A-Za-z0-9_]{0,127}$");
        when(maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable())
                .thenReturn(1200);
        when(maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable())
                .thenReturn(1);
        MessageRecordConverter messageRecordConverter = Mockito.mock(MessageRecordConverter.class);
        InsertManager insertManager = Mockito.mock(InsertManager.class);
        Mockito.doThrow(new IOException("Failed flushing"))
                .when(insertManager)
                .insert(Mockito.anyList());
        MaxComputeSink maxComputeSink = new MaxComputeSink(insertManager, messageRecordConverter, Mockito.mock(StatsDReporter.class), Mockito.mock(MaxComputeMetrics.class));
        List<Message> messages = Arrays.asList(
                new Message("key1".getBytes(StandardCharsets.UTF_8), "message1".getBytes(StandardCharsets.UTF_8)),
                new Message("key2".getBytes(StandardCharsets.UTF_8), "invalidMessage2".getBytes(StandardCharsets.UTF_8))
        );
        List<RecordWrapper> validRecords = Arrays.asList(
                new RecordWrapper(Mockito.mock(Record.class), 0, null, null),
                new RecordWrapper(Mockito.mock(Record.class), 1, null, null)
        );
        when(messageRecordConverter.convert(messages)).thenReturn(new RecordWrappers(validRecords, new ArrayList<>()));

        SinkResponse sinkResponse = maxComputeSink.pushToSink(messages);

        Assertions.assertEquals(2, sinkResponse.getErrors().size());
        Assert.assertTrue(sinkResponse.getErrors()
                .values()
                .stream()
                .allMatch(s -> ErrorType.SINK_RETRYABLE_ERROR.equals(s.getErrorType())));
    }

    /**
     * Verifies that a {@link NonRetryableException} during insert marks every valid record as non-retryable.
     *
     * <p>Given two valid records and an {@link InsertManager} stubbed to throw a {@link NonRetryableException},
     * when {@link MaxComputeSink#pushToSink(List)} is called, then the response contains two error entries and
     * every error is of type {@link ErrorType#SINK_NON_RETRYABLE_ERROR}.</p>
     *
     * @throws IOException     never in this test; declared because the insert path may throw it
     * @throws TunnelException never in this test; declared because the insert path may throw it
     */
    @Test
    public void shouldMarkAllMessageAsFailedWithNonRetryableErrorWhenInsertThrowNonRetryableException() throws IOException, TunnelException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeAccessId())
                .thenReturn("accessId");
        when(maxComputeSinkConfig.getMaxComputeAccessKey())
                .thenReturn("accessKey");
        when(maxComputeSinkConfig.getMaxComputeOdpsUrl())
                .thenReturn("odpsUrl");
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("projectId");
        when(maxComputeSinkConfig.getMaxComputeSchema())
                .thenReturn("schema");
        when(maxComputeSinkConfig.getMaxComputeTunnelUrl())
                .thenReturn("tunnelUrl");
        when(maxComputeSinkConfig.getTableValidatorNameRegex())
                .thenReturn("^[A-Za-z][A-Za-z0-9_]{0,127}$");
        when(maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable())
                .thenReturn(1200);
        when(maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable())
                .thenReturn(1);
        MessageRecordConverter messageRecordConverter = Mockito.mock(MessageRecordConverter.class);
        InsertManager insertManager = Mockito.mock(InsertManager.class);
        Mockito.doThrow(NonRetryableException.class)
                .when(insertManager)
                .insert(Mockito.anyList());
        MaxComputeSink maxComputeSink = new MaxComputeSink(insertManager, messageRecordConverter, Mockito.mock(StatsDReporter.class), Mockito.mock(MaxComputeMetrics.class));
        List<Message> messages = Arrays.asList(
                new Message("key1".getBytes(StandardCharsets.UTF_8), "message1".getBytes(StandardCharsets.UTF_8)),
                new Message("key2".getBytes(StandardCharsets.UTF_8), "invalidMessage2".getBytes(StandardCharsets.UTF_8))
        );
        List<RecordWrapper> validRecords = Arrays.asList(
                new RecordWrapper(Mockito.mock(Record.class), 0, null, null),
                new RecordWrapper(Mockito.mock(Record.class), 1, null, null)
        );
        when(messageRecordConverter.convert(messages)).thenReturn(new RecordWrappers(validRecords, new ArrayList<>()));

        SinkResponse sinkResponse = maxComputeSink.pushToSink(messages);

        Assertions.assertEquals(2, sinkResponse.getErrors().size());
        Assert.assertTrue(sinkResponse.getErrors()
                .values()
                .stream()
                .allMatch(s -> ErrorType.SINK_NON_RETRYABLE_ERROR.equals(s.getErrorType())));
    }

    /**
     * Verifies that an unexpected exception during insert marks every valid record with the default error type.
     *
     * <p>Given two valid records and an {@link InsertManager} stubbed to throw a generic
     * {@link RuntimeException}, when {@link MaxComputeSink#pushToSink(List)} is called, then the response
     * contains two error entries and every error is of type {@link ErrorType#DEFAULT_ERROR}.</p>
     *
     * @throws IOException     never in this test; declared because the insert path may throw it
     * @throws TunnelException never in this test; declared because the insert path may throw it
     */
    @Test
    public void shouldMarkAllMessageAsFailedWhenInsertThrowExceptionError() throws IOException, TunnelException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeAccessId())
                .thenReturn("accessId");
        when(maxComputeSinkConfig.getMaxComputeAccessKey())
                .thenReturn("accessKey");
        when(maxComputeSinkConfig.getMaxComputeOdpsUrl())
                .thenReturn("odpsUrl");
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("projectId");
        when(maxComputeSinkConfig.getMaxComputeSchema())
                .thenReturn("schema");
        when(maxComputeSinkConfig.getMaxComputeTunnelUrl())
                .thenReturn("tunnelUrl");
        when(maxComputeSinkConfig.getTableValidatorNameRegex())
                .thenReturn("^[A-Za-z][A-Za-z0-9_]{0,127}$");
        when(maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable())
                .thenReturn(1200);
        when(maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable())
                .thenReturn(1);
        MessageRecordConverter messageRecordConverter = Mockito.mock(MessageRecordConverter.class);
        InsertManager insertManager = Mockito.mock(InsertManager.class);
        Mockito.doThrow(new RuntimeException("Unexpected Error"))
                .when(insertManager)
                .insert(Mockito.anyList());
        MaxComputeSink maxComputeSink = new MaxComputeSink(insertManager, messageRecordConverter, Mockito.mock(StatsDReporter.class), Mockito.mock(MaxComputeMetrics.class));
        List<Message> messages = Arrays.asList(
                new Message("key1".getBytes(StandardCharsets.UTF_8), "message1".getBytes(StandardCharsets.UTF_8)),
                new Message("key2".getBytes(StandardCharsets.UTF_8), "invalidMessage2".getBytes(StandardCharsets.UTF_8))
        );
        List<RecordWrapper> validRecords = Arrays.asList(
                new RecordWrapper(Mockito.mock(Record.class), 0, null, null),
                new RecordWrapper(Mockito.mock(Record.class), 1, null, null)
        );
        when(messageRecordConverter.convert(messages)).thenReturn(new RecordWrappers(validRecords, new ArrayList<>()));

        SinkResponse sinkResponse = maxComputeSink.pushToSink(messages);

        Assertions.assertEquals(2, sinkResponse.getErrors().size());
        Assert.assertTrue(sinkResponse.getErrors()
                .values()
                .stream()
                .allMatch(s -> ErrorType.DEFAULT_ERROR.equals(s.getErrorType())));
    }

    /**
     * Verifies that closing the sink performs no action and raises no exception.
     *
     * <p>Given a sink built from mocked collaborators, when {@link MaxComputeSink#close()} is invoked, then it
     * completes without throwing, confirming the no-op contract.</p>
     */
    @Test
    public void shouldDoNothing() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxComputeAccessId())
                .thenReturn("accessId");
        when(maxComputeSinkConfig.getMaxComputeAccessKey())
                .thenReturn("accessKey");
        when(maxComputeSinkConfig.getMaxComputeOdpsUrl())
                .thenReturn("odpsUrl");
        when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("projectId");
        when(maxComputeSinkConfig.getMaxComputeSchema())
                .thenReturn("schema");
        when(maxComputeSinkConfig.getMaxComputeTunnelUrl())
                .thenReturn("tunnelUrl");
        when(maxComputeSinkConfig.getTableValidatorNameRegex())
                .thenReturn("^[A-Za-z][A-Za-z0-9_]{0,127}$");
        when(maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable())
                .thenReturn(1200);
        when(maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable())
                .thenReturn(1);
        MessageRecordConverter messageRecordConverter = Mockito.mock(MessageRecordConverter.class);
        InsertManager insertManager = Mockito.mock(InsertManager.class);

        MaxComputeSink maxComputeSink = new MaxComputeSink(insertManager, messageRecordConverter, Mockito.mock(StatsDReporter.class), Mockito.mock(MaxComputeMetrics.class));

        assertDoesNotThrow(maxComputeSink::close);
    }

}
