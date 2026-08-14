package com.gotocompany.depot.maxcompute;

import com.aliyun.odps.exceptions.SchemaMismatchException;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.NonRetryableException;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.maxcompute.client.insert.InsertManager;
import com.gotocompany.depot.maxcompute.converter.record.MessageRecordConverter;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link Sink} implementation that writes batches of Depot {@link Message} objects to an Alibaba
 * MaxCompute table.
 *
 * <p>This is the terminal component of the MaxCompute sink pipeline assembled by
 * {@link MaxComputeSinkFactory}. For every batch handed to {@link #pushToSink(List)} it:</p>
 * <ul>
 *     <li>converts the incoming protobuf messages into MaxCompute records through the
 *     {@link MessageRecordConverter}, separating records that converted cleanly from records that
 *     failed validation or deserialization;</li>
 *     <li>captures the time spent in conversion as a latency metric through {@link Instrumentation};</li>
 *     <li>records every conversion failure in the {@link SinkResponse}, keyed by the original message
 *     index, so that the caller can route those messages to a dead-letter or retry queue;</li>
 *     <li>streams the valid records into MaxCompute through the {@link InsertManager} and classifies any
 *     insertion failure as non-retryable, retryable, or default, emitting the corresponding error
 *     counters.</li>
 * </ul>
 *
 * <p>An instance is meant to be driven by a single worker thread; the collaborators it delegates to own
 * any thread-scoped state (for example the streaming upload sessions managed by the
 * {@link InsertManager}).</p>
 *
 * @see Sink
 * @see MaxComputeSinkFactory
 * @see InsertManager
 * @see MessageRecordConverter
 */
@Slf4j
public class MaxComputeSink implements Sink {

    /**
     * Performs the streaming insert of the valid records of each batch into the MaxCompute table.
     */
    private final InsertManager insertManager;
    /**
     * Converts the incoming protobuf messages into MaxCompute records, partitioning the batch into valid
     * and invalid records.
     */
    private final MessageRecordConverter messageRecordConverter;
    /**
     * Emits StatsD metrics for this sink, such as the conversion latency and the per-error-type counters.
     */
    private final Instrumentation instrumentation;
    /**
     * Supplies the metric names and tag templates used when reporting MaxCompute operations and errors.
     */
    private final MaxComputeMetrics maxComputeMetrics;

    /**
     * Creates a MaxCompute sink wired with the collaborators it needs to convert and insert records.
     *
     * <p>The {@link Instrumentation} used for metric reporting is built internally from the supplied
     * {@link StatsDReporter} and is scoped to this class.</p>
     *
     * @param insertManager          manager that performs the streaming insert of valid records into MaxCompute
     * @param messageRecordConverter converter that turns the incoming messages into MaxCompute records
     * @param statsDReporter         reporter used to build the {@link Instrumentation} that emits StatsD metrics
     * @param maxComputeMetrics      holder of metric identifiers and tag templates for MaxCompute operations
     */
    public MaxComputeSink(InsertManager insertManager,
                          MessageRecordConverter messageRecordConverter,
                          StatsDReporter statsDReporter,
                          MaxComputeMetrics maxComputeMetrics) {
        this.insertManager = insertManager;
        this.messageRecordConverter = messageRecordConverter;
        this.maxComputeMetrics = maxComputeMetrics;
        this.instrumentation = new Instrumentation(statsDReporter, this.getClass());
    }


    /**
     * Converts a batch of messages and streams the valid ones into MaxCompute.
     *
     * <p>The method first converts {@code messages} into a {@link RecordWrappers} bundle, recording the
     * elapsed conversion time as a latency metric. Each record that could not be converted is added to the
     * response error map keyed by its original index. The remaining valid records are then handed to the
     * {@link InsertManager}. Failures raised during insertion are caught and translated into error entries
     * and error counters as follows:</p>
     * <ul>
     *     <li>{@link NonRetryableException} is recorded as {@link ErrorType#SINK_NON_RETRYABLE_ERROR}; when
     *     its cause is a {@link SchemaMismatchException} an additional schema-mismatch counter is emitted;</li>
     *     <li>{@link IOException} and {@link TunnelException} are recorded as
     *     {@link ErrorType#SINK_RETRYABLE_ERROR};</li>
     *     <li>any other exception is recorded as {@link ErrorType#DEFAULT_ERROR}.</li>
     * </ul>
     *
     * <p>In every failure branch all valid records of the batch share the same {@link ErrorInfo}, because a
     * streaming insert either succeeds or fails for the whole pack.</p>
     *
     * @param messages the batch of messages to convert and insert; each message keeps its positional index,
     *                 which is reused as the key in the response error map
     * @return a {@link SinkResponse} whose error map holds one entry per failed record, or an empty error map
     *         when conversion and insertion both succeeded for every record
     * @throws SinkException declared by the {@link Sink} contract; this implementation captures conversion and
     *                       insertion failures into the returned response rather than propagating them
     */
    @Override
    public SinkResponse pushToSink(List<Message> messages) throws SinkException {
        SinkResponse sinkResponse = new SinkResponse();
        Instant conversionStartTime = Instant.now();
        RecordWrappers recordWrappers = messageRecordConverter.convert(messages);
        instrumentation.captureDurationSince(maxComputeMetrics.getMaxComputeConversionLatencyMetric(), conversionStartTime);
        recordWrappers.getInvalidRecords()
                .forEach(invalidRecord -> sinkResponse.getErrors().put(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));
        try {
            insertManager.insert(recordWrappers.getValidRecords());
        } catch (NonRetryableException e) {
            log.error("Error while inserting records to MaxCompute: ", e);
            sinkResponse.addErrors(recordWrappers.getValidRecords().stream().map(RecordWrapper::getIndex).collect(Collectors.toList()), new ErrorInfo(e, ErrorType.SINK_NON_RETRYABLE_ERROR));
            instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeOperationTotalMetric(),
                    String.format(MaxComputeMetrics.MAXCOMPUTE_ERROR_TAG, e.getClass().getSimpleName()));
            if (e.getCause() instanceof SchemaMismatchException) {
                instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeOperationTotalMetric(),
                        String.format(MaxComputeMetrics.MAXCOMPUTE_ERROR_TAG, SchemaMismatchException.class.getSimpleName()));
            }
        } catch (IOException | TunnelException e) {
            log.error("Error while inserting records to MaxCompute: ", e);
            sinkResponse.addErrors(recordWrappers.getValidRecords().stream().map(RecordWrapper::getIndex).collect(Collectors.toList()), new ErrorInfo(e, ErrorType.SINK_RETRYABLE_ERROR));
            instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeOperationTotalMetric(),
                    String.format(MaxComputeMetrics.MAXCOMPUTE_ERROR_TAG, e.getClass().getSimpleName()));
        } catch (Exception e) {
            log.error("Error while inserting records to MaxCompute: ", e);
            sinkResponse.addErrors(recordWrappers.getValidRecords().stream().map(RecordWrapper::getIndex).collect(Collectors.toList()), new ErrorInfo(e, ErrorType.DEFAULT_ERROR));
            instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeOperationTotalMetric(),
                    String.format(MaxComputeMetrics.MAXCOMPUTE_ERROR_TAG, e.getClass().getSimpleName()));
        }
        return sinkResponse;
    }

    /**
     * Releases resources held by the sink.
     *
     * <p>This implementation owns no closeable resources of its own, because the MaxCompute streaming
     * upload sessions are created and recycled by the {@link InsertManager}. The method is therefore an
     * intentional no-op.</p>
     *
     * @throws IOException declared by {@link java.io.Closeable}; never actually thrown by this implementation
     */
    @Override
    public void close() throws IOException {
    }

}
