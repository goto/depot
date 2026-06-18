package com.gotocompany.depot.log;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.SinkResponse;

import java.io.IOException;
import java.util.List;

/**
 * Sink implementation that parses each record and writes its decoded form to the application log.
 *
 * <p>{@code LogSink} is the simplest Depot sink: instead of delivering records to an external system
 * it logs them, which makes it useful for debugging, local development, and verifying that messages
 * parse correctly against their schema. For every record it parses the configured portion (key or
 * value) into a {@link ParsedMessage} and logs the decoded payload together with the record's
 * metadata.</p>
 *
 * <p>It honors the standard {@link Sink} contract: parsing failures are reported per record through
 * the returned {@link SinkResponse} rather than aborting the batch, so a malformed record is recorded
 * as an {@link ErrorType#DESERIALIZATION_ERROR} while the remaining records continue to be logged.
 * Instances are created by {@link LogSinkFactory}.</p>
 *
 * @see Sink
 * @see LogSinkFactory
 * @see SinkResponse
 */
public class LogSink implements Sink {
    /** Parser used to decode each record's configured payload into a {@link ParsedMessage}. */
    private final MessageParser messageParser;
    /** Logging and metrics facade through which parsed records are emitted. */
    private final Instrumentation instrumentation;
    /** Sink configuration that selects the schema message mode and the schema class to parse. */
    private final SinkConfig config;

    /**
     * Creates a log sink wired with its parser, instrumentation, and configuration.
     *
     * @param config the sink configuration that selects which portion of each record is parsed and
     *     the schema class to parse it against
     * @param messageParser the parser used to decode each record before it is logged
     * @param instrumentation the logging and metrics facade used to emit parsed records
     */
    public LogSink(SinkConfig config, MessageParser messageParser, Instrumentation instrumentation) {
        this.messageParser = messageParser;
        this.instrumentation = instrumentation;
        this.config = config;
    }

    /**
     * Parses each record and writes its decoded payload and metadata to the log.
     *
     * <p>The configured {@link SinkConnectorSchemaMessageMode} determines whether the key or the value
     * of each record is parsed, and the corresponding schema class is taken from the sink
     * configuration. Each record is parsed in turn; on success the {@link ParsedMessage} and the
     * record's {@linkplain Message#getMetadataString() metadata} are logged. If parsing throws an
     * {@link java.io.IOException}, the failure is recorded in the response as an
     * {@link ErrorType#DESERIALIZATION_ERROR} against that record's index and processing continues
     * with the next record.</p>
     *
     * @param messages the batch of records to parse and log
     * @return a {@link SinkResponse} holding a deserialization error for every record that failed to
     *     parse; empty of errors when every record was logged successfully
     * @throws SinkException declared by the {@link Sink#pushToSink(java.util.List)} contract; this
     *     implementation reports parse failures through the returned {@link SinkResponse} and does not
     *     itself throw it
     */
    @Override
    public SinkResponse pushToSink(List<Message> messages) throws SinkException {
        SinkResponse response = new SinkResponse();
        SinkConnectorSchemaMessageMode mode = config.getSinkConnectorSchemaMessageMode();
        String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? config.getSinkConnectorSchemaProtoMessageClass() : config.getSinkConnectorSchemaProtoKeyClass();
        for (int ii = 0; ii < messages.size(); ii++) {
            Message message = messages.get(ii);
            try {
                ParsedMessage parsedMessage =
                        messageParser.parse(
                                message,
                                mode,
                                schemaClass);
                instrumentation.logInfo("\n================= DATA =======================\n{}"
                                + "\n================= METADATA =======================\n{}\n",
                        parsedMessage.toString(), message.getMetadataString());
            } catch (IOException e) {
                response.addErrors(ii, new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
            }
        }
        return response;
    }

    /**
     * Releases resources held by this sink.
     *
     * <p>{@code LogSink} holds no closeable resources of its own, so this implementation does nothing
     * and returns immediately.</p>
     *
     * @throws IOException declared by {@link java.io.Closeable#close()}; never actually thrown by this
     *     implementation
     */
    @Override
    public void close() throws IOException {

    }
}
