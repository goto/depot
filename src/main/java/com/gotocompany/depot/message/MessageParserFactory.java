package com.gotocompany.depot.message;

import com.gotocompany.depot.message.json.JsonMessageParser;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.JsonParserMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import com.gotocompany.depot.config.SinkConfig;

/**
 * Factory for constructing the {@link MessageParser} that matches the configured schema data type.
 *
 * <p>Depot supports more than one payload encoding, and this factory centralizes the decision of
 * which parser implementation to instantiate. It inspects
 * {@link SinkConfig#getSinkConnectorSchemaDataType()} and returns a {@link JsonMessageParser} for
 * JSON payloads or a {@link ProtoMessageParser} for Protobuf payloads, wiring each parser with the
 * metrics and schema-update collaborators it requires.</p>
 *
 * @see MessageParser
 * @see JsonMessageParser
 * @see ProtoMessageParser
 */
public class MessageParserFactory {
    /**
     * Creates the parser matching the sink's configured schema data type.
     *
     * <p>When the configured type is {@code JSON} a {@link JsonMessageParser} is returned, configured
     * with its own {@link Instrumentation} and {@link JsonParserMetrics}. When the type is
     * {@code PROTOBUF} a {@link ProtoMessageParser} is returned, wired with the supplied
     * {@link DepotStencilUpdateListener} so it can react to schema-registry updates. Any other
     * configured type is rejected.</p>
     *
     * @param config the sink configuration that determines the schema data type and parser settings
     * @param statsDReporter the reporter used to emit parser metrics
     * @param depotStencilUpdateListener the listener notified of Stencil schema updates; used only by
     *     the Protobuf parser and may be {@code null}
     * @return a parser appropriate for the configured schema data type
     * @throws IllegalArgumentException if the configured schema data type is not supported
     */
    public static MessageParser getParser(SinkConfig config, StatsDReporter statsDReporter, DepotStencilUpdateListener depotStencilUpdateListener) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case JSON:
                return new JsonMessageParser(config,
                        new Instrumentation(statsDReporter, JsonMessageParser.class),
                        new JsonParserMetrics(config));
            case PROTOBUF:
                return new ProtoMessageParser(config, statsDReporter, depotStencilUpdateListener);
            default:
                throw new IllegalArgumentException("Schema Type is not supported");
        }
    }

    /**
     * Creates the parser matching the sink's configured schema data type without a schema-update
     * listener.
     *
     * <p>This convenience overload delegates to
     * {@link #getParser(SinkConfig, StatsDReporter, DepotStencilUpdateListener)} with a {@code null}
     * listener. It is suitable when the caller does not need to be notified of Protobuf
     * schema-registry updates.</p>
     *
     * @param config the sink configuration that determines the schema data type and parser settings
     * @param statsDReporter the reporter used to emit parser metrics
     * @return a parser appropriate for the configured schema data type
     * @throws IllegalArgumentException if the configured schema data type is not supported
     */
    public static MessageParser getParser(SinkConfig config, StatsDReporter statsDReporter) {
        return getParser(config, statsDReporter, null);
    }
}
