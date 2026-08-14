package com.gotocompany.depot.bigquery.handler;

import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.config.enums.SinkConnectorSchemaDataType;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.bigquery.client.BigQueryClient;

/**
 * Factory that selects the appropriate {@link ErrorHandler} for the configured schema data type.
 *
 * <p>Exposes a single {@code static}
 * {@link #create(BigQuerySinkConfig, BigQueryClient, StatsDReporter)} helper and is not
 * intended to be instantiated.</p>
 */
public class ErrorHandlerFactory {
    /**
     * Creates the {@link ErrorHandler} matching the sink's schema data type.
     *
     * <p>When the sink connector schema data type is {@code JSON}, returns a
     * {@link JsonErrorHandler} wired with its own {@link Instrumentation}; for every other
     * data type returns a {@link NoopErrorHandler}.</p>
     *
     * @param sinkConfig     the BigQuery sink configuration that determines the schema
     *                       data type and the JSON handler behaviour
     * @param bigQueryClient the client the JSON handler uses to read and upsert the table
     *                       schema
     * @param statsDReprter  the reporter used to build the handler's instrumentation
     * @return a {@link JsonErrorHandler} when the schema data type is {@code JSON},
     *         otherwise a {@link NoopErrorHandler}
     */
    public static ErrorHandler create(BigQuerySinkConfig sinkConfig, BigQueryClient bigQueryClient, StatsDReporter statsDReprter) {
        if (SinkConnectorSchemaDataType.JSON == sinkConfig.getSinkConnectorSchemaDataType()) {
            return new JsonErrorHandler(
                    bigQueryClient,
                    sinkConfig, new Instrumentation(statsDReprter, JsonErrorHandler.class));
        }
        return new NoopErrorHandler();
    }
}
