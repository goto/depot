package com.gotocompany.depot.bigquery;

import com.gotocompany.depot.bigquery.client.BigQueryClient;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverterCache;
import com.gotocompany.depot.bigquery.json.BigqueryJsonUpdateListener;
import com.gotocompany.depot.bigquery.proto.BigqueryProtoUpdateListener;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;

/**
 * Factory that creates the {@link DepotStencilUpdateListener} appropriate for the schema data type.
 *
 * <p>Exposes a single {@code static}
 * {@link #create(BigQuerySinkConfig, BigQueryClient, MessageRecordConverterCache, StatsDReporter)}
 * helper and is not intended to be instantiated.</p>
 */
public class BigqueryStencilUpdateListenerFactory {
    /**
     * Creates the schema update listener matching the configured schema data type.
     *
     * <p>Returns a {@link BigqueryJsonUpdateListener} for {@code JSON} schemas and a
     * {@link BigqueryProtoUpdateListener} for {@code PROTOBUF} schemas.</p>
     *
     * @param config         the sink configuration that determines the schema data type
     * @param bqClient       the BigQuery client passed to the created listener
     * @param converterCache the converter cache shared with the created listener
     * @param statsDReporter the reporter used to build the JSON listener's instrumentation
     * @return a {@link DepotStencilUpdateListener} for the configured schema data type
     * @throws ConfigurationException if the configured schema data type is not supported
     */
    public static DepotStencilUpdateListener create(BigQuerySinkConfig config, BigQueryClient bqClient, MessageRecordConverterCache converterCache, StatsDReporter statsDReporter) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case JSON:
                return new BigqueryJsonUpdateListener(config, converterCache, bqClient, new Instrumentation(statsDReporter, BigqueryJsonUpdateListener.class));
            case PROTOBUF:
                return new BigqueryProtoUpdateListener(config, bqClient, converterCache);
            default:
                throw new ConfigurationException("Schema Type is not supported");
        }
    }
}
