package com.gotocompany.depot.maxcompute;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.bigquery.BigqueryStencilUpdateListenerFactory;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.schema.MaxComputeJsonSchemaCacheFactory;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class MaxComputeJsonSinkFactory implements MaxComputeSinkFactory {

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final SinkConfig sinkConfig;
    private final MaxComputeClient maxComputeClient;
    private final StatsDReporter statsDReporter;
    private final MaxComputeMetrics maxComputeMetrics;

    public MaxComputeJsonSinkFactory(StatsDReporter statsDReporter,
                                      StencilClient stencilClient,
                                      Map<String, String> env){
        this.maxComputeSinkConfig = ConfigFactory.create(MaxComputeSinkConfig.class, env);
        this.sinkConfig = ConfigFactory.create(SinkConfig.class, env);
        this.maxComputeMetrics = new MaxComputeMetrics(sinkConfig);
        this.statsDReporter = statsDReporter;
        this.maxComputeClient = new MaxComputeClient(maxComputeSinkConfig, statsDReporter, maxComputeMetrics);
    }

    public void init() {
        DepotStencilUpdateListener depotStencilUpdateListener = MaxComputeJsonSchemaCacheFactory.create(maxComputeSinkConfig, converterCache, statsDReporter);
    }

    public Sink create() {
        return null;
    }
}
