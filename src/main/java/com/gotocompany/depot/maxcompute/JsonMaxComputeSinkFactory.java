package com.gotocompany.depot.maxcompute;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkFactory;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class JsonMaxComputeSinkFactory implements SinkFactory {

    private final StatsDReporter statsDReporter;
    private final StencilClient stencilClient;
    private final SinkConfig sinkConfig;

    public JsonMaxComputeSinkFactory(StatsDReporter statsDReporter,
                                         StencilClient stencilClient,
                                         Map<String, String> env) {
        this.statsDReporter = statsDReporter;
        this.stencilClient = stencilClient;
        this.sinkConfig = ConfigFactory.create(SinkConfig.class, env);
    }

    @Override
    public void init() {

    }

    @Override
    public Sink create() {
        return null;
    }
}
