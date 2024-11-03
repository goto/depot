package com.gotocompany.depot.maxcompute;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.metrics.StatsDReporter;


public class MaxComputeSinkFactory {
    private final StatsDReporter statsDReporter;
    private final MaxComputeSinkConfig sinkConfig;
    private MaxComputeClient maxComputeClient;

    public MaxComputeSinkFactory(MaxComputeSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }
    public void init() {
        this.maxComputeClient = new MaxComputeClient(this.sinkConfig);
    }
}
