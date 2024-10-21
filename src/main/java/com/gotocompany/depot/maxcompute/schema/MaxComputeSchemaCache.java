package com.gotocompany.depot.maxcompute.schema;

import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.helper.MaxComputeSchemaHelper;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import com.gotocompany.depot.utils.StencilUtils;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import lombok.Getter;

public class MaxComputeSchemaCache extends DepotStencilUpdateListener {

    private final MaxComputeSchemaHelper maxComputeSchemaHelper;
    private final SinkConfig sinkConfig;
    private final StencilClient stencilClient;
    @Getter
    private MaxComputeSchema maxComputeSchema;

    public MaxComputeSchemaCache(MaxComputeSchemaHelper maxComputeSchemaHelper,
                                 SinkConfig sinkConfig,
                                 StatsDReporter statsDReporter) {
        this.maxComputeSchemaHelper = maxComputeSchemaHelper;
        this.sinkConfig = sinkConfig;
        if (sinkConfig.isSchemaRegistryStencilEnable()) {
            StencilConfig stencilConfig = StencilUtils.getStencilConfig(sinkConfig, statsDReporter.getClient(), this);
            stencilClient = StencilClientFactory.getClient(sinkConfig.getSchemaRegistryStencilUrls(), stencilConfig);
        } else {
            stencilClient = StencilClientFactory.getClient();
        }
    }


    @Override
    public void updateSchema() {
        this.maxComputeSchema = maxComputeSchemaHelper.buildMaxComputeSchema(
                stencilClient.get(getSchemaClass())
        );
    }

    private String getSchemaClass() {
        return sinkConfig.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
    }
}
