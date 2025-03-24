package com.gotocompany.depot.maxcompute.schema;

import com.aliyun.odps.TableSchema;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MaxComputeJsonSchemaCache extends DepotStencilUpdateListener {

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final MaxComputeJsonSchemaBuilder MaxComputeJsonSchemaBuilder;
    private final MaxComputeClient maxComputeClient;

    @Override
    public void updateSchema() {
        TableSchema existingSchema = maxComputeClient.getLatestTableSchema();
    }
}
