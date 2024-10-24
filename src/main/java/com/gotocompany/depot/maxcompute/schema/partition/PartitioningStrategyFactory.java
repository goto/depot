package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PartitioningStrategyFactory {

    private final ConverterOrchestrator converterOrchestrator;
    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final MaxComputeSchemaCache maxComputeSchemaCache;

    public PartitioningStrategy<?> createPartitioningStrategy() {
        String partitionKey = maxComputeSinkConfig.getTablePartitionKey();
        Descriptors.FieldDescriptor fieldDescriptor = maxComputeSchemaCache.getMaxComputeSchema()
                .getDescriptor()
                .findFieldByName(partitionKey);
        TypeInfo partitionKeyTypeInfo = converterOrchestrator.convert(fieldDescriptor);

        if (TypeInfoFactory.TIMESTAMP_NTZ.equals(partitionKeyTypeInfo)) {
            return new TimestampPartitioningStrategy(maxComputeSinkConfig);
        } else {
            return new DefaultPartitioningStrategy(partitionKeyTypeInfo, maxComputeSinkConfig);
        }
    }

}
