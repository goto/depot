package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PartitioningStrategyFactory {

    private final ConverterOrchestrator converterOrchestrator;
    private final MaxComputeSinkConfig maxComputeSinkConfig;

    public PartitioningStrategy createPartitioningStrategy(Descriptors.Descriptor descriptor) {
        if (!maxComputeSinkConfig.isTablePartitioningEnabled()) {
            return null;
        }
        String partitionKey = maxComputeSinkConfig.getTablePartitionKey();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor
                .findFieldByName(partitionKey);
        if (fieldDescriptor == null) {
            throw new IllegalArgumentException("Partition key not found in the descriptor: " + partitionKey);
        }
        TypeInfo partitionKeyTypeInfo = converterOrchestrator.convert(fieldDescriptor);
        if (TypeInfoFactory.TIMESTAMP.equals(partitionKeyTypeInfo)) {
            return new TimestampPartitioningStrategy(maxComputeSinkConfig);
        } else {
            return new DefaultPartitioningStrategy(partitionKeyTypeInfo, maxComputeSinkConfig);
        }
    }

}
