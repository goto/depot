package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class TableUtil {

    private final ConverterOrchestrator converterOrchestrator;
    private final MaxComputeSinkConfig maxComputeSinkConfig;

    public TableSchema buildTableSchema(Descriptors.Descriptor descriptor) {
        String partitionKey = maxComputeSinkConfig.getTablePartitionKey();
        List<Column> inferredFields = buildInferredFields(descriptor, partitionKey);
        List<Column> defaultFields = buildDefaultColumns();
        TableSchema.Builder tableSchemaBuilder = com.aliyun.odps.TableSchema.builder();
        tableSchemaBuilder.withColumns(inferredFields);
        tableSchemaBuilder.withColumns(defaultFields);
        if (maxComputeSinkConfig.isTablePartitioningEnabled()) {
            Column partitionColumn = buildPartitionColumn(descriptor, partitionKey);
            tableSchemaBuilder.withPartitionColumn(partitionColumn);
        }
        return tableSchemaBuilder.build();
    }

    private List<Column> buildInferredFields(Descriptors.Descriptor descriptor,
                                             String partitionKey) {
        return descriptor.getFields()
                .stream()
                .filter(fieldDescriptor -> !fieldDescriptor.getName().equals(partitionKey))
                .map(fieldDescriptor -> Column.newBuilder(fieldDescriptor.getName(),
                        converterOrchestrator.convert(fieldDescriptor)).build())
                .collect(Collectors.toList());
    }

    private Column buildPartitionColumn(Descriptors.Descriptor descriptor, String partitionKey) {
        return descriptor.getFields()
                .stream()
                .filter(fieldDescriptor -> fieldDescriptor.getName().equals(partitionKey))
                .map(fieldDescriptor -> Column.newBuilder(fieldDescriptor.getName(),
                        converterOrchestrator.convert(fieldDescriptor)).build())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Partition key not found in descriptor"));
    }

    private List<Column> buildDefaultColumns() {
        if (!maxComputeSinkConfig.shouldAddMetadata()) {
            return new ArrayList<>();
        }
        if (StringUtils.isBlank(maxComputeSinkConfig.getMaxcomputeMetadataNamespace())) {
            return maxComputeSinkConfig.getMetadataColumnsTypes()
                    .stream()
                    .map(tuple -> Column.newBuilder(tuple.getFirst(), MetadataUtil.getMetadataTypeInfo(tuple.getSecond())).build())
                    .collect(Collectors.toList());
        }
        return Collections.singletonList(Column.newBuilder(maxComputeSinkConfig.getMaxcomputeMetadataNamespace(),
                MetadataUtil.getMetadataTypeInfo(maxComputeSinkConfig)).build());
    }
}
