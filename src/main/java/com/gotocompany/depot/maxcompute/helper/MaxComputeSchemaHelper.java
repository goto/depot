package com.gotocompany.depot.maxcompute.helper;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class MaxComputeSchemaHelper {

    private final ConverterOrchestrator converterOrchestrator;
    private final MaxComputeSinkConfig maxComputeSinkConfig;

    public MaxComputeSchema buildMaxComputeSchema(Descriptors.Descriptor descriptor) {
        List<Column> dataColumn = buildInferredFields(descriptor, maxComputeSinkConfig.getTablePartitionKey());
        List<Column> defaultColumns = buildDefaultColumns();
        Column partitionColumn = maxComputeSinkConfig.isTablePartitioningEnabled() ?
                buildPartitionColumn(descriptor, maxComputeSinkConfig.getTablePartitionKey()) : null;
        TableSchema.Builder tableSchemaBuilder = com.aliyun.odps.TableSchema.builder();
        tableSchemaBuilder.withColumns(dataColumn);
        tableSchemaBuilder.withColumns(defaultColumns);
        if (Objects.nonNull(partitionColumn)) {
            tableSchemaBuilder.withPartitionColumn(partitionColumn);
        }

        return MaxComputeSchema.builder()
                .descriptor(descriptor)
                .tableSchema(tableSchemaBuilder.build())
                .dataColumns(dataColumn.stream().collect(Collectors.toMap(Column::getName, Column::getTypeInfo)))
                .defaultColumns(defaultColumns.stream().collect(Collectors.toMap(Column::getName, Column::getTypeInfo)))
                .partitionColumns(Objects.nonNull(partitionColumn) ?
                        Collections.singletonMap(partitionColumn.getName(), partitionColumn.getTypeInfo()) : Collections.emptyMap())
                .build();

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
