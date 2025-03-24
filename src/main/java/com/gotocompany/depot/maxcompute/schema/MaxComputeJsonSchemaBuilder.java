package com.gotocompany.depot.maxcompute.schema;


import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class MaxComputeJsonSchemaBuilder {

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final PartitioningStrategy partitioningStrategy;
    private final MetadataUtil metadataUtil;


    public MaxComputeSchema build() {
        List<Column> metadataColumns = buildMetadataColumns();
        TableSchema.Builder tableSchemaBuilder = com.aliyun.odps.TableSchema.builder()
                .withColumns(metadataColumns)
                .withColumns(buildDataColumns());
        if (maxComputeSinkConfig.isTablePartitioningEnabled()) {
            tableSchemaBuilder.withPartitionColumn(buildPartitionColumn());
        }
        return new MaxComputeSchema(tableSchemaBuilder.build(), metadataColumns.stream().collect(Collectors.toMap(Column::getName, Column::getTypeInfo)));
    }

    private Column buildPartitionColumn() {
        return partitioningStrategy.getPartitionColumn();
    }

    private List<Column> buildDataColumns() {
        return maxComputeSinkConfig.getDefaultColumns()
                .stream()
                .filter(col -> {
                    if (!maxComputeSinkConfig.isTablePartitioningEnabled() || !col.getName().equals(maxComputeSinkConfig.getTablePartitionKey())) {
                        return true;
                    }
                    return !partitioningStrategy.shouldReplaceOriginalColumn();
                }).collect(Collectors.toList());
    }

    private List<Column> buildMetadataColumns() {
        if (!maxComputeSinkConfig.shouldAddMetadata()) {
            return new ArrayList<>();
        }
        if (StringUtils.isNotBlank(maxComputeSinkConfig.getMaxcomputeMetadataNamespace())) {
            throw new IllegalArgumentException("Maxcompute metadata namespace is not supported for JSON schema");
        }
        return maxComputeSinkConfig.getMetadataColumnsTypes()
                .stream()
                .map(tuple -> Column.newBuilder(tuple.getFirst(), metadataUtil.getMetadataTypeInfo(tuple.getSecond())).build())
                .collect(Collectors.toList());
    }

}
