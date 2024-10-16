package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.type.BaseTypeInfoConverter;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableUtil {

    private static final Map<String, TypeInfo> TYPE_INFO_MAP;
    private static final BaseTypeInfoConverter PRIMITIVE_TYPE_INFO_CONVERTER = new BaseTypeInfoConverter();

    static {
        TYPE_INFO_MAP = new HashMap<>();
        TYPE_INFO_MAP.put("integer", TypeInfoFactory.INT);
        TYPE_INFO_MAP.put("long", TypeInfoFactory.BIGINT);
        TYPE_INFO_MAP.put("float", TypeInfoFactory.FLOAT);
        TYPE_INFO_MAP.put("double", TypeInfoFactory.DOUBLE);
        TYPE_INFO_MAP.put("string", TypeInfoFactory.STRING);
        TYPE_INFO_MAP.put("boolean", TypeInfoFactory.BOOLEAN);
        TYPE_INFO_MAP.put("timestamp", TypeInfoFactory.TIMESTAMP_NTZ);
    }

    public static TableSchema buildTableSchema(Descriptors.Descriptor descriptor,
                                               MaxComputeSinkConfig maxComputeSinkConfig) {
        String partitionKey = maxComputeSinkConfig.getTablePartitionKey();
        List<Column> inferredFields = buildInferredFields(descriptor, partitionKey);
        List<Column> defaultFields = buildDefaultFields(maxComputeSinkConfig);
        TableSchema.Builder tableSchemaBuilder = com.aliyun.odps.TableSchema.builder();
        tableSchemaBuilder.withColumns(inferredFields);
        tableSchemaBuilder.withColumns(defaultFields);
        if (maxComputeSinkConfig.isTablePartitioningEnabled()) {
            Column partitionColumn = buildPartitionColumn(descriptor, partitionKey);
            tableSchemaBuilder.withPartitionColumn(partitionColumn);
        }
        return tableSchemaBuilder.build();
    }

    private static List<Column> buildInferredFields(Descriptors.Descriptor descriptor,
                                                    String partitionKey) {
        return descriptor.getFields()
                .stream()
                .filter(fieldDescriptor -> !fieldDescriptor.getName().equals(partitionKey))
                .map(fieldDescriptor -> Column.newBuilder(fieldDescriptor.getName(),
                        PRIMITIVE_TYPE_INFO_CONVERTER.convert(fieldDescriptor)).build())
                .collect(Collectors.toList());
    }

    private static Column buildPartitionColumn(Descriptors.Descriptor descriptor, String partitionKey) {
        return descriptor.getFields()
                .stream()
                .filter(fieldDescriptor -> fieldDescriptor.getName().equals(partitionKey))
                .map(fieldDescriptor -> Column.newBuilder(fieldDescriptor.getName(),
                        PRIMITIVE_TYPE_INFO_CONVERTER.convert(fieldDescriptor)).build())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Partition key not found in descriptor"));
    }

    private static List<Column> buildDefaultFields(MaxComputeSinkConfig maxComputeSinkConfig) {
        if (!maxComputeSinkConfig.shouldAddMetadata()) {
            return new ArrayList<>();
        }
        if (StringUtils.isBlank(maxComputeSinkConfig.getMaxcomputeMetadataNamespace())) {
            return maxComputeSinkConfig.getMetadataColumnsTypes()
                    .stream()
                    .map(tuple -> Column.newBuilder(tuple.getFirst(), TYPE_INFO_MAP.get(tuple.getSecond().toLowerCase()))
                            .build())
                    .collect(Collectors.toList());
        }
        return Collections.singletonList(Column.newBuilder(maxComputeSinkConfig.getMaxcomputeMetadataNamespace(),
                        TypeInfoFactory.getStructTypeInfo(maxComputeSinkConfig.getMetadataColumnsTypes()
                                        .stream()
                                        .map(TupleString::getFirst)
                                        .collect(Collectors.toList()),
                                maxComputeSinkConfig.getMetadataColumnsTypes()
                                        .stream()
                                        .map(tuple -> TYPE_INFO_MAP.get(tuple.getSecond().toLowerCase()))
                                        .collect(Collectors.toList())))
                .build());
    }
}
