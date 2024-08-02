package com.gotocompany.depot.maxcompute.utils;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

import java.util.List;
import java.util.stream.Collectors;

public class ProtoToSchemaUtils {

    public static TableSchema toTableSchema(Descriptors.Descriptor descriptor) {
        TableSchema.Builder tableSchemaBuilder = TableSchema.builder();
        descriptor.getFields()
                .forEach(fieldDescriptor -> handleColumnMapping(fieldDescriptor, tableSchemaBuilder));
        return tableSchemaBuilder.build();
    }

    private static TypeInfo toTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        switch (fieldDescriptor.getJavaType()) {
            case INT:
                return TypeInfoFactory.INT;
            case LONG:
                return TypeInfoFactory.BIGINT;
            case FLOAT:
            case DOUBLE:
                return TypeInfoFactory.DOUBLE;
            case BOOLEAN:
                return TypeInfoFactory.BOOLEAN;
            case STRING:
            case ENUM:
                return TypeInfoFactory.STRING;
            case MESSAGE:
                return messageToTypeInfo(fieldDescriptor);
            case BYTE_STRING:
                return TypeInfoFactory.BINARY;
            default:
                throw new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getJavaType());
        }
    }

    private static void handleColumnMapping(Descriptors.FieldDescriptor fieldDescriptor,
                                            TableSchema.Builder tableSchemaBuilder) {
        if (fieldDescriptor.isRepeated()) {
            tableSchemaBuilder.withColumn(Column.newBuilder(fieldDescriptor.getName(), TypeInfoFactory.getArrayTypeInfo(toTypeInfo(fieldDescriptor))).build());
        }
        tableSchemaBuilder.withColumn(Column.newBuilder(fieldDescriptor.getName(), toTypeInfo(fieldDescriptor)).build());
    }

    private static StructTypeInfo messageToTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        List<String> fieldNames = fieldDescriptor.getMessageType().getFields().stream()
                .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toList());
        List<TypeInfo> typeInfos = fieldDescriptor.getMessageType().getFields().stream()
                .map(ProtoToSchemaUtils::toTypeInfo)
                .collect(Collectors.toList());
        return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
    }

}
