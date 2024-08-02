package com.gotocompany.depot.maxcompute.utils;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

public class ProtoToSchemaUtils {

    public static TableSchema toTableSchema(Descriptors.Descriptor descriptor) {
        TableSchema.Builder tableSchemaBuilder = TableSchema.builder();
        descriptor.getFields()
                .forEach(fieldDescriptor -> handleColumnMapping(fieldDescriptor, tableSchemaBuilder));
        return tableSchemaBuilder.build();
    }


    private static void handleColumnMapping(Descriptors.FieldDescriptor fieldDescriptor,
                                            TableSchema.Builder tableSchemaBuilder) {
        if (fieldDescriptor.isRepeated()) {
            tableSchemaBuilder.withColumn(Column.newBuilder(fieldDescriptor.getName(),
                    TypeInfoFactory.getArrayTypeInfo(DescriptorUtils.toTypeInfo(fieldDescriptor))).build());
            return;
        }
        tableSchemaBuilder.withColumn(Column.newBuilder(fieldDescriptor.getName(), DescriptorUtils.toTypeInfo(fieldDescriptor)).build());
    }

}
