package com.gotocompany.depot.maxcompute.utils;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ProtoToRecordUtils {

    public static Record toRecord(TableSchema tableSchema, DynamicMessage dynamicMessage) {
        ArrayRecord arrayRecord = new ArrayRecord(tableSchema);
        dynamicMessage.getAllFields()
                .forEach((fieldDescriptor, value1) -> {
                    Object value = handleField(fieldDescriptor, value1);
                    if (fieldDescriptor.isRepeated()) {
                        arrayRecord.setArray(fieldDescriptor.getName(), (List<?>) value);
                        return;
                    }
                    if (Descriptors.FieldDescriptor.JavaType.MESSAGE.equals(fieldDescriptor.getJavaType())) {
                        arrayRecord.setStruct(fieldDescriptor.getName(), (SimpleStruct) value);
                        return;
                    }
                    arrayRecord.set(fieldDescriptor.getName(), value);
                });
        return arrayRecord;
    }

    private static Object handleField(Descriptors.FieldDescriptor fieldDescriptor, Object value) {
        if (fieldDescriptor.isRepeated() && !Descriptors.FieldDescriptor.JavaType.MESSAGE.equals(fieldDescriptor.getJavaType())) {
            return value;
        }
        if (fieldDescriptor.isRepeated()) {
            return ((List<?>) value).stream()
                    .map(v -> dynamicMessageToSimpleStruct((DynamicMessage) v))
                    .collect(Collectors.toList());
        }
        if (Descriptors.FieldDescriptor.JavaType.MESSAGE.equals(fieldDescriptor.getJavaType())) {
            return dynamicMessageToSimpleStruct((DynamicMessage) value);
        }
        return value;
    }

    private static SimpleStruct dynamicMessageToSimpleStruct(DynamicMessage dynamicMessage) {
        ProtoRecordWrapper protoRecordWrapper = new ProtoRecordWrapper();
        dynamicMessage.getAllFields().forEach((fieldDescriptor, value1) -> {
            Object value = handleField(fieldDescriptor, value1);
            protoRecordWrapper.addField(fieldDescriptor.getName(), DescriptorUtils.toTypeInfo(fieldDescriptor), value);
        });
        return new SimpleStruct(TypeInfoFactory.getStructTypeInfo(protoRecordWrapper.fieldNames, protoRecordWrapper.typeInfos), protoRecordWrapper.values);
    }


    private final static class ProtoRecordWrapper {
        private final List<String> fieldNames;
        private final List<TypeInfo> typeInfos;
        private final List<Object> values;

        private ProtoRecordWrapper() {
            this.fieldNames = new ArrayList<>();
            this.typeInfos = new ArrayList<>();
            this.values = new ArrayList<>();
        }

        private void addField(String fieldName, TypeInfo typeInfo, Object value) {
            this.fieldNames.add(fieldName);
            this.typeInfos.add(typeInfo);
            this.values.add(value);
        }
    }

}
