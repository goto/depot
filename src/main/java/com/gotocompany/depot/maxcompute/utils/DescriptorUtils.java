package com.gotocompany.depot.maxcompute.utils;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

import java.util.List;
import java.util.stream.Collectors;

public class DescriptorUtils {

    /**
     * Converts a field descriptor to a type info.
     * @param fieldDescriptor
     * @return
     */
    public static TypeInfo toTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        TypeInfo baseTypeInfo;
        switch (fieldDescriptor.getJavaType()) {
            case INT:
                baseTypeInfo = TypeInfoFactory.INT;
                break;
            case LONG:
                baseTypeInfo = TypeInfoFactory.BIGINT;
                break;
            case FLOAT:
            case DOUBLE:
                baseTypeInfo = TypeInfoFactory.DOUBLE;
                break;
            case BOOLEAN:
                baseTypeInfo = TypeInfoFactory.BOOLEAN;
                break;
            case STRING:
            case ENUM:
                baseTypeInfo = TypeInfoFactory.STRING;
                break;
            case MESSAGE:
                baseTypeInfo = messageToTypeInfo(fieldDescriptor);
                break;
            case BYTE_STRING:
                baseTypeInfo = TypeInfoFactory.BINARY;
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getJavaType());
        }
        return fieldDescriptor.isRepeated() ? TypeInfoFactory.getArrayTypeInfo(baseTypeInfo) : baseTypeInfo;
    }

    /**
     * Converts a message field to a struct type.
     * Recursively converts the fields of the message to struct types.
     * @param fieldDescriptor
     * @return
     */
    private static StructTypeInfo messageToTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        List<String> fieldNames = fieldDescriptor.getMessageType().getFields().stream()
                .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toList());
        List<TypeInfo> typeInfos = fieldDescriptor.getMessageType().getFields().stream()
                .map(DescriptorUtils::toTypeInfo)
                .collect(Collectors.toList());
        return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
    }
}
