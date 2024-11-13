package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.PrimitiveTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;

public class TypeInfoUtils {
    public static boolean isPrimitiveType(TypeInfo typeInfo) {
        return typeInfo instanceof PrimitiveTypeInfo;
    }

    public static boolean isStructArrayType(TypeInfo typeInfo) {
        return typeInfo instanceof ArrayTypeInfo && ((ArrayTypeInfo) typeInfo).getElementTypeInfo() instanceof StructTypeInfo;
    }

    public static boolean isPrimitiveArrayType(TypeInfo typeInfo) {
        return typeInfo instanceof ArrayTypeInfo && ((ArrayTypeInfo) typeInfo).getElementTypeInfo() instanceof PrimitiveTypeInfo;
    }

    public static boolean isStructType(TypeInfo typeInfo) {
        return typeInfo instanceof StructTypeInfo;
    }
}