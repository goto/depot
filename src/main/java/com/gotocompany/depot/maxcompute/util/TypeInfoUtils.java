package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.PrimitiveTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;

/**
 * Utility class to check the type of {@link TypeInfo} objects.
 * This class is deprecated and will be removed in future releases once official support for schema evolution is added.
 *
 * <p>These predicates are used while walking a {@link com.aliyun.odps.TableSchema} (for example when
 * computing schema differences) to decide how to descend into nested types. The class exposes only
 * static methods and is not intended to be instantiated.</p>
 *
 * @deprecated This class is deprecated and will be removed in a future release once official support
 *         for schema evolution is added.
 */
@Deprecated
public class TypeInfoUtils {
    /**
     * Determines whether the given type is a MaxCompute primitive type.
     *
     * @param typeInfo the type to test
     * @return {@code true} if {@code typeInfo} is a {@link PrimitiveTypeInfo}, {@code false} otherwise
     */
    public static boolean isPrimitiveType(TypeInfo typeInfo) {
        return typeInfo instanceof PrimitiveTypeInfo;
    }

    /**
     * Determines whether the given type is an array whose elements are structs.
     *
     * @param typeInfo the type to test
     * @return {@code true} if {@code typeInfo} is an {@link ArrayTypeInfo} whose element type is a
     *         {@link StructTypeInfo}, {@code false} otherwise
     */
    public static boolean isStructArrayType(TypeInfo typeInfo) {
        return typeInfo instanceof ArrayTypeInfo && ((ArrayTypeInfo) typeInfo).getElementTypeInfo() instanceof StructTypeInfo;
    }

    /**
     * Determines whether the given type is an array whose elements are primitives.
     *
     * @param typeInfo the type to test
     * @return {@code true} if {@code typeInfo} is an {@link ArrayTypeInfo} whose element type is a
     *         {@link PrimitiveTypeInfo}, {@code false} otherwise
     */
    public static boolean isPrimitiveArrayType(TypeInfo typeInfo) {
        return typeInfo instanceof ArrayTypeInfo && ((ArrayTypeInfo) typeInfo).getElementTypeInfo() instanceof PrimitiveTypeInfo;
    }

    /**
     * Determines whether the given type is a MaxCompute struct type.
     *
     * @param typeInfo the type to test
     * @return {@code true} if {@code typeInfo} is a {@link StructTypeInfo}, {@code false} otherwise
     */
    public static boolean isStructType(TypeInfo typeInfo) {
        return typeInfo instanceof StructTypeInfo;
    }
}
