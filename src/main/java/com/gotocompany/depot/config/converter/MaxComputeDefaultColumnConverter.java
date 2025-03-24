package com.gotocompany.depot.config.converter;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableSet;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Set;

import static com.aliyun.odps.OdpsType.*;

public class MaxComputeDefaultColumnConverter implements Converter<Column>  {
    private static final Set<OdpsType> PRIMITIVE_TYPES = ImmutableSet.of(
            BIGINT, DOUBLE, BOOLEAN, DATETIME, STRING, DECIMAL,
            ARRAY, TINYINT, SMALLINT, INT, FLOAT, DATE, TIMESTAMP,
            BINARY, TIMESTAMP_NTZ);

    private static final String NAME_TYPE_SEPARATOR = ":";
    private static final int COLUMN_NAME_INDEX = 0;
    private static final int COLUMN_TYPE_INDEX = 1;

    @Override
    public Column convert(Method method, String s) {
        String[] parts = s.split(NAME_TYPE_SEPARATOR);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid column format: " + s);
        }
        String name = parts[COLUMN_NAME_INDEX];
        OdpsType type = OdpsType.valueOf(parts[COLUMN_TYPE_INDEX].toUpperCase());
        if (!PRIMITIVE_TYPES.contains(type)) {
            throw new IllegalArgumentException("Unsupported column type: " + type);
        }
        return Column.newBuilder(name, TypeInfoFactory.getPrimitiveTypeInfo(type))
                .build();
    }
}
