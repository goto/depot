package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MetadataUtil {

    private static final Map<String, TypeInfo> METADATA_TYPE_MAP;

    static {
        METADATA_TYPE_MAP = new HashMap<>();
        METADATA_TYPE_MAP.put("integer", TypeInfoFactory.INT);
        METADATA_TYPE_MAP.put("long", TypeInfoFactory.BIGINT);
        METADATA_TYPE_MAP.put("float", TypeInfoFactory.FLOAT);
        METADATA_TYPE_MAP.put("double", TypeInfoFactory.DOUBLE);
        METADATA_TYPE_MAP.put("string", TypeInfoFactory.STRING);
        METADATA_TYPE_MAP.put("boolean", TypeInfoFactory.BOOLEAN);
        METADATA_TYPE_MAP.put("timestamp", TypeInfoFactory.TIMESTAMP);
    }

    public static TypeInfo getMetadataTypeInfo(String type) {
        return METADATA_TYPE_MAP.get(type.toLowerCase());
    }

    public static StructTypeInfo getMetadataTypeInfo(MaxComputeSinkConfig maxComputeSinkConfig) {
        return TypeInfoFactory.getStructTypeInfo(maxComputeSinkConfig.getMetadataColumnsTypes()
                        .stream()
                        .map(TupleString::getFirst)
                        .collect(Collectors.toList()),
                maxComputeSinkConfig.getMetadataColumnsTypes()
                        .stream()
                        .map(tuple -> METADATA_TYPE_MAP.get(tuple.getSecond().toLowerCase()))
                        .collect(Collectors.toList()));
    }

}
