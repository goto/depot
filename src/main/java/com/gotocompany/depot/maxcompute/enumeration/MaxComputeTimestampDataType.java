package com.gotocompany.depot.maxcompute.enumeration;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import lombok.Getter;

@Getter
public enum MaxComputeTimestampDataType {

    TIMESTAMP(TypeInfoFactory.TIMESTAMP),
    TIMESTAMP_NTZ(TypeInfoFactory.TIMESTAMP_NTZ);

    private final TypeInfo typeInfo;

    MaxComputeTimestampDataType(TypeInfo typeInfo) {
        this.typeInfo = typeInfo;
    }

}
