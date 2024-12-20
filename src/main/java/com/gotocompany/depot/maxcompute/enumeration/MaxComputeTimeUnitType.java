package com.gotocompany.depot.maxcompute.enumeration;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import lombok.Getter;

@Getter
public enum MaxComputeTimeUnitType {
    TIMESTAMP(TypeInfoFactory.TIMESTAMP),
    TIMESTAMP_NTZ(TypeInfoFactory.TIMESTAMP_NTZ);

    private final TypeInfo typeInfo;

    MaxComputeTimeUnitType(TypeInfo typeInfo){
        this.typeInfo = typeInfo;
    }

}
