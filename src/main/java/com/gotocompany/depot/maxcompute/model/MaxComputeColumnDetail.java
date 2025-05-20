package com.gotocompany.depot.maxcompute.model;

import com.aliyun.odps.type.NestedTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

@AllArgsConstructor
public class MaxComputeColumnDetail {
    private String prefix;
    private String name;
    @Getter
    private TypeInfo typeInfo;
    private boolean isArrayElement;

    public String getDDL() {
        String typeInfoRepresentation = typeInfo instanceof NestedTypeInfo ? ((NestedTypeInfo) typeInfo).getTypeName(true) : typeInfo.toString();
        return String.format("%s %s", getFullName(), typeInfoRepresentation);
    }

    public String getFullName() {
        return StringUtils.isBlank(prefix) ? String.format("`%s`", name) : String.format("%s%s.`%s`", prefix, isArrayElement ? ".element" : "", name);
    }
}
