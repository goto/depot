package com.gotocompany.depot.maxcompute.converter.mapper.casted;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.converter.mapper.ProtoPrimitiveDataTypeMapper;

import java.util.Map;
import java.util.function.Function;

public class FloatToDoubleDataTypeMapper implements ProtoPrimitiveDataTypeMapper {

    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.of(
                Descriptors.FieldDescriptor.Type.FLOAT, TypeInfoFactory.DOUBLE
        );
    }

    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.of(
                Descriptors.FieldDescriptor.Type.FLOAT, object -> isValid((float) object)
        );
    }

    private double isValid(float value) {
        if (!Float.isFinite(value)) {
            throw new InvalidMessageException("Invalid float value: " + value);
        }
        return Double.parseDouble(Float.toString(value));
    }
}
