package com.gotocompany.depot.maxcompute.converter.mapper.noncasted;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.converter.mapper.ProtoPrimitiveDataTypeMapper;

import java.util.Map;
import java.util.function.Function;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FLOAT;


/**
 * {@link ProtoPrimitiveDataTypeMapper} that maps the Protobuf {@code FLOAT} type to MaxCompute {@code FLOAT}.
 *
 * <p>The value is passed through unchanged after a finiteness check that rejects {@code NaN} and infinite values.
 * This is the default float mapper, used unless float-to-double or float-to-decimal conversion is enabled.</p>
 *
 * @see ProtoPrimitiveDataTypeMapper
 */
public class FloatDataTypeMapper implements ProtoPrimitiveDataTypeMapper {

    /**
     * Returns the MaxCompute type mapping for the float type.
     *
     * @return a map from {@code FLOAT} to the MaxCompute {@code FLOAT} type
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(FLOAT, TypeInfoFactory.FLOAT)
                .build();
    }

    /**
     * Returns the value-conversion mapping for the float type.
     *
     * @return a map from {@code FLOAT} to a converter that validates and returns the float value
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(FLOAT, object -> isValid((float) object))
                .build();
    }

    /**
     * Validates that a float value is finite and returns it unchanged.
     *
     * @param value the float value to validate
     * @return the same value when it is finite
     * @throws InvalidMessageException if the value is {@code NaN} or infinite
     */
    private static float isValid(float value) {
        if (!Float.isFinite(value)) {
            throw new InvalidMessageException("Invalid float value: " + value);
        }
        return value;
    }

}
