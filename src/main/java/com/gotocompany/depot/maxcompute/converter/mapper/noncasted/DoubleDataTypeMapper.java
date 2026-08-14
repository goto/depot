package com.gotocompany.depot.maxcompute.converter.mapper.noncasted;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.converter.mapper.ProtoPrimitiveDataTypeMapper;

import java.util.Map;
import java.util.function.Function;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.DOUBLE;

/**
 * {@link ProtoPrimitiveDataTypeMapper} that maps the Protobuf {@code DOUBLE} type to MaxCompute {@code DOUBLE}.
 *
 * <p>The value is passed through unchanged after a finiteness check that rejects {@code NaN} and infinite values.
 * This is the default double mapper, used unless double-to-decimal conversion is enabled.</p>
 *
 * @see ProtoPrimitiveDataTypeMapper
 */
public class DoubleDataTypeMapper implements ProtoPrimitiveDataTypeMapper {

    /**
     * Returns the MaxCompute type mapping for the double type.
     *
     * @return a map from {@code DOUBLE} to the MaxCompute {@code DOUBLE} type
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(Descriptors.FieldDescriptor.Type.DOUBLE, TypeInfoFactory.DOUBLE)
                .build();
    }

    /**
     * Returns the value-conversion mapping for the double type.
     *
     * @return a map from {@code DOUBLE} to a converter that validates and returns the double value
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(DOUBLE, object -> isValid((double) object))
                .build();
    }

    /**
     * Validates that a double value is finite and returns it unchanged.
     *
     * @param value the double value to validate
     * @return the same value when it is finite
     * @throws InvalidMessageException if the value is {@code NaN} or infinite
     */
    private static double isValid(double value) {
        if (!Double.isFinite(value)) {
            throw new InvalidMessageException("Invalid double value: " + value);
        }
        return value;
    }

}
