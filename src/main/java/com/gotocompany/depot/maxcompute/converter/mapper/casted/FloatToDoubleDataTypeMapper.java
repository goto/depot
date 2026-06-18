package com.gotocompany.depot.maxcompute.converter.mapper.casted;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.converter.mapper.ProtoPrimitiveDataTypeMapper;

import java.util.Map;
import java.util.function.Function;

/**
 * {@link ProtoPrimitiveDataTypeMapper} that maps the Protobuf {@code FLOAT} type to MaxCompute {@code DOUBLE}.
 *
 * <p>Selected when float-to-double conversion is enabled. After a finiteness check, the float is widened to a
 * double via its shortest decimal string representation, which avoids the binary-rounding artifacts of a direct
 * {@code float} to {@code double} cast.</p>
 *
 * @see ProtoPrimitiveDataTypeMapper
 */
public class FloatToDoubleDataTypeMapper implements ProtoPrimitiveDataTypeMapper {

    /**
     * Returns the MaxCompute type mapping for the float type.
     *
     * @return a map from {@code FLOAT} to the MaxCompute {@code DOUBLE} type
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.of(
                Descriptors.FieldDescriptor.Type.FLOAT, TypeInfoFactory.DOUBLE
        );
    }

    /**
     * Returns the value-conversion mapping for the float type.
     *
     * @return a map from {@code FLOAT} to a converter that validates the float and widens it to a double
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.of(
                Descriptors.FieldDescriptor.Type.FLOAT, object -> isValid((float) object)
        );
    }

    /**
     * Validates that a float value is finite and widens it to a double.
     *
     * <p>The conversion goes through the float's decimal string form so that the resulting double matches the
     * value a human would read for the float, rather than its exact binary expansion.</p>
     *
     * @param value the float value to validate and widen
     * @return the value as a double
     * @throws InvalidMessageException if the value is {@code NaN} or infinite
     */
    private double isValid(float value) {
        if (!Float.isFinite(value)) {
            throw new InvalidMessageException("Invalid float value: " + value);
        }
        return Double.parseDouble(Float.toString(value));
    }
}
