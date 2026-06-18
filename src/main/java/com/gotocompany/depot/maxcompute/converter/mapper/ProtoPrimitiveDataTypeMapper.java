package com.gotocompany.depot.maxcompute.converter.mapper;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;

import java.util.Map;
import java.util.function.Function;

/**
 * Strategy that contributes the type and value mappings for a group of primitive Protobuf field types.
 *
 * <p>Each implementation owns one slice of the primitive mapping problem (non-numeric types, integers, floats,
 * or doubles) and exposes two parallel tables: one mapping each handled Protobuf type to its MaxCompute
 * {@link TypeInfo}, and one mapping each handled Protobuf type to the function that converts a raw value into
 * the object MaxCompute expects. {@link ProtoPrimitiveDataTypeMapperFactory} selects the appropriate
 * implementations from configuration and merges their tables.</p>
 *
 * @see ProtoPrimitiveDataTypeMapperFactory
 */
public interface ProtoPrimitiveDataTypeMapper {

    /**
     * Returns the mapping from each handled Protobuf type to its MaxCompute {@link TypeInfo}.
     *
     * @return the Protobuf-type-to-MaxCompute-type mappings contributed by this mapper
     */
    Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap();

    /**
     * Returns the mapping from each handled Protobuf type to its value-conversion function.
     *
     * <p>Each function takes the raw parsed value and returns the object expected by the MaxCompute SDK.</p>
     *
     * @return the Protobuf-type-to-value-converter mappings contributed by this mapper
     */
    Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap();

}
