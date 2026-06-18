package com.gotocompany.depot.maxcompute.converter.mapper.noncasted;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.maxcompute.converter.mapper.ProtoPrimitiveDataTypeMapper;

import java.util.Map;
import java.util.function.Function;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.BOOL;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.BYTES;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.ENUM;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.STRING;

/**
 * {@link ProtoPrimitiveDataTypeMapper} for the non-numeric primitive Protobuf types.
 *
 * <p>It maps {@code BYTES} to MaxCompute {@code BINARY}, both {@code STRING} and {@code ENUM} to {@code STRING},
 * and {@code BOOL} to {@code BOOLEAN}. Strings and booleans pass through unchanged, enums are rendered via their
 * string representation, and byte strings are wrapped in a MaxCompute {@link Binary}.</p>
 *
 * @see ProtoPrimitiveDataTypeMapper
 */
public class NonNumericDataTypeMapper implements ProtoPrimitiveDataTypeMapper {

    /**
     * Returns the MaxCompute type mapping for the non-numeric types.
     *
     * @return a map from {@code BYTES}, {@code STRING}, {@code ENUM}, and {@code BOOL} to their MaxCompute types
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(BYTES, TypeInfoFactory.BINARY)
                .put(STRING, TypeInfoFactory.STRING)
                .put(ENUM, TypeInfoFactory.STRING)
                .put(BOOL, TypeInfoFactory.BOOLEAN)
                .build();
    }

    /**
     * Returns the value-conversion mapping for the non-numeric types.
     *
     * @return a map from each handled type to its value converter (identity for strings and booleans, string
     *         rendering for enums, and binary wrapping for byte strings)
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(BYTES, object -> toBinaryFrom((ByteString) object))
                .put(STRING, Function.identity())
                .put(ENUM, Object::toString)
                .put(BOOL, Function.identity())
                .build();
    }

    /**
     * Wraps a protobuf {@link ByteString} into a MaxCompute {@link Binary}.
     *
     * @param object the byte string to wrap
     * @return the MaxCompute {@link Binary} holding the same bytes
     */
    private static Binary toBinaryFrom(ByteString object) {
        return new Binary(object.toByteArray());
    }

}
