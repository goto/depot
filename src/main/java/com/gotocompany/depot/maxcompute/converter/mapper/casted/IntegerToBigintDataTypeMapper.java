package com.gotocompany.depot.maxcompute.converter.mapper.casted;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.maxcompute.converter.mapper.ProtoPrimitiveDataTypeMapper;

import java.util.Map;
import java.util.function.Function;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.INT32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.INT64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SFIXED32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SFIXED64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT64;

/**
 * {@link ProtoPrimitiveDataTypeMapper} that widens every integer Protobuf type to MaxCompute {@code BIGINT}.
 *
 * <p>All integer families map to {@code BIGINT}. The 64-bit values pass through unchanged, while the 32-bit
 * values are upcast from {@link Integer} to {@code long}. This mapper is selected when integer-to-bigint widening
 * is enabled, in place of
 * {@link com.gotocompany.depot.maxcompute.converter.mapper.noncasted.IntegerDataTypeMapper}.</p>
 *
 * @see ProtoPrimitiveDataTypeMapper
 */
public class IntegerToBigintDataTypeMapper implements ProtoPrimitiveDataTypeMapper {

    /**
     * Returns the MaxCompute type mapping for the integer types.
     *
     * @return a map from every supported integer type to {@code BIGINT}
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(INT64, TypeInfoFactory.BIGINT)
                .put(INT32, TypeInfoFactory.BIGINT)
                .put(UINT64, TypeInfoFactory.BIGINT)
                .put(UINT32, TypeInfoFactory.BIGINT)
                .put(FIXED64, TypeInfoFactory.BIGINT)
                .put(FIXED32, TypeInfoFactory.BIGINT)
                .put(SFIXED64, TypeInfoFactory.BIGINT)
                .put(SFIXED32, TypeInfoFactory.BIGINT)
                .put(SINT64, TypeInfoFactory.BIGINT)
                .put(SINT32, TypeInfoFactory.BIGINT)
                .build();
    }

    /**
     * Returns the value-conversion mapping for the integer types.
     *
     * @return a map whose 64-bit entries pass values through unchanged and whose 32-bit entries upcast them to {@code long}
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(INT64, Function.identity())
                .put(INT32, this::upcastInteger)
                .put(UINT64, Function.identity())
                .put(UINT32, this::upcastInteger)
                .put(FIXED64, Function.identity())
                .put(FIXED32, this::upcastInteger)
                .put(SFIXED64, Function.identity())
                .put(SFIXED32, this::upcastInteger)
                .put(SINT64, Function.identity())
                .put(SINT32, this::upcastInteger)
                .build();
    }

    /**
     * Upcasts a 32-bit integer value to a {@code long}.
     *
     * @param object the value to upcast, expected to be an {@link Integer}
     * @return the value as a boxed {@link Long}
     */
    private Object upcastInteger(Object object) {
        return ((Integer) object).longValue();
    }
}
