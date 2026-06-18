package com.gotocompany.depot.maxcompute.converter.mapper.noncasted;

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
 * {@link ProtoPrimitiveDataTypeMapper} that maps integer Protobuf types to their natural-width MaxCompute types.
 *
 * <p>The 64-bit families ({@code INT64}, {@code UINT64}, {@code FIXED64}, {@code SFIXED64}, {@code SINT64}) map to
 * {@code BIGINT}, and the 32-bit families ({@code INT32}, {@code UINT32}, {@code FIXED32}, {@code SFIXED32},
 * {@code SINT32}) map to {@code INT}. All values pass through unchanged. This is the default integer mapper, used
 * unless integer-to-bigint widening is enabled.</p>
 *
 * @see com.gotocompany.depot.maxcompute.converter.mapper.casted.IntegerToBigintDataTypeMapper
 * @see ProtoPrimitiveDataTypeMapper
 */
public class IntegerDataTypeMapper implements ProtoPrimitiveDataTypeMapper {

    /**
     * Returns the MaxCompute type mapping for the integer types.
     *
     * @return a map from the 64-bit integer types to {@code BIGINT} and the 32-bit integer types to {@code INT}
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(INT64, TypeInfoFactory.BIGINT)
                .put(INT32, TypeInfoFactory.INT)
                .put(UINT64, TypeInfoFactory.BIGINT)
                .put(UINT32, TypeInfoFactory.INT)
                .put(FIXED64, TypeInfoFactory.BIGINT)
                .put(FIXED32, TypeInfoFactory.INT)
                .put(SFIXED64, TypeInfoFactory.BIGINT)
                .put(SFIXED32, TypeInfoFactory.INT)
                .put(SINT64, TypeInfoFactory.BIGINT)
                .put(SINT32, TypeInfoFactory.INT)
                .build();
    }

    /**
     * Returns the value-conversion mapping for the integer types.
     *
     * @return a map from each handled integer type to an identity converter, since no value change is required
     */
    @Override
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(INT64, Function.identity())
                .put(INT32, Function.identity())
                .put(UINT64, Function.identity())
                .put(UINT32, Function.identity())
                .put(FIXED64, Function.identity())
                .put(FIXED32, Function.identity())
                .put(SFIXED64, Function.identity())
                .put(SFIXED32, Function.identity())
                .put(SINT64, Function.identity())
                .put(SINT32, Function.identity())
                .build();
    }

}
