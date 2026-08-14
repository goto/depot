package com.gotocompany.depot.maxcompute.converter.mapper;

import com.aliyun.odps.type.TypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.mapper.casted.DoubleToDecimalDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.casted.FloatToDecimalDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.casted.FloatToDoubleDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.casted.IntegerToBigintDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.noncasted.DoubleDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.noncasted.FloatDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.noncasted.IntegerDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.noncasted.NonNumericDataTypeMapper;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Builds and combines the {@link ProtoPrimitiveDataTypeMapper}s that define how primitive Protobuf types map to
 * MaxCompute types and values.
 *
 * <p>It selects a concrete mapper for each numeric family based on configuration, and always uses the
 * {@link NonNumericDataTypeMapper} for non-numeric types. The selection rules are:</p>
 * <ul>
 *     <li>integers map to {@code BIGINT} ({@link IntegerToBigintDataTypeMapper}) when integer-to-bigint is
 *     enabled, otherwise they keep their natural widths ({@link IntegerDataTypeMapper});</li>
 *     <li>doubles map to {@code DECIMAL} ({@link DoubleToDecimalDataTypeMapper}) when double-to-decimal is
 *     enabled, otherwise to {@code DOUBLE} ({@link DoubleDataTypeMapper});</li>
 *     <li>floats map to {@code DOUBLE} ({@link FloatToDoubleDataTypeMapper}) when float-to-double is enabled;
 *     otherwise to {@code DECIMAL} ({@link FloatToDecimalDataTypeMapper}) when float-to-decimal is enabled, or to
 *     {@code FLOAT} ({@link FloatDataTypeMapper}) by default.</li>
 * </ul>
 *
 * <p>{@link #getProtoTypeMap()} and {@link #getProtoPayloadMapperMap()} merge the per-family tables into the
 * unified maps consumed by
 * {@link com.gotocompany.depot.maxcompute.converter.PrimitiveProtobufMaxComputeConverter}.</p>
 *
 * @see ProtoPrimitiveDataTypeMapper
 */
@RequiredArgsConstructor
public class ProtoPrimitiveDataTypeMapperFactory {

    /**
     * Mapper for non-numeric primitive types (bytes, string, enum, and boolean).
     */
    private final ProtoPrimitiveDataTypeMapper nonNumericDataTypeMapper;
    /**
     * Mapper selected for the integer-family types, varying by the integer-to-bigint configuration.
     */
    private final ProtoPrimitiveDataTypeMapper integerProtoPrimitiveDataTypeMapper;
    /**
     * Mapper selected for the float type, varying by the float-to-double and float-to-decimal configuration.
     */
    private final ProtoPrimitiveDataTypeMapper floatProtoPrimitiveDataTypeMapper;
    /**
     * Mapper selected for the double type, varying by the double-to-decimal configuration.
     */
    private final ProtoPrimitiveDataTypeMapper doubleProtoPrimitiveDataTypeMapper;

    /**
     * Selects the per-family mappers from configuration.
     *
     * @param maxComputeSinkConfig the sink configuration whose feature flags choose the integer, float, and
     *                             double mappers
     */
    public ProtoPrimitiveDataTypeMapperFactory(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.nonNumericDataTypeMapper = new NonNumericDataTypeMapper();
        this.integerProtoPrimitiveDataTypeMapper = maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled() ? new IntegerToBigintDataTypeMapper() : new IntegerDataTypeMapper();
        this.doubleProtoPrimitiveDataTypeMapper = maxComputeSinkConfig.isProtoDoubleToDecimalEnabled() ? new DoubleToDecimalDataTypeMapper(maxComputeSinkConfig) : new DoubleDataTypeMapper();
        if (maxComputeSinkConfig.isProtoFloatTypeToDoubleEnabled()) {
            this.floatProtoPrimitiveDataTypeMapper = new FloatToDoubleDataTypeMapper();
        } else {
            this.floatProtoPrimitiveDataTypeMapper = maxComputeSinkConfig.isProtoFloatTypeToDecimalEnabled() ? new FloatToDecimalDataTypeMapper(maxComputeSinkConfig) : new FloatDataTypeMapper();
        }
    }

    /**
     * Returns the merged Protobuf-type-to-MaxCompute-type mapping across all primitive families.
     *
     * @return the unified type mapping built from the non-numeric, integer, float, and double mappers
     */
    public Map<Descriptors.FieldDescriptor.Type, TypeInfo> getProtoTypeMap() {
        return mergeMaps(ImmutableList.of(
                nonNumericDataTypeMapper.getProtoTypeMap(),
                integerProtoPrimitiveDataTypeMapper.getProtoTypeMap(),
                floatProtoPrimitiveDataTypeMapper.getProtoTypeMap(),
                doubleProtoPrimitiveDataTypeMapper.getProtoTypeMap()
        ));
    }

    /**
     * Returns the merged Protobuf-type-to-value-converter mapping across all primitive families.
     *
     * @return the unified value-conversion mapping built from the non-numeric, integer, float, and double mappers
     */
    public Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> getProtoPayloadMapperMap() {
        return mergeMaps(ImmutableList.of(
                nonNumericDataTypeMapper.getProtoPayloadMapperMap(),
                integerProtoPrimitiveDataTypeMapper.getProtoPayloadMapperMap(),
                floatProtoPrimitiveDataTypeMapper.getProtoPayloadMapperMap(),
                doubleProtoPrimitiveDataTypeMapper.getProtoPayloadMapperMap()
        ));
    }

    /**
     * Merges several maps into a single immutable map.
     *
     * @param <K>  the key type of the maps
     * @param <V>  the value type of the maps
     * @param maps the maps to merge, applied in order
     * @return an immutable map containing all entries of the supplied maps
     * @throws IllegalArgumentException if the supplied maps contain duplicate keys
     */
    private static <K, V> Map<K, V> mergeMaps(List<Map<K, V>> maps) {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (Map<K, V> map : maps) {
            builder.putAll(map);
        }
        return builder.build();
    }

}
