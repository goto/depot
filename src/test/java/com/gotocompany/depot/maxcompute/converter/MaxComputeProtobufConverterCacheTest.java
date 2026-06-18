package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputeProtobufConverterCacheOuterClass;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.enumeration.MaxComputeTimestampDataType;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MaxComputeProtobufConverterCache}, which both memoizes derived MaxCompute type
 * information and resolves the concrete {@link ProtobufMaxComputeConverter} for each Protobuf field.
 *
 * <p>The cache exposes {@link MaxComputeProtobufConverterCache#getOrCreateTypeInfo(ProtoPayload)} (with and
 * without a supplier of the type),
 * {@link MaxComputeProtobufConverterCache#getConverter(Descriptors.FieldDescriptor)}, and
 * {@link MaxComputeProtobufConverterCache#clearCache()}. Tests build it in {@link #setup()} from a
 * Mockito-mocked {@link MaxComputeSinkConfig} and use the generated
 * {@code TestMaxComputeProtobufConverterCache} descriptor as the source of fields.</p>
 *
 * <p>Caching behaviour is asserted by reflecting on the private {@code typeInfoCache} map and checking its size
 * and keys (each key is composed of the nesting level and the field's full name). Converter resolution is
 * asserted by checking the runtime class returned for primitive, timestamp, duration, struct, and message
 * fields.</p>
 */
public class MaxComputeProtobufConverterCacheTest {

    /**
     * Descriptor of the generated {@code TestMaxComputeProtobufConverterCache} fixture message, providing the
     * fields whose types and converters are resolved.
     */
    private final Descriptors.Descriptor descriptor = TestMaxComputeProtobufConverterCacheOuterClass.TestMaxComputeProtobufConverterCache.getDescriptor();

    /**
     * The cache under test, rebuilt in {@link #setup()} from the mocked configuration.
     */
    private MaxComputeProtobufConverterCache maxComputeProtobufConverterCache;

    /**
     * Builds the cache under test from a Mockito-mocked {@link MaxComputeSinkConfig}.
     *
     * <p>Stubs the configuration with a five-year past and future event-time tolerance, a UTC zone, the widest
     * possible valid-timestamp range, partitioning enabled on {@code partition_key}, {@code TIMESTAMP_NTZ}
     * timestamps, and a maximum nested depth of {@code 15}, then constructs the
     * {@link MaxComputeProtobufConverterCache}.</p>
     */
    @Before
    public void setup() {
        MaxComputeSinkConfig maxComputeSinkConfig = mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(5);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(5);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.MIN);
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.MAX);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("partition_key");
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);
        maxComputeProtobufConverterCache = new MaxComputeProtobufConverterCache(maxComputeSinkConfig);
    }

    /**
     * Verifies that resolving a type for the first time populates the cache.
     *
     * <p>Reflectively confirms the {@code typeInfoCache} starts empty, calls
     * {@link MaxComputeProtobufConverterCache#getOrCreateTypeInfo(ProtoPayload)} for the {@code string_field},
     * and asserts the cache then holds a single entry keyed by the nesting level and the field's full
     * name.</p>
     *
     * @throws NoSuchFieldException   if the reflective lookup of the cache field fails
     * @throws IllegalAccessException if the reflective access to the cache field is denied
     */
    @Test
    public void shouldCreateTypeInfoIfNotExists() throws NoSuchFieldException, IllegalAccessException {
        Field cacheField = maxComputeProtobufConverterCache.getClass()
                .getDeclaredField("typeInfoCache");
        cacheField.setAccessible(true);
        Map<String, TypeInfo> typeInfoCache = ((Map<String, TypeInfo>) cacheField.get(maxComputeProtobufConverterCache));
        assertEquals(0, typeInfoCache.size());

        TypeInfo typeInfo = maxComputeProtobufConverterCache.getOrCreateTypeInfo(new ProtoPayload(descriptor.findFieldByName("string_field")));

        assertEquals(1, typeInfoCache.size());
        assertEquals(typeInfo, typeInfoCache.get(String.format("%d_%s", 0, descriptor.findFieldByName("string_field").getFullName())));
    }

    /**
     * Verifies that a second resolution of the same field is served from the cache.
     *
     * <p>Resolves the {@code string_field} type once to populate the cache, wraps the cache map in a Mockito
     * spy, resolves it again, and asserts that no additional {@code put} occurs and that the cached value is
     * returned for the level-and-full-name key.</p>
     *
     * @throws NoSuchFieldException   if the reflective lookup of the cache field fails
     * @throws IllegalAccessException if the reflective access to the cache field is denied
     */
    @Test
    public void shouldGetCachedTypeInfo() throws NoSuchFieldException, IllegalAccessException {
        Field cacheField = maxComputeProtobufConverterCache.getClass().getDeclaredField("typeInfoCache");
        cacheField.setAccessible(true);
        maxComputeProtobufConverterCache.getOrCreateTypeInfo(new ProtoPayload(descriptor.findFieldByName("string_field")));
        Map<String, TypeInfo> typeInfoCache = Mockito.spy((Map<String, TypeInfo>) cacheField.get(maxComputeProtobufConverterCache));
        assertEquals(1, typeInfoCache.size());

        TypeInfo typeInfo = maxComputeProtobufConverterCache.getOrCreateTypeInfo(new ProtoPayload(descriptor.findFieldByName("string_field")));

        assertEquals(1, typeInfoCache.size());
        verify(typeInfoCache, Mockito.times(0)).put(descriptor.findFieldByName("string_field").getFullName(), typeInfo);
        assertEquals(typeInfo, typeInfoCache.get(String.format("%d_%s", 0, descriptor.findFieldByName("string_field").getFullName())));
    }

    /**
     * Verifies that the supplier-based overload populates the cache on a miss.
     *
     * <p>Calls
     * {@link MaxComputeProtobufConverterCache#getOrCreateTypeInfo(ProtoPayload, java.util.function.Supplier)}
     * for the {@code inner_message_field} with a supplier returning a fixed struct type, then asserts the cache
     * holds a single entry keyed by the level and the field's full name.</p>
     *
     * @throws NoSuchFieldException   if the reflective lookup of the cache field fails
     * @throws IllegalAccessException if the reflective access to the cache field is denied
     */
    @Test
    public void shouldCreateTypeInfoIfNotExistsFromSupplierLogic() throws NoSuchFieldException, IllegalAccessException {
        TypeInfo expectedTypeInfo = TypeInfoFactory.getStructTypeInfo(Collections.singletonList("string_field"), Collections.singletonList(TypeInfoFactory.STRING));
        Field cacheField = maxComputeProtobufConverterCache.getClass()
                .getDeclaredField("typeInfoCache");
        cacheField.setAccessible(true);
        Map<String, TypeInfo> typeInfoCache = ((Map<String, TypeInfo>) cacheField.get(maxComputeProtobufConverterCache));
        assertEquals(0, typeInfoCache.size());

        TypeInfo typeInfo = maxComputeProtobufConverterCache.getOrCreateTypeInfo(new ProtoPayload(descriptor.findFieldByName("inner_message_field")),
                () -> expectedTypeInfo);

        assertEquals(1, typeInfoCache.size());
        assertEquals(typeInfo, typeInfoCache.get(String.format("%d_%s", 0, descriptor.findFieldByName("inner_message_field").getFullName())));
    }

    /**
     * Verifies that a second supplier-based resolution is served from the cache.
     *
     * <p>Resolves the {@code inner_message_field} type via the supplier overload twice and asserts the cache
     * still holds exactly one entry, keyed by the level and the field's full name, returning the cached
     * value.</p>
     *
     * @throws NoSuchFieldException   if the reflective lookup of the cache field fails
     * @throws IllegalAccessException if the reflective access to the cache field is denied
     */
    @Test
    public void shouldGetTypeInfoFromCacheWithSupplierLogic() throws NoSuchFieldException, IllegalAccessException {
        TypeInfo expectedTypeInfo = TypeInfoFactory.getStructTypeInfo(Collections.singletonList("string_field"), Collections.singletonList(TypeInfoFactory.STRING));
        Field cacheField = maxComputeProtobufConverterCache.getClass()
                .getDeclaredField("typeInfoCache");
        cacheField.setAccessible(true);
        Map<String, TypeInfo> typeInfoCache = ((Map<String, TypeInfo>) cacheField.get(maxComputeProtobufConverterCache));
        maxComputeProtobufConverterCache.getOrCreateTypeInfo(new ProtoPayload(descriptor.findFieldByName("inner_message_field")),
                () -> expectedTypeInfo);
        assertEquals(1, typeInfoCache.size());

        TypeInfo typeInfo = maxComputeProtobufConverterCache.getOrCreateTypeInfo(new ProtoPayload(descriptor.findFieldByName("inner_message_field")),
                () -> expectedTypeInfo);

        assertEquals(1, typeInfoCache.size());
        assertEquals(typeInfo, typeInfoCache.get(String.format("%d_%s", 0, descriptor.findFieldByName("inner_message_field").getFullName())));
    }

    /**
     * Verifies that a scalar field resolves to the primitive converter.
     *
     * <p>Calls {@link MaxComputeProtobufConverterCache#getConverter(Descriptors.FieldDescriptor)} for the
     * {@code string_field} and asserts the returned converter is a
     * {@link PrimitiveProtobufMaxComputeConverter}.</p>
     */
    @Test
    public void shouldGetConverterForPrimitive() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("string_field");

        ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fieldDescriptor);

        assertEquals(PrimitiveProtobufMaxComputeConverter.class, converter.getClass());
    }

    /**
     * Verifies that a timestamp field resolves to the timestamp converter selected by configuration.
     *
     * <p>Calls {@link MaxComputeProtobufConverterCache#getConverter(Descriptors.FieldDescriptor)} for the
     * {@code timestamp_field} and asserts the returned converter is a
     * {@link TimestampNTZProtobufMaxComputeConverter}, matching the stubbed {@code TIMESTAMP_NTZ} mapping.</p>
     */
    @Test
    public void shouldGetConverterForTimestamp() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("timestamp_field");

        ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fieldDescriptor);

        assertEquals(TimestampNTZProtobufMaxComputeConverter.class, converter.getClass());
    }

    /**
     * Verifies that a duration field resolves to the duration converter.
     *
     * <p>Calls {@link MaxComputeProtobufConverterCache#getConverter(Descriptors.FieldDescriptor)} for the
     * {@code duration_field} and asserts the returned converter is a
     * {@link DurationProtobufMaxComputeConverter}.</p>
     */
    @Test
    public void shouldGetConverterForDuration() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("duration_field");

        ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fieldDescriptor);

        assertEquals(DurationProtobufMaxComputeConverter.class, converter.getClass());
    }

    /**
     * Verifies that a struct field resolves to the struct converter.
     *
     * <p>Calls {@link MaxComputeProtobufConverterCache#getConverter(Descriptors.FieldDescriptor)} for the
     * {@code struct_field} and asserts the returned converter is a
     * {@link StructProtobufMaxComputeConverter}.</p>
     */
    @Test
    public void shouldGetConverterForStruct() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("struct_field");

        ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fieldDescriptor);

        assertEquals(StructProtobufMaxComputeConverter.class, converter.getClass());
    }

    /**
     * Verifies that a nested message field resolves to the message converter.
     *
     * <p>Calls {@link MaxComputeProtobufConverterCache#getConverter(Descriptors.FieldDescriptor)} for the
     * {@code inner_message_field} and asserts the returned converter is a
     * {@link MessageProtobufMaxComputeConverter}.</p>
     */
    @Test
    public void shouldGetConverterForMessage() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("inner_message_field");

        ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fieldDescriptor);

        assertEquals(MessageProtobufMaxComputeConverter.class, converter.getClass());
    }

    /**
     * Verifies that {@link MaxComputeProtobufConverterCache#clearCache()} empties the cached type information.
     *
     * <p>Populates the cache by resolving the {@code string_field} type, asserts it holds one entry, then calls
     * {@link MaxComputeProtobufConverterCache#clearCache()} and asserts the cache is empty.</p>
     *
     * @throws NoSuchFieldException   if the reflective lookup of the cache field fails
     * @throws IllegalAccessException if the reflective access to the cache field is denied
     */
    @Test
    public void shouldClearTypeInfoCache() throws NoSuchFieldException, IllegalAccessException {
        Field cacheField = maxComputeProtobufConverterCache.getClass()
                .getDeclaredField("typeInfoCache");
        cacheField.setAccessible(true);
        Map<String, TypeInfo> typeInfoCache = ((Map<String, TypeInfo>) cacheField.get(maxComputeProtobufConverterCache));
        maxComputeProtobufConverterCache.getOrCreateTypeInfo(new ProtoPayload(descriptor.findFieldByName("string_field")));
        assertEquals(1, typeInfoCache.size());

        maxComputeProtobufConverterCache.clearCache();

        assertEquals(0, typeInfoCache.size());
    }

}
