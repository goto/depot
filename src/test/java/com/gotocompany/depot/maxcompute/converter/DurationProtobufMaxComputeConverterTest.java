package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link DurationProtobufMaxComputeConverter}, which maps the well-known Protobuf
 * {@code google.protobuf.Duration} type onto a MaxCompute struct.
 *
 * <p>A duration is represented in MaxCompute as a {@code STRUCT<seconds:BIGINT,nanos:BIGINT>}. The suite uses
 * the {@code TestRoot} and {@code TestRootRepeated} descriptors from the generated
 * {@code TestMaxComputeTypeInfo} fixtures to drive the converter, and asserts on both the derived
 * {@link TypeInfo} and the converted struct values for singular and repeated duration fields. Each test uses a
 * real converter instance with no mocking.</p>
 */
public class DurationProtobufMaxComputeConverterTest {

    /**
     * Index of the singular {@code duration_field} within the {@code TestRoot} descriptor.
     */
    private static final int DURATION_INDEX = 5;

    /**
     * Descriptor of the {@code TestRoot} fixture message, used to resolve the singular duration field.
     */
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();

    /**
     * The converter under test.
     */
    private final DurationProtobufMaxComputeConverter durationProtobufMaxComputeConverter = new DurationProtobufMaxComputeConverter();

    /**
     * Descriptor of the {@code TestRootRepeated} fixture message, used to resolve the repeated duration field.
     */
    private final Descriptors.Descriptor repeatedDescriptor = TestMaxComputeTypeInfo.TestRootRepeated.getDescriptor();

    /**
     * Verifies that the converter derives a {@code STRUCT<seconds:BIGINT,nanos:BIGINT>} type for a duration
     * field.
     *
     * <p>Resolves the duration field descriptor and asserts that
     * {@link DurationProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} produces a struct type whose
     * type name is {@code STRUCT<seconds:BIGINT,nanos:BIGINT>}.</p>
     */
    @Test
    public void shouldConvertToStruct() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(DURATION_INDEX);

        TypeInfo typeInfo = durationProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(fieldDescriptor));

        assertEquals("STRUCT<seconds:BIGINT,nanos:BIGINT>", typeInfo.getTypeName());
    }

    /**
     * Verifies that a singular duration value is converted to a MaxCompute struct.
     *
     * <p>Builds a {@code TestRoot} message carrying a duration of one second and one nanosecond, converts the
     * field via {@link DurationProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the
     * result is a {@code ReorderableStruct} with type {@code STRUCT<seconds:BIGINT,nanos:BIGINT>} and values
     * {@code [1L, 1L]}.</p>
     */
    @Test
    public void shouldConvertDurationPayloadToStruct() {
        Duration duration = Duration.newBuilder()
                .setSeconds(1)
                .setNanos(1)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setDurationField(duration)
                .build();
        List<String> expectedFieldNames = Arrays.asList("seconds", "nanos");
        List<TypeInfo> expectedTypeInfos = Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT);
        List<Object> values = Arrays.asList(1L, 1L);
        Object result = durationProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(5), message.getField(descriptor.getFields().get(5)), 0));

        assertThat(result)
                .isInstanceOf(com.aliyun.odps.data.ReorderableStruct.class)
                .extracting("typeInfo", "values")
                .containsExactly(TypeInfoFactory.getStructTypeInfo(expectedFieldNames, expectedTypeInfos), values);
    }

    /**
     * Verifies that a repeated duration field is converted to a list of MaxCompute structs.
     *
     * <p>Builds a {@code TestRootRepeated} message with two durations, converts the repeated field via
     * {@link DurationProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * two-element {@link java.util.List} whose entries are {@code ReorderableStruct}s holding the values
     * {@code [1L, 1L]} and {@code [2L, 2L]} respectively.</p>
     */
    @Test
    public void shouldConvertRepeatedDurationPayloadToStructList() {
        Duration duration1 = Duration.newBuilder()
                .setSeconds(1)
                .setNanos(1)
                .build();
        Duration duration2 = Duration.newBuilder()
                .setSeconds(2)
                .setNanos(2)
                .build();
        TestMaxComputeTypeInfo.TestRootRepeated message = TestMaxComputeTypeInfo.TestRootRepeated.newBuilder()
                .addAllDurationFields(Arrays.asList(duration1, duration2))
                .build();
        List<String> expectedFieldNames = Arrays.asList("seconds", "nanos");
        List<TypeInfo> expectedTypeInfos = Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT);
        List<Object> values1 = Arrays.asList(1L, 1L);
        List<Object> values2 = Arrays.asList(2L, 2L);

        Object result = durationProtobufMaxComputeConverter.convertPayload(new ProtoPayload(repeatedDescriptor.getFields().get(5), message.getField(repeatedDescriptor.getFields().get(5)), 0));

        assertThat(result)
                .isInstanceOf(List.class);
        assertThat((List<?>) result)
                .hasSize(2)
                .allMatch(element -> element instanceof com.aliyun.odps.data.ReorderableStruct)
                .extracting("typeInfo", "values")
                .containsExactly(
                        Assertions.tuple(TypeInfoFactory.getStructTypeInfo(expectedFieldNames, expectedTypeInfos), values1),
                        Assertions.tuple(TypeInfoFactory.getStructTypeInfo(expectedFieldNames, expectedTypeInfos), values2)
                );
    }

}
