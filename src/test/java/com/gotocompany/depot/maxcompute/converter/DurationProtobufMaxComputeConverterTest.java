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

public class DurationProtobufMaxComputeConverterTest {

    private static final int DURATION_INDEX = 5;
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final DurationProtobufMaxComputeConverter durationProtobufMaxComputeConverter = new DurationProtobufMaxComputeConverter();
    private final Descriptors.Descriptor repeatedDescriptor = TestMaxComputeTypeInfo.TestRootRepeated.getDescriptor();

    @Test
    public void shouldConvertToStruct() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(DURATION_INDEX);

        TypeInfo typeInfo = durationProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(fieldDescriptor));

        assertEquals("STRUCT<seconds:BIGINT,nanos:BIGINT>", typeInfo.getTypeName());
    }

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
        Object result = durationProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(5), message.getField(descriptor.getFields().get(5)), true, 0));

        assertThat(result)
                .isInstanceOf(com.aliyun.odps.data.SimpleStruct.class)
                .extracting("typeInfo", "values")
                .containsExactly(TypeInfoFactory.getStructTypeInfo(expectedFieldNames, expectedTypeInfos), values);
    }

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

        Object result = durationProtobufMaxComputeConverter.convertPayload(new ProtoPayload(repeatedDescriptor.getFields().get(5), message.getField(repeatedDescriptor.getFields().get(5)), true, 0));

        assertThat(result)
                .isInstanceOf(List.class);
        assertThat((List<?>) result)
                .hasSize(2)
                .allMatch(element -> element instanceof com.aliyun.odps.data.SimpleStruct)
                .extracting("typeInfo", "values")
                .containsExactly(
                        Assertions.tuple(TypeInfoFactory.getStructTypeInfo(expectedFieldNames, expectedTypeInfos), values1),
                        Assertions.tuple(TypeInfoFactory.getStructTypeInfo(expectedFieldNames, expectedTypeInfos), values2)
                );
    }

}
