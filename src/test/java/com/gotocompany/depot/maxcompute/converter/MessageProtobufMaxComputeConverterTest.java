package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.ReorderableStruct;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.enumeration.MaxComputeTimestampDataType;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class MessageProtobufMaxComputeConverterTest {

    private MessageProtobufMaxComputeConverter messageProtobufMaxComputeConverter;
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final Descriptors.Descriptor payloadDescriptor = TestMaxComputeTypeInfo.TestBuyerWrapper.getDescriptor();

    @Before
    public void init() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        messageProtobufMaxComputeConverter = new MessageProtobufMaxComputeConverter(new MaxComputeProtobufConverterCache(maxComputeSinkConfig), maxComputeSinkConfig);
    }

    @Test
    public void shouldConvertMessageToProperTypeInfo() {
        TypeInfo firstMessageFieldTypeInfo = messageProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.getFields().get(1)));
        TypeInfo secondMessageFieldTypeInfo = messageProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.getFields().get(2)));

        String expectedFirstMessageTypeRepresentation = "STRUCT<string_field:STRING,another_inner_field:STRUCT<string_field:STRING>,another_inner_list_field:ARRAY<STRUCT<string_field:STRING>>>";
        String expectedSecondMessageTypeRepresentation = String.format("ARRAY<%s>", expectedFirstMessageTypeRepresentation);

        assertEquals(expectedFirstMessageTypeRepresentation, firstMessageFieldTypeInfo.toString());
        assertEquals(expectedSecondMessageTypeRepresentation, secondMessageFieldTypeInfo.toString());
    }

    @Test
    public void shouldConvertToStruct() {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(1704067200)
                .setNanos(0)
                .build();
        Duration duration = Duration.newBuilder()
                .setSeconds(100)
                .build();
        TestMaxComputeTypeInfo.TestBuyer message = TestMaxComputeTypeInfo.TestBuyer.newBuilder()
                .setName("buyerName")
                .setCart(TestMaxComputeTypeInfo.TestCart.newBuilder()
                        .setCartId("cart_id")
                        .addAllItems(Arrays.asList(
                                TestMaxComputeTypeInfo.TestItem.newBuilder()
                                        .setId("item1")
                                        .setQuantity(1)
                                        .build(),
                                TestMaxComputeTypeInfo.TestItem.newBuilder()
                                        .setId("item2")
                                        .build()))
                        .setCreatedAt(timestamp)
                        .setCartAge(duration)
                )
                .setCreatedAt(timestamp)
                .build();
        TestMaxComputeTypeInfo.TestBuyerWrapper wrapper = TestMaxComputeTypeInfo.TestBuyerWrapper
                .newBuilder()
                .setBuyer(message)
                .build();
        StructTypeInfo durationTypeInfo = TypeInfoFactory.getStructTypeInfo(Arrays.asList("seconds", "nanos"), Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT));
        StructTypeInfo itemTypeInfo = TypeInfoFactory.getStructTypeInfo(Arrays.asList("id", "quantity", "type", "empty_holder"),
                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.INT, TypeInfoFactory.STRING, TypeInfoFactory.getStructTypeInfo(Collections.singletonList("id"), Collections.singletonList(TypeInfoFactory.STRING))));
        StructTypeInfo cartTypeInfo = TypeInfoFactory.getStructTypeInfo(
                Arrays.asList("cart_id", "items", "created_at", "cart_age"),
                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.getArrayTypeInfo(itemTypeInfo), TypeInfoFactory.TIMESTAMP_NTZ, durationTypeInfo)
        );
        StructTypeInfo expectedStructTypeInfo = TypeInfoFactory.getStructTypeInfo(
                Arrays.asList("name", "cart", "created_at"),
                Arrays.asList(TypeInfoFactory.STRING, cartTypeInfo, TypeInfoFactory.TIMESTAMP_NTZ)
        );
        List<Object> expectedStructValues = Arrays.asList(
                "buyerName",
                new ReorderableStruct(cartTypeInfo,
                        Arrays.asList(
                                "cart_id",
                                Arrays.asList(new SimpleStruct(itemTypeInfo, Arrays.asList("item1", 1, "TEST_1", null)), new SimpleStruct(itemTypeInfo, Arrays.asList("item2", 0, "TEST_1", null))),
                                LocalDateTime.ofEpochSecond(timestamp.getSeconds(), 0, java.time.ZoneOffset.UTC),
                                new SimpleStruct(durationTypeInfo, Arrays.asList(duration.getSeconds(), ((Integer) duration.getNanos()).longValue())))),
                LocalDateTime.ofEpochSecond(timestamp.getSeconds(), 0, java.time.ZoneOffset.UTC)
        );

        ReorderableStruct result =  (ReorderableStruct) messageProtobufMaxComputeConverter.convertPayload(new ProtoPayload(payloadDescriptor.getFields().get(0), wrapper.getField(payloadDescriptor.getFields().get(0)), 0));

        assertEquals(expectedStructTypeInfo, result.getTypeInfo());
        assertEquals(expectedStructValues.toString(), result.getFieldValues().toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenMaxNestedMessageDepthIsLessThanOne() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(0);

        new MessageProtobufMaxComputeConverter(new MaxComputeProtobufConverterCache(maxComputeSinkConfig), maxComputeSinkConfig);
    }

}
