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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MessageProtobufMaxComputeConverter}, which recursively maps nested Protobuf messages
 * onto MaxCompute structs.
 *
 * <p>The converter walks a message descriptor to produce a {@code STRUCT} type (and matching
 * {@code ReorderableStruct} values), delegating each field to the appropriate converter via a
 * {@link MaxComputeProtobufConverterCache}, and enforces a maximum nesting depth taken from
 * {@link MaxComputeSinkConfig}. Tests build the converter in {@link #init()} from a Mockito-mocked
 * configuration (UTC zone, a permissive timestamp range, {@code TIMESTAMP_NTZ} timestamps, and a maximum
 * nested depth of {@code 15}) and exercise it against the generated {@code TestMaxComputeTypeInfo}
 * fixtures.</p>
 *
 * <p>The suite covers the derived struct type for singular and repeated message fields, the full value
 * conversion of a deeply nested buyer/cart/item message, and the guard that rejects a non-positive nesting
 * depth.</p>
 */
public class MessageProtobufMaxComputeConverterTest {

    /**
     * The converter under test, rebuilt in {@link #init()} from the mocked configuration.
     */
    private MessageProtobufMaxComputeConverter messageProtobufMaxComputeConverter;

    /**
     * Descriptor of the {@code TestRoot} fixture message, used to resolve nested message fields by index.
     */
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();

    /**
     * Descriptor of the {@code TestBuyerWrapper} fixture message, used to drive the full struct conversion.
     */
    private final Descriptors.Descriptor payloadDescriptor = TestMaxComputeTypeInfo.TestBuyerWrapper.getDescriptor();

    /**
     * Builds the converter under test from a Mockito-mocked {@link MaxComputeSinkConfig}.
     *
     * <p>Stubs the configuration with a UTC zone, a permissive valid-timestamp range, {@code TIMESTAMP_NTZ} as
     * the Protobuf timestamp mapping, and a maximum nested message depth of {@code 15}, then constructs the
     * {@link MessageProtobufMaxComputeConverter} backed by a {@link MaxComputeProtobufConverterCache}.</p>
     */
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

    /**
     * Verifies that singular and repeated message fields are mapped to the expected struct types.
     *
     * <p>Resolves the converter type for the first (singular) and second (repeated) message fields of
     * {@code TestRoot} via {@link MessageProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)}. Asserts
     * the singular field becomes a nested
     * {@code STRUCT<string_field:STRING,another_inner_field:STRUCT<string_field:STRING>,another_inner_list_field:ARRAY<STRUCT<string_field:STRING>>>}
     * and that the repeated field becomes an {@code ARRAY} of that same struct type.</p>
     */
    @Test
    public void shouldConvertMessageToProperTypeInfo() {
        TypeInfo firstMessageFieldTypeInfo = messageProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.getFields().get(1)));
        TypeInfo secondMessageFieldTypeInfo = messageProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.getFields().get(2)));

        String expectedFirstMessageTypeRepresentation = "STRUCT<string_field:STRING,another_inner_field:STRUCT<string_field:STRING>,another_inner_list_field:ARRAY<STRUCT<string_field:STRING>>>";
        String expectedSecondMessageTypeRepresentation = String.format("ARRAY<%s>", expectedFirstMessageTypeRepresentation);

        assertEquals(expectedFirstMessageTypeRepresentation, firstMessageFieldTypeInfo.toString());
        assertEquals(expectedSecondMessageTypeRepresentation, secondMessageFieldTypeInfo.toString());
    }

    /**
     * Verifies the end-to-end value conversion of a deeply nested message into a MaxCompute struct.
     *
     * <p>Builds a {@code TestBuyerWrapper} wrapping a buyer with a cart, two items, a creation timestamp, and a
     * cart-age duration, then converts the wrapped buyer field via
     * {@link MessageProtobufMaxComputeConverter#convertPayload(ProtoPayload)}. Asserts the resulting
     * {@code ReorderableStruct} carries the expected nested struct type and that its field values match the
     * expected name, nested cart (items, timestamp, and duration struct), and top-level creation
     * timestamp.</p>
     */
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

    /**
     * Verifies that constructing the converter with a non-positive maximum nesting depth is rejected.
     *
     * <p>Stubs the configuration to report a maximum nested message depth of {@code 0} and expects the
     * {@link MessageProtobufMaxComputeConverter} constructor to throw an {@link IllegalArgumentException}.</p>
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenMaxNestedMessageDepthIsLessThanOne() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(0);

        new MessageProtobufMaxComputeConverter(new MaxComputeProtobufConverterCache(maxComputeSinkConfig), maxComputeSinkConfig);
    }

}
