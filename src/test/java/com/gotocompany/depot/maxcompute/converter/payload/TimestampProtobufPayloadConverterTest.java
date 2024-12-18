package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.converter.type.TimestampProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class TimestampProtobufPayloadConverterTest {

    private final TimestampProtobufTypeInfoConverter timestampTypeInfoConverter = new TimestampProtobufTypeInfoConverter();
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final Descriptors.Descriptor repeatedDescriptor = TestMaxComputeTypeInfo.TestRootRepeated.getDescriptor();
    private TimestampProtobufPayloadConverter timestampPayloadConverter = new TimestampProtobufPayloadConverter(timestampTypeInfoConverter, Mockito.mock(MaxComputeSinkConfig.class));

    @Before
    public void setUp() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:01", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        timestampPayloadConverter = new TimestampProtobufPayloadConverter(timestampTypeInfoConverter, maxComputeSinkConfig);
    }

    @Test
    public void shouldConvertToTimestampNtz() {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(2500)
                .setNanos(100)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC);

        Object result = timestampPayloadConverter.convertSingular(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));

        assertThat(result)
                .isEqualTo(expectedLocalDateTime);
    }

    @Test
    public void shouldConvertRepeatedTimestampPayloadToTimestampList() {
        Timestamp timestamp1 = Timestamp.newBuilder()
                .setSeconds(2500)
                .setNanos(100)
                .build();
        Timestamp timestamp2 = Timestamp.newBuilder()
                .setSeconds(3600)
                .setNanos(200)
                .build();
        TestMaxComputeTypeInfo.TestRootRepeated message = TestMaxComputeTypeInfo.TestRootRepeated.newBuilder()
                .addAllTimestampFields(Arrays.asList(timestamp1, timestamp2))
                .build();
        LocalDateTime expectedLocalDateTime1 = LocalDateTime.ofEpochSecond(
                timestamp1.getSeconds(), timestamp1.getNanos(), java.time.ZoneOffset.UTC);
        LocalDateTime expectedLocalDateTime2 = LocalDateTime.ofEpochSecond(
                timestamp2.getSeconds(), timestamp2.getNanos(), java.time.ZoneOffset.UTC);

        Object result = timestampPayloadConverter.convert(new ProtoPayload(repeatedDescriptor.getFields().get(3), message.getField(repeatedDescriptor.getFields().get(3)), true));

        assertThat(result)
                .isInstanceOf(List.class);
        assertThat(((List<?>) result).stream().map(LocalDateTime.class::cast))
                .hasSize(2)
                .containsExactly(expectedLocalDateTime1, expectedLocalDateTime2);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenDateIsOutOfMinValidRange() {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(0)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC);

        Object result = timestampPayloadConverter.convertSingular(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));

        assertThat(result)
                .isEqualTo(expectedLocalDateTime);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenDateIsOutOfMaxValidRange() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        timestampPayloadConverter = new TimestampProtobufPayloadConverter(timestampTypeInfoConverter, maxComputeSinkConfig);

        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(3600 * 48)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC);

        Object result = timestampPayloadConverter.convertSingular(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));

        assertThat(result).isEqualTo(expectedLocalDateTime);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenTimeDifferenceExceedsMaxPastDuration() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(5);
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(5);
        timestampPayloadConverter = new TimestampProtobufPayloadConverter(timestampTypeInfoConverter, maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(3600)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();

        timestampPayloadConverter.convertSingular(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenTimeDifferenceExceedsMaxFutureDuration() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(5);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(1);
        timestampPayloadConverter = new TimestampProtobufPayloadConverter(timestampTypeInfoConverter, maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000 + Duration.ofDays(365 * 6).toMinutes() * 60)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();

        timestampPayloadConverter.convertSingular(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));
    }

    @Test
    public void shouldSkipDifferenceValidationWhenPartitionDisabled() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(5);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(1);
        timestampPayloadConverter = new TimestampProtobufPayloadConverter(timestampTypeInfoConverter, maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000 + Duration.ofDays(365 * 6).toMinutes() * 60)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC);

        LocalDateTime result = (LocalDateTime) timestampPayloadConverter.convertSingular(
                new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));

        assertThat(result)
                .isEqualTo(expectedLocalDateTime);
    }

    @Test
    public void shouldSkipDifferenceValidationWhenIsNotRootLevel() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(5);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(1);
        timestampPayloadConverter = new TimestampProtobufPayloadConverter(timestampTypeInfoConverter, maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000 + Duration.ofDays(365 * 6).toMinutes() * 60)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC);

        LocalDateTime result = (LocalDateTime) timestampPayloadConverter.convertSingular(new ProtoPayload(descriptor.getFields().get(3),
                message.getField(descriptor.getFields().get(3)), false));

        assertThat(result)
                .isEqualTo(expectedLocalDateTime);
    }

}
