package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.enumeration.MaxComputeTimestampDataType;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TimestampProtobufMaxComputeConverter}, which maps the well-known Protobuf
 * {@code google.protobuf.Timestamp} type onto a MaxCompute {@code TIMESTAMP} (a {@link java.sql.Timestamp}).
 *
 * <p>This is the zoned counterpart of the {@code TIMESTAMP_NTZ} converter and applies the same configurable
 * policies sourced from {@link MaxComputeSinkConfig}:</p>
 * <ul>
 *     <li>a valid minimum and maximum timestamp range;</li>
 *     <li>maximum allowed past and future event-time differences, enforced only for the partition key at the
 *     root nesting level;</li>
 *     <li>optional normalization of out-of-range or negative nanosecond components;</li>
 *     <li>optionally mapping negative-epoch-second timestamps to {@code null};</li>
 *     <li>optional truncation to a configured {@link ChronoUnit}.</li>
 * </ul>
 *
 * <p>A baseline converter is created in {@link #setUp()}; individual tests that need different policies build a
 * fresh converter from their own Mockito-mocked configuration. Inputs are driven through the {@code TestRoot}
 * and {@code TestRootRepeated} fixtures, and assertions cover both successful conversions and the exceptions
 * raised for invalid input.</p>
 */
public class TimestampProtobufMaxComputeConverterTest {

    /**
     * Index of the singular {@code timestamp_field} within the {@code TestRoot} descriptor.
     */
    private static final int TIMESTAMP_INDEX = 3;

    /**
     * Descriptor of the {@code TestRoot} fixture message, used to resolve the singular timestamp field.
     */
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();

    /**
     * Descriptor of the {@code TestRootRepeated} fixture message, used to resolve the repeated timestamp field.
     */
    private final Descriptors.Descriptor repeatedDescriptor = TestMaxComputeTypeInfo.TestRootRepeated.getDescriptor();

    /**
     * The converter under test, initialised in {@link #setUp()} and rebuilt by tests that need other policies.
     */
    private TimestampProtobufMaxComputeConverter timestampProtobufMaxComputeConverter;

    /**
     * Builds the baseline converter under test from a Mockito-mocked {@link MaxComputeSinkConfig}.
     *
     * <p>Stubs a UTC zone, a valid-timestamp range from {@code 1970-01-01T00:00:01} to
     * {@code 9999-01-01T23:59:59}, partitioning enabled on {@code timestamp_field}, generous past and future
     * event-time tolerances, {@code TIMESTAMP} as the Protobuf timestamp mapping, and disabled negative-second
     * handling. Tests requiring different policies replace this converter with one built from their own
     * configuration.</p>
     */
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
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP);
        when(maxComputeSinkConfig.isIgnoreNegativeSecondTimestampEnabled()).thenReturn(false);
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
    }

    /**
     * Verifies that the converter derives a {@code TIMESTAMP} type for a timestamp field.
     *
     * <p>Resolves the timestamp field via
     * {@link TimestampProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} and asserts it equals
     * {@code TypeInfoFactory.TIMESTAMP}.</p>
     */
    @Test
    public void shouldConvertToTimestampNtz() {
        TypeInfo typeInfo = timestampProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.getFields().get(TIMESTAMP_INDEX)));

        assertEquals(TypeInfoFactory.TIMESTAMP, typeInfo);
    }

    /**
     * Verifies that a singular timestamp value is converted to the corresponding {@link java.sql.Timestamp}.
     *
     * <p>Builds a {@code TestRoot} carrying a timestamp of {@code 2500} seconds and {@code 100} nanos, converts
     * it via {@link TimestampProtobufMaxComputeConverter#convertSingularPayload(ProtoPayload)}, and asserts the
     * result equals the {@code java.sql.Timestamp} for that UTC instant.</p>
     */
    @Test
    public void shouldConvertPayloadToTimestampNtz() {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(2500)
                .setNanos(100)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        java.sql.Timestamp expectedTimestamp = java.sql.Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC));

        Object result = timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));

        assertThat(result)
                .isEqualTo(expectedTimestamp);
    }

    /**
     * Verifies that a repeated timestamp field is converted to a list of {@link java.sql.Timestamp} values.
     *
     * <p>Builds a {@code TestRootRepeated} with two timestamps, converts the repeated field via
     * {@link TimestampProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * two-element {@link java.util.List} containing the expected {@code java.sql.Timestamp} values in
     * order.</p>
     */
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
        java.sql.Timestamp expectedLocalDateTime1 = java.sql.Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                timestamp1.getSeconds(), timestamp1.getNanos(), java.time.ZoneOffset.UTC));
        java.sql.Timestamp expectedLocalDateTime2 = java.sql.Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                timestamp2.getSeconds(), timestamp2.getNanos(), java.time.ZoneOffset.UTC));

        Object result = timestampProtobufMaxComputeConverter.convertPayload(new ProtoPayload(repeatedDescriptor.getFields().get(3), message.getField(repeatedDescriptor.getFields().get(3)), 0));

        assertThat(result)
                .isInstanceOf(List.class);
        assertThat(((List<?>) result).stream().map(java.sql.Timestamp.class::cast))
                .hasSize(2)
                .containsExactly(expectedLocalDateTime1, expectedLocalDateTime2);
    }

    /**
     * Verifies that a timestamp below the configured minimum is rejected.
     *
     * <p>With the baseline minimum of {@code 1970-01-01T00:00:01}, converts a timestamp at epoch second
     * {@code 0} and expects an {@link InvalidMessageException}.</p>
     */
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

        timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));
    }

    /**
     * Verifies that a timestamp above the configured maximum is rejected.
     *
     * <p>Rebuilds the converter with a maximum of {@code 1970-01-01T23:59:59}, converts a timestamp
     * {@code 48} hours past the epoch, and expects an {@link InvalidMessageException}.</p>
     */
    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenDateIsOutOfMaxValidRange() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);

        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(3600 * 48)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();

        timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));
    }

    /**
     * Verifies that a timestamp older than the maximum past event-time difference is rejected.
     *
     * <p>Rebuilds the converter with partitioning enabled and a five-year past tolerance, converts a timestamp
     * far in the past relative to now, and expects an {@link InvalidMessageException}.</p>
     */
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
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(3600)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();

        timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));
    }

    /**
     * Verifies that a timestamp beyond the maximum future event-time difference is rejected.
     *
     * <p>Rebuilds the converter with partitioning enabled and a one-year future tolerance, converts a
     * timestamp roughly six years in the future, and expects an {@link InvalidMessageException}.</p>
     */
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
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000 + Duration.ofDays(365 * 6).toMinutes() * 60)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();

        timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));
    }

    /**
     * Verifies that the event-time difference validation is skipped when partitioning is disabled.
     *
     * <p>Rebuilds the converter with partitioning disabled and a one-year future tolerance, converts a
     * timestamp roughly six years in the future, and asserts it is converted to the expected
     * {@link java.sql.Timestamp} rather than rejected.</p>
     */
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
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000 + Duration.ofDays(365 * 6).toMinutes() * 60)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        java.sql.Timestamp expectedTimestamp = java.sql.Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC));

        java.sql.Timestamp result = (java.sql.Timestamp) timestampProtobufMaxComputeConverter.convertSingularPayload(
                new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));

        assertThat(result)
                .isEqualTo(expectedTimestamp);
    }

    /**
     * Verifies that the event-time difference validation is skipped for non-root nesting levels.
     *
     * <p>Rebuilds the converter with partitioning enabled and a one-year future tolerance, converts a
     * far-future timestamp at nesting level {@code 1} (not the root), and asserts it is converted to the
     * expected {@link java.sql.Timestamp} rather than rejected.</p>
     */
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
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000 + Duration.ofDays(365 * 6).toMinutes() * 60)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        java.sql.Timestamp expectedTimestamp = java.sql.Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC));

        java.sql.Timestamp result = (java.sql.Timestamp) timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3),
                message.getField(descriptor.getFields().get(3)), 1));

        assertThat(result).isEqualTo(expectedTimestamp);
    }

    /**
     * Verifies that an out-of-range nanosecond component is carried into the seconds when nano handling is
     * enabled.
     *
     * <p>Rebuilds the converter with nano handling enabled, converts a timestamp of {@code 2500} seconds and
     * {@code 1500000000} nanos, and asserts the excess nanoseconds roll over so the result equals the
     * {@link java.sql.Timestamp} at {@code 2501} seconds and {@code 500000000} nanos.</p>
     */
    @Test
    public void shouldConvertExcessNanosToSecondsWhenEnabled() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isNanoHandlingEnabled()).thenReturn(true);
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);

        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(2500)
                .setNanos(1500000000)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();

        java.sql.Timestamp expectedTimestamp = java.sql.Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                timestamp.getSeconds() + 1, 500000000, ZoneOffset.UTC));
        Object result = timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));
        assertThat(result).isEqualTo(expectedTimestamp);
    }

    /**
     * Verifies that an out-of-range nanosecond component is rejected when nano handling is disabled.
     *
     * <p>Rebuilds the converter with nano handling disabled, converts a timestamp whose nanosecond component
     * is {@code 1000000000} (a full second), and expects a {@link java.time.DateTimeException}.</p>
     */
    @Test(expected = java.time.DateTimeException.class)
    public void shouldThrowExceptionForExcessNanosWhenDisabled() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isNanoHandlingEnabled()).thenReturn(false);
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);

        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(2500)
                .setNanos(1000000000)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();

        timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));
    }

    /**
     * Verifies that a negative nanosecond component is rejected when nano handling is disabled.
     *
     * <p>Rebuilds the converter with nano handling disabled, converts a timestamp of {@code 2500} seconds and
     * {@code -1} nanos, and expects a {@link java.time.DateTimeException}.</p>
     */
    @Test(expected = java.time.DateTimeException.class)
    public void shouldThrowExceptionForNegativeNanosWhenDisabled() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isNanoHandlingEnabled()).thenReturn(false);
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);

        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(2500)
                .setNanos(-1)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();

        timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));
    }

    /**
     * Verifies that a negative nanosecond component is normalized when nano handling is enabled.
     *
     * <p>Rebuilds the converter with nano handling enabled, converts a timestamp of {@code 2500} seconds and
     * {@code -500000000} nanos, and asserts the negative nanos are normalized so the result equals the
     * {@link java.sql.Timestamp} at {@code 2500} seconds with zero nanoseconds.</p>
     */
    @Test
    public void shouldHandleNegativeNanosWhenEnabled() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isNanoHandlingEnabled()).thenReturn(true);
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);

        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(2500)
                .setNanos(-500000000)  // Negative nanoseconds
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();

        java.sql.Timestamp expectedTimestamp = java.sql.Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), 0, ZoneOffset.UTC));

        Object result = timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));

        assertThat(result).isEqualTo(expectedTimestamp);
    }

    /**
     * Verifies that a timestamp with an in-range nanosecond component is converted directly.
     *
     * <p>Converts a timestamp of {@code 2500} seconds and {@code 500000000} nanos with the baseline converter
     * and asserts the result equals the expected {@link java.sql.Timestamp}.</p>
     */
    @Test
    public void shouldConvertPayloadWithValidNanos() {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(2500)
                .setNanos(500000000)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        java.sql.Timestamp expectedTimestamp = java.sql.Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), ZoneOffset.UTC));

        Object result = timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), 0));
        assertThat(result).isEqualTo(expectedTimestamp);
    }

    /**
     * Verifies that a negative-epoch timestamp is converted to {@code null} when the corresponding flag is
     * enabled.
     *
     * <p>Rebuilds the converter with ignore-negative-second handling enabled, converts a timestamp at
     * {@code -1000} seconds, and asserts the result is {@code null}.</p>
     */
    @Test
    public void shouldConvertPayloadToNullWhenSecondsIsNegativeAndFlagIsTrue() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:01", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.isIgnoreNegativeSecondTimestampEnabled()).thenReturn(true);
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(-1000L)
                .setNanos(2)
                .build();

        Object result = timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), timestamp, 0));

        assertThat(result).isNull();
    }

    /**
     * Verifies that a negative-epoch timestamp is converted normally when the ignore flag is disabled.
     *
     * <p>Rebuilds the converter with ignore-negative-second handling disabled and a minimum of
     * {@code 0001-01-01T00:00:01}, converts a timestamp at {@code -1000} seconds and {@code 2} nanos, and
     * asserts the result equals the corresponding {@link java.sql.Timestamp}.</p>
     */
    @Test
    public void shouldConvertPayloadToTimestampWhenSecondsIsNegativeAndFlagIsFalse() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("0001-01-01T00:00:01", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.isIgnoreNegativeSecondTimestampEnabled()).thenReturn(false);
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(-1000L)
                .setNanos(2)
                .build();
        java.sql.Timestamp expectedTimestamp = java.sql.Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                -1000L, 2, ZoneOffset.UTC));

        Object result = timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), timestamp, 0));

        assertThat(result).isEqualTo(expectedTimestamp);
    }

    /**
     * Verifies that the converted timestamp is truncated to the configured unit.
     *
     * <p>Rebuilds the converter with a truncation mode of {@link ChronoUnit#MICROS}, converts a timestamp of
     * {@code 2500} seconds and {@code 123123123} nanos, and asserts the result equals the corresponding
     * {@link java.sql.Timestamp} truncated to microseconds.</p>
     */
    @Test
    public void shouldConvertPayloadToTimestampTruncatedToSelectedUnit() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("0001-01-01T00:00:01", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.isIgnoreNegativeSecondTimestampEnabled()).thenReturn(false);
        when(maxComputeSinkConfig.getTimestampTruncateMode()).thenReturn(ChronoUnit.MICROS);
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(2500)
                .setNanos(123123123)
                .build();
        java.sql.Timestamp expectedTimestamp = java.sql.Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                        timestamp.getSeconds(), timestamp.getNanos(), ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.MICROS));

        Object result = timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), timestamp, 0));

        assertThat(result).isEqualTo(expectedTimestamp);
    }
}
