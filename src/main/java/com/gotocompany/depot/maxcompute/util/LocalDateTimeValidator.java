package com.gotocompany.depot.maxcompute.util;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Objects;

/**
 * Validates and normalises Protobuf timestamp values before they are written to MaxCompute,
 * enforcing the sink's configured acceptable time range and partition-key freshness rules.
 *
 * <p>Given the seconds and nanoseconds of a timestamp, the validator builds a {@link LocalDateTime}
 * in the configured {@link ZoneId}, optionally normalising out-of-range nanosecond components and
 * truncating to a configured precision. It then checks the value against an absolute minimum and
 * maximum timestamp and, for the configured partition key at the root level, against a sliding
 * window of allowed past and future years. Violations are surfaced as
 * {@link com.gotocompany.depot.exception.InvalidMessageException}.</p>
 *
 * <p>An instance is configured once from {@link MaxComputeSinkConfig} and is intended to be reused
 * across messages.</p>
 */
public class LocalDateTimeValidator {

    /**
     * Number of days used to approximate a year when converting the configured year-based event-time
     * windows into duration amounts.
     */
    private static final long DAYS_IN_YEAR = 365L;
    /**
     * Number of nanoseconds in one second, used to normalise nanosecond components into the
     * {@code [0, 1_000_000_000)} range.
     */
    private static final int NANOS_IN_ONE_SECOND = 1_000_000_000;

    /**
     * Maximum amount of time in the past, relative to now, that a partition-key event time may fall.
     */
    private final TemporalAmount maxPastEventTimeDifference;
    /**
     * Maximum amount of time in the future, relative to now, that a partition-key event time may fall.
     */
    private final TemporalAmount maxFutureEventTimeDifference;
    /**
     * Time zone used to interpret incoming epoch-based timestamps as local date-times.
     */
    private final ZoneId zoneId;
    /**
     * Inclusive lower bound of the absolute timestamp range accepted by the sink.
     */
    private final LocalDateTime validMinTimestamp;
    /**
     * Inclusive upper bound of the absolute timestamp range accepted by the sink.
     */
    private final LocalDateTime validMaxTimestamp;
    /**
     * Whether table partitioning is enabled; partition-key freshness checks apply only when
     * {@code true}.
     */
    private final boolean isTablePartitioningEnabled;
    /**
     * Name of the field designated as the table partition key.
     */
    private final String tablePartitionKey;
    /**
     * Configured maximum number of years in the past allowed for a partition-key event time; used in
     * error messages.
     */
    private final int maxPastYearEventTimeDifference;
    /**
     * Configured maximum number of years in the future allowed for a partition-key event time; used in
     * error messages.
     */
    private final int maxFutureYearEventTimeDifference;
    /**
     * Whether nanosecond normalisation is applied before the local date-time is constructed.
     */
    private final boolean isNanoHandlingEnabled;
    /**
     * Optional precision to which timestamps are truncated, or {@code null} to leave them untruncated.
     */
    private final ChronoUnit timestampTruncateMode;

    /**
     * Creates a validator whose thresholds and behaviour are taken from the supplied configuration.
     *
     * <p>The configured year-based past and future windows are converted to day-based durations using
     * {@link #DAYS_IN_YEAR}, and the time zone, absolute timestamp bounds, partition settings,
     * nanosecond-handling flag, and truncation mode are cached for use during validation.</p>
     *
     * @param maxComputeSinkConfig the sink configuration providing the timestamp validation thresholds
     *        and related settings
     */
    public LocalDateTimeValidator(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.maxPastEventTimeDifference = Duration.ofDays(maxComputeSinkConfig.getMaxPastYearEventTimeDifference() * DAYS_IN_YEAR);
        this.maxFutureEventTimeDifference = Duration.ofDays(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference() * DAYS_IN_YEAR);
        this.zoneId = maxComputeSinkConfig.getZoneId();
        this.validMinTimestamp = maxComputeSinkConfig.getValidMinTimestamp();
        this.validMaxTimestamp = maxComputeSinkConfig.getValidMaxTimestamp();
        this.isTablePartitioningEnabled = maxComputeSinkConfig.isTablePartitioningEnabled();
        this.tablePartitionKey = maxComputeSinkConfig.getTablePartitionKey();
        this.maxPastYearEventTimeDifference = maxComputeSinkConfig.getMaxPastYearEventTimeDifference();
        this.maxFutureYearEventTimeDifference = maxComputeSinkConfig.getMaxFutureYearEventTimeDifference();
        this.isNanoHandlingEnabled = maxComputeSinkConfig.isNanoHandlingEnabled();
        this.timestampTruncateMode = maxComputeSinkConfig.getTimestampTruncateMode();
    }

    /**
     * Builds a {@link LocalDateTime} from the given epoch seconds and nanoseconds and validates it
     * against the configured constraints.
     *
     * <p>When nanosecond handling is enabled, a negative nanosecond component is clamped to zero and a
     * component of one second or more is carried over into the seconds. The instant is then resolved
     * in the configured {@link ZoneId} and, if a truncation mode is configured, truncated to that
     * precision. The resulting value is checked against the absolute valid range and, when it is the
     * root-level partition key, against the allowed past and future event-time window.</p>
     *
     * @param seconds the epoch-seconds component of the timestamp
     * @param nanos the nanoseconds-of-second component of the timestamp
     * @param fieldName the name of the field being validated, used to detect the partition key
     * @param isRootLevel whether the field is at the root of the message, as partition-key checks
     *        apply only to root-level fields
     * @return the validated, and possibly normalised and truncated, local date-time
     * @throws com.gotocompany.depot.exception.InvalidMessageException if the timestamp is outside the
     *         configured absolute range or violates the partition-key freshness window
     */
    public LocalDateTime parseAndValidate(long seconds, int nanos, String fieldName, boolean isRootLevel) {
        if (isNanoHandlingEnabled) {
            if (nanos < 0) {
                nanos = 0;
            } else if (nanos >= NANOS_IN_ONE_SECOND) {
                seconds += nanos / NANOS_IN_ONE_SECOND;
                nanos = nanos % NANOS_IN_ONE_SECOND;
            }
        }
        Instant instant = Instant.now();
        ZoneOffset zoneOffset = zoneId.getRules().getOffset(instant);
        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(seconds, nanos, zoneOffset);
        if (Objects.nonNull(this.timestampTruncateMode)) {
            localDateTime = localDateTime.truncatedTo(this.timestampTruncateMode);
        }
        validateTimestampRange(localDateTime);
        validateTimestampPartitionKey(fieldName, localDateTime, isRootLevel);
        return localDateTime;
    }

    /**
     * Verifies that the given timestamp falls within the configured absolute minimum and maximum.
     *
     * @param localDateTime the timestamp to check
     * @throws com.gotocompany.depot.exception.InvalidMessageException if the timestamp is before the
     *         configured minimum or after the configured maximum
     */
    private void validateTimestampRange(LocalDateTime localDateTime) {
        if (localDateTime.isBefore(validMinTimestamp) || localDateTime.isAfter(validMaxTimestamp)) {
            throw new InvalidMessageException(String.format("Timestamp %s is out of allowed range range min: %s max: %s",
                    localDateTime, validMinTimestamp, validMaxTimestamp));
        }
    }

    /**
     * Enforces the past and future event-time window for the partition key.
     *
     * <p>The check is skipped entirely when partitioning is disabled, when the field is not at the
     * root level, or when the field is not the configured partition key. Otherwise the event time is
     * compared against the current time extended by the configured past and future windows.</p>
     *
     * @param fieldName the name of the field being validated
     * @param eventTime the timestamp value to check
     * @param isRootLevel whether the field is at the root level of the message
     * @throws com.gotocompany.depot.exception.InvalidMessageException if the partition-key event time
     *         is further in the past or future than the configured windows allow
     */
    private void validateTimestampPartitionKey(String fieldName, LocalDateTime eventTime, boolean isRootLevel) {
        if (!isTablePartitioningEnabled) {
            return;
        }
        if (!isRootLevel) {
            return;
        }
        if (fieldName.equals(tablePartitionKey)) {
            Instant now = Instant.now();
            Instant eventTimeInstant = eventTime.toInstant(zoneId.getRules().getOffset(now));

            if (now.minus(maxPastEventTimeDifference).isAfter(eventTimeInstant)) {
                throw new InvalidMessageException(String.format("Timestamp is in the past, you can only stream data within %d year(s) in the past", maxPastYearEventTimeDifference));
            }
            if (now.plus(maxFutureEventTimeDifference).isBefore(eventTimeInstant)) {
                throw new InvalidMessageException(String.format("Timestamp is in the future, you can only stream data within %d year(s) in the future", maxFutureYearEventTimeDifference));
            }
        }
    }
}
