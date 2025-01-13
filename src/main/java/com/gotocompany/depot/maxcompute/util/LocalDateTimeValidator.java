package com.gotocompany.depot.maxcompute.util;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;

public class LocalDateTimeValidator {

    private static final long DAYS_IN_YEAR = 365L;

    private final TemporalAmount maxPastEventTimeDifference;
    private final TemporalAmount maxFutureEventTimeDifference;
    private final ZoneId zoneId;
    private final LocalDateTime validMinTimestamp;
    private final LocalDateTime validMaxTimestamp;
    private final boolean isTablePartitioningEnabled;
    private final String tablePartitionKey;
    private final int maxPastYearEventTimeDifference;
    private final int maxFutureYearEventTimeDifference;

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
    }

    public LocalDateTime parseAndValidate(long seconds, int nanos, String fieldName, boolean isRootLevel) {
        Instant instant = Instant.now();
        ZoneOffset zoneOffset = zoneId.getRules().getOffset(instant);
        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(seconds, nanos, zoneOffset);
        validateTimestampRange(localDateTime);
        validateTimestampPartitionKey(fieldName, localDateTime, isRootLevel);
        return localDateTime;
    }

    private void validateTimestampRange(LocalDateTime localDateTime) {
        if (localDateTime.isBefore(validMinTimestamp) || localDateTime.isAfter(validMaxTimestamp)) {
            throw new InvalidMessageException(String.format("Timestamp %s is out of allowed range range min: %s max: %s",
                    localDateTime, validMinTimestamp, validMaxTimestamp));
        }
    }

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
