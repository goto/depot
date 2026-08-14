package com.gotocompany.depot.bigquery.storage.proto;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.BigQuerySinkConfig;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Utility for converting {@link Instant} values into BigQuery-compatible timestamps and enforcing the
 * range constraints imposed by BigQuery.
 *
 * <p>BigQuery stores timestamps as microseconds since the Unix epoch, and the Storage Write API
 * applies stricter bounds to columns used for time partitioning than to ordinary timestamp columns.
 * This helper performs the unit conversion and validates that an instant falls within the appropriate
 * allowed window before it is written.</p>
 */
public class TimeStampUtils {
    /** Number of days (1825, i.e. five years) a partition timestamp may lie in the past. */
    private static final long FIVE_YEARS_DAYS = 1825;
    /** Number of days (365, i.e. one year) a partition timestamp may lie in the future. */
    private static final long ONE_YEAR_DAYS = 365;
    /** Inclusive lower bound BigQuery accepts for non-partition timestamp columns. */
    private static final Instant MIN_TIMESTAMP = Instant.parse("0001-01-01T00:00:00Z");
    /** Inclusive upper bound BigQuery accepts for non-partition timestamp columns. */
    private static final Instant MAX_TIMESTAMP = Instant.parse("9999-12-31T23:59:59.999999Z");

    /**
     * Converts an instant to BigQuery microseconds and validates it against the relevant range limits.
     *
     * <p>The instant is first converted to microseconds since the epoch. The validation that follows
     * depends on whether the field is the configured top-level partition key:</p>
     * <ul>
     *     <li>For the partition column (top level and matching
     *     {@code config.getTablePartitionKey()}), the value must fall within 1825 days in the past and
     *     365 days in the future relative to the current instant.</li>
     *     <li>For any other timestamp field, the value must lie strictly between {@link #MIN_TIMESTAMP}
     *     and {@link #MAX_TIMESTAMP}.</li>
     * </ul>
     *
     * @param instant         the timestamp value to convert and validate
     * @param fieldDescriptor the descriptor of the destination field, used for partition-key matching
     *                        and error messages
     * @param isTopLevel      {@code true} if the field is at the top level of the message, which is a
     *                        prerequisite for being treated as the partition column
     * @param config          the sink configuration providing the configured table partition key
     * @return the timestamp expressed in microseconds since the Unix epoch
     * @throws UnsupportedOperationException if the value lies outside the allowed partition window
     *                                       (for the partition column) or outside the BigQuery
     *                                       timestamp bounds (for other columns)
     */
    public static long getBQInstant(Instant instant, Descriptors.FieldDescriptor fieldDescriptor, boolean isTopLevel, BigQuerySinkConfig config) {
        // Timestamp should be in microseconds
        long timeStamp = TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
        // Partition column is always top level
        if (isTopLevel && fieldDescriptor.getName().equals(config.getTablePartitionKey())) {
            Instant currentInstant = Instant.now();
            boolean isValid;
            boolean isPastInstant = currentInstant.isAfter(instant);
            if (isPastInstant) {
                Instant fiveYearPast = currentInstant.minusMillis(TimeUnit.DAYS.toMillis(FIVE_YEARS_DAYS));
                isValid = fiveYearPast.isBefore(instant);
            } else {
                Instant oneYearFuture = currentInstant.plusMillis(TimeUnit.DAYS.toMillis(ONE_YEAR_DAYS));
                isValid = oneYearFuture.isAfter(instant);

            }
            if (!isValid) {
                throw new UnsupportedOperationException(instant + " for field "
                        + fieldDescriptor.getFullName() + " is outside the allowed bounds. "
                        + "You can only stream to date range within 1825 days in the past "
                        + "and 366 days in the future relative to the current date.");
            }
            return timeStamp;
        } else {
            // other timestamps should be in the limit specifies by BQ
            if (instant.isAfter(MIN_TIMESTAMP) && instant.isBefore(MAX_TIMESTAMP)) {
                return timeStamp;
            } else {
                throw new UnsupportedOperationException(instant
                        + " for field "
                        + fieldDescriptor.getFullName()
                        + " is outside the allowed bounds in BQ.");
            }
        }
    }
}
