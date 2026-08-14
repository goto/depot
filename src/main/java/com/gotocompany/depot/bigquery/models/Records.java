package com.gotocompany.depot.bigquery.models;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

/**
 * Container holding the outcome of converting a batch of messages for the BigQuery sink.
 *
 * <p>Splits the converted {@link Record} instances into those that are valid and ready to
 * be written and those that failed conversion and therefore carry error information. The
 * sink writes the valid records and reports the invalid ones as errors.</p>
 *
 * <p>Lombok generates the all-arguments constructor, getters, and {@code equals},
 * {@code hashCode} and {@code toString} implementations.</p>
 *
 * @see Record
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class Records {
    /** Records that converted successfully and can be written to BigQuery. */
    private final List<Record> validRecords;
    /** Records that failed conversion and carry error information. */
    private final List<Record> invalidRecords;
}
