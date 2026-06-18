package com.gotocompany.depot.bigquery.models;

import com.gotocompany.depot.error.ErrorInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Map;

/**
 * Immutable value object representing one message converted for the BigQuery sink.
 *
 * <p>A record carries the column values to be written, the associated metadata, the
 * original index of the message within the incoming batch, and, when the message could
 * not be converted, an {@link ErrorInfo} describing the failure. Instances are produced
 * by the message-to-record converter and grouped into {@link Records}.</p>
 *
 * <p>Lombok generates the all-arguments constructor, a builder, getters, and
 * {@code equals}, {@code hashCode} and {@code toString} implementations.</p>
 *
 * @see Records
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode
@Builder
@ToString
public class Record {
    /** Metadata associated with the message, keyed by metadata column name. */
    private final Map<String, Object> metadata;
    /** Column values to be written to BigQuery, keyed by column name. */
    private final Map<String, Object> columns;
    /** Zero-based position of the originating message within the incoming batch. */
    private final long index;
    /** Conversion error for an invalid record, or {@code null} for a valid record. */
    private final ErrorInfo errorInfo;
}
