package com.gotocompany.depot.bigquery;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Immutable value object describing the Kafka-style envelope metadata of a record, shared across the
 * BigQuery sink tests.
 *
 * <p>It bundles the source coordinates of a record ({@code topic}, {@code partition} and
 * {@code offset}) with its event {@code timestamp} and the {@code loadTime} at which the record was
 * ingested. The Lombok annotations generate the all-arguments constructor, the field accessors and
 * the {@code equals}/{@code hashCode}/{@code toString} implementations, so the type is purely a
 * convenient carrier used to build test messages and the matching expected metadata columns (see
 * {@link TestMessageBuilder}).</p>
 */
@EqualsAndHashCode
@ToString
@Getter
@AllArgsConstructor
public class TestMetadata {
    /** Name of the source topic the simulated record was consumed from. */
    private final String topic;
    /** Zero-based partition index of the simulated record within its topic. */
    private final int partition;
    /** Offset of the simulated record within its partition. */
    private final long offset;
    /** Event timestamp of the record, expressed in epoch milliseconds. */
    private final long timestamp;
    /** Ingestion (load) time recorded for the record, expressed in epoch milliseconds. */
    private final long loadTime;
}
