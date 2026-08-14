package com.gotocompany.depot.message;

import com.gotocompany.depot.schema.LogicalType;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Schema-agnostic view over a value that carries one of Depot's logical types.
 *
 * <p>Several sink formats need to interpret a raw payload value not by its physical wire type but by
 * the higher-level concept it represents, such as a timestamp, a duration or a free-form struct. A
 * {@code LogicalValue} bridges a concrete message representation (for example a Protobuf
 * well-known-type message) and these logical concepts, exposing a small set of typed accessors that
 * each return the value reinterpreted as the corresponding Java type.</p>
 *
 * <p>Instances are obtained from {@link ParsedMessage#getLogicalValue()} and are typically backed by
 * a single underlying message. Callers are expected to first inspect {@link #getType()} and then
 * invoke only the accessor that matches the reported {@link LogicalType}; invoking an accessor that
 * does not match the underlying value may fail or yield an undefined result depending on the
 * implementation.</p>
 *
 * @see LogicalType
 * @see ParsedMessage#getLogicalValue()
 */
public interface LogicalValue {
    /**
     * Returns the logical type of this value.
     *
     * <p>The returned {@link LogicalType} indicates which of the typed accessors on this interface is
     * meaningful for the underlying value.</p>
     *
     * @return the {@link LogicalType} describing how this value should be interpreted
     */
    LogicalType getType();

    /**
     * Reinterprets the underlying value as a point in time.
     *
     * <p>This accessor is meaningful when {@link #getType()} reports {@link LogicalType#TIMESTAMP}.</p>
     *
     * @return the value expressed as an {@link Instant}
     */
    Instant getTimestamp();

    /**
     * Reinterprets the underlying value as a free-form, dynamically typed structure.
     *
     * <p>This accessor is meaningful when {@link #getType()} reports {@link LogicalType#STRUCT}. The
     * returned map is keyed by field name and its values are the plain Java representations of the
     * struct's fields, with nested structures converted recursively.</p>
     *
     * @return a map from field name to the converted Java value of each struct field
     */
    Map<String, Object> getStruct();

    /**
     * Reinterprets the underlying value as an elapsed amount of time.
     *
     * <p>This accessor is meaningful when {@link #getType()} reports {@link LogicalType#DURATION}.</p>
     *
     * @return the value expressed as a {@link Duration}
     */
    Duration getDuration();
}
