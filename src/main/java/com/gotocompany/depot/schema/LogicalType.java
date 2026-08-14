package com.gotocompany.depot.schema;

/**
 * Classifies the higher-level meaning of a {@link Schema} or value beyond its physical structure.
 *
 * <p>Where {@link SchemaFieldType} captures the storage category of a field, {@code LogicalType}
 * identifies well-known semantic types that warrant special handling by the sinks, such as timestamps
 * and durations. It is reported by {@link Schema#logicalType()} and drives which accessor of a
 * {@link com.gotocompany.depot.message.LogicalValue} is meaningful for a given value. For Protobuf,
 * the well-known constants are recognized from the message's fully qualified type name (for example
 * {@code google.protobuf.Timestamp}) or from map-entry options.</p>
 *
 * @see Schema#logicalType()
 * @see com.gotocompany.depot.message.LogicalValue
 */
public enum LogicalType {
    /**
     * An ordinary, structured message with no special well-known interpretation.
     */
    MESSAGE,
    /**
     * A point in time, corresponding to the {@code google.protobuf.Timestamp} well-known type.
     */
    TIMESTAMP,
    /**
     * A dynamically typed, free-form structure, corresponding to the {@code google.protobuf.Struct}
     * well-known type.
     */
    STRUCT,
    /**
     * An elapsed amount of time, corresponding to the {@code google.protobuf.Duration} well-known
     * type.
     */
    DURATION,
    /**
     * A map (associative array), corresponding to a Protobuf map-entry message.
     */
    MAP

}
