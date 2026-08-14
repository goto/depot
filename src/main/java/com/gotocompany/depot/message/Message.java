package com.gotocompany.depot.message;

import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.common.TupleString;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * In-memory representation of a single record consumed by a Depot sink.
 *
 * <p>A {@code Message} couples the raw key and value payloads of a record with an arbitrary bag of
 * metadata describing it. The key and value are held as opaque {@link Object} references because, at
 * this layer, Depot has not yet decided how to interpret them; downstream {@link MessageParser}
 * implementations later decode them as Protobuf or JSON according to the configured schema. The most
 * common payload types are a {@code byte[]} of serialized Protobuf and a {@link String} of JSON.</p>
 *
 * <p>Metadata is stored in a {@link Map} keyed by column name and is populated once at construction
 * from a varargs array of {@link Tuple} entries. It typically carries injected attributes such as the
 * source topic, partition, offset and ingestion timestamps that can optionally be written alongside
 * the parsed payload.</p>
 *
 * <p>Lombok generates the {@code getLogKey()}, {@code getLogMessage()} and no-argument
 * {@code getMetadata()} accessors from {@code @Getter}, along with value-based {@code equals} and
 * {@code hashCode} implementations from {@code @EqualsAndHashCode}.</p>
 *
 * @see MessageContainer
 * @see MessageParser
 */
@Getter
@EqualsAndHashCode
public class Message {
    /**
     * Raw, unparsed key payload of the record, or {@code null} when the record has no key.
     *
     * <p>Held as an opaque {@link Object} (commonly a {@code byte[]} or a {@link String}) and later
     * interpreted by a {@link MessageParser} when {@link SinkConnectorSchemaMessageMode#LOG_KEY} is
     * parsed.</p>
     */
    private final Object logKey;
    /**
     * Raw, unparsed value payload of the record, or {@code null} when the record has no value.
     *
     * <p>Held as an opaque {@link Object} (commonly a {@code byte[]} or a {@link String}) and later
     * interpreted by a {@link MessageParser} when {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE}
     * is parsed.</p>
     */
    private final Object logMessage;
    /**
     * Mutable map of record metadata keyed by column name.
     *
     * <p>Populated once at construction from the supplied {@link Tuple} entries and thereafter read
     * via {@link #getMetadata(List)} or the Lombok-generated no-argument accessor.</p>
     */
    private final Map<String, Object> metadata = new HashMap<>();

    /**
     * Renders the entire metadata map as a single human-readable string.
     *
     * <p>Each entry is formatted as {@code key=value}, the entries are joined with a comma and a
     * space, and the whole result is wrapped in curly braces. The ordering of entries follows the
     * iteration order of the backing {@link HashMap} and is therefore not guaranteed.</p>
     *
     * @return a brace-delimited, comma-separated string of all metadata entries
     */
    public String getMetadataString() {
        return metadata.keySet().stream()
                .map(key -> key + "=" + metadata.get(key))
                .collect(Collectors.joining(", ", "{", "}"));
    }

    /**
     * Creates a message from its raw key and value payloads together with optional metadata entries.
     *
     * <p>Each supplied {@link Tuple} is stored in the metadata map using its first component as the
     * key and its second component as the value. The constructor is annotated with
     * {@code @SafeVarargs} because it only reads from the generic {@code tuples} array.</p>
     *
     * @param logKey the raw key payload, or {@code null} if the record has no key
     * @param logMessage the raw value payload, or {@code null} if the record has no value
     * @param tuples metadata entries to associate with the message, each mapping a column name to its
     *     value; may be empty
     */
    @SafeVarargs
    public Message(Object logKey, Object logMessage, Tuple<String, Object>... tuples) {
        this.logKey = logKey;
        this.logMessage = logMessage;
        Arrays.stream(tuples).forEach(t -> metadata.put(t.getFirst(), t.getSecond()));
    }

    /**
     * Returns the subset of metadata requested by the given column definitions.
     *
     * <p>For every {@link TupleString} in {@code metadataColumnsTypes} the column name (its first
     * component) is used to look up the corresponding value in this message's metadata, and the
     * matching entries are collected into a new map keyed by that column name. The second component
     * of each {@link TupleString}, which describes the column's declared type, is not consulted
     * here.</p>
     *
     * @param metadataColumnsTypes the metadata columns to extract, each pairing a column name with
     *     its declared type
     * @return a map from each requested column name to its metadata value
     * @throws NullPointerException if {@code metadataColumnsTypes} is {@code null}, or if a requested
     *     column has no associated value in the metadata, because the underlying collector rejects
     *     {@code null} values
     */
    public Map<String, Object> getMetadata(List<TupleString> metadataColumnsTypes) {
        return metadataColumnsTypes.stream()
                .collect(Collectors.toMap(
                        TupleString::getFirst, columnAndType -> metadata.get(columnAndType.getFirst())));
    }
}
