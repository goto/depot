package com.gotocompany.depot.message;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.SinkConfig;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Static helper methods shared by Depot's message-parsing and record-conversion code.
 *
 * <p>The utilities here cover three concerns: assembling the metadata columns that may be written
 * alongside a record, reading individual fields out of a JSON document by name, and validating that a
 * {@link Message}'s payloads are of an expected type. The class is not meant to be instantiated and
 * exposes only static methods.</p>
 *
 * @see Message
 * @see SinkConfig
 */
public class MessageUtils {

    /**
     * Builds the metadata map to attach to a record, honoring the sink's metadata configuration.
     *
     * <p>When {@link SinkConfig#shouldAddMetadata()} is enabled, the configured metadata columns are
     * read from the message and then post-processed so that columns declared as timestamps are
     * converted from their epoch representation using {@code timeStampConvertor}. When metadata is
     * disabled an empty map is returned.</p>
     *
     * @param message the record whose metadata is being extracted
     * @param config the sink configuration declaring whether and which metadata columns to emit
     * @param timeStampConvertor function converting an epoch {@link Long} into the representation
     *     expected for timestamp columns
     * @return a map of metadata column names to values, or an empty map when metadata is disabled
     * @throws NullPointerException if metadata is enabled and a configured column has no corresponding
     *     value in the message, because the underlying collectors reject {@code null} values
     */
    public static Map<String, Object> getMetaData(Message message, SinkConfig config, Function<Long, Object> timeStampConvertor) {
        if (config.shouldAddMetadata()) {
            List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
            Map<String, Object> metadata = message.getMetadata(metadataColumnsTypes);
            return checkAndSetTimeStampColumns(metadata, metadataColumnsTypes, timeStampConvertor);
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Converts the timestamp-typed entries of a metadata map using the supplied converter.
     *
     * <p>Each column in {@code metadataColumnsTypes} is re-collected into a new map: when a column is
     * declared with the type {@code "timestamp"} and its current value is a {@link Long}, the value is
     * passed through {@code timeStampConvertor}; every other value is copied unchanged. The declared
     * type is taken from the second component of each {@link TupleString}.</p>
     *
     * @param metadata the current metadata values keyed by column name
     * @param metadataColumnsTypes the column definitions pairing each column name with its declared
     *     type
     * @param timeStampConvertor function converting an epoch {@link Long} into the representation
     *     expected for timestamp columns
     * @return a new map with timestamp columns converted and all other columns preserved
     * @throws NullPointerException if a listed column has no value in {@code metadata}, because the
     *     underlying collector rejects {@code null} values
     */
    public static Map<String, Object> checkAndSetTimeStampColumns(Map<String, Object> metadata, List<TupleString> metadataColumnsTypes, Function<Long, Object> timeStampConvertor) {
        return metadataColumnsTypes.stream().collect(Collectors.toMap(TupleString::getFirst, t -> {
            String key = t.getFirst();
            String dataType = t.getSecond();
            Object value = metadata.get(key);
            if (value instanceof Long && dataType.equals("timestamp")) {
                return timeStampConvertor.apply((Long) value);
            }
            return value;
        }));
    }

    /**
     * Reads a single field from a JSON document by name using JSONPath.
     *
     * <p>The {@code name} is turned into the JSONPath expression {@code "$." + name} and evaluated
     * against {@code jsonObject} with the given JSONPath {@link Configuration}. The configuration
     * controls how the document is navigated, including the JSON provider used to interpret
     * {@code jsonObject}.</p>
     *
     * @param name the name of the field to read, used as a top-level JSONPath property
     * @param jsonObject the JSON document to read from, in a form understood by the configured JSON
     *     provider
     * @param jsonPathConfig the JSONPath configuration controlling evaluation
     * @return the value located at the requested field
     * @throws IllegalArgumentException if the field cannot be found in the document
     */
    public static Object getFieldFromJsonObject(String name, Object jsonObject, Configuration jsonPathConfig) {
        try {
            String jsonPathName = "$." + name;
            JsonPath jsonPath = JsonPath.compile(jsonPathName);
            return jsonPath.read(jsonObject, jsonPathConfig);
        } catch (PathNotFoundException e) {
            throw new IllegalArgumentException("Invalid field config : " + name, e);
        }
    }

    /**
     * Verifies that a message's populated payloads are instances of an expected class.
     *
     * <p>Both the log key and the log message are checked: if either is non-{@code null} and is not an
     * instance of {@code validClass}, an exception is thrown that reports the expected class alongside
     * the actual classes of the key and value (or {@code n/a} when a payload is absent). A
     * {@code null} payload passes the check.</p>
     *
     * @param message the message whose key and value payloads are validated
     * @param validClass the class both payloads are required to be instances of
     * @throws IOException if a non-{@code null} key or value is not an instance of {@code validClass}
     */
    public static void validate(Message message, Class validClass) throws IOException {
        if ((message.getLogKey() != null && !(validClass.isInstance(message.getLogKey())))
                || (message.getLogMessage() != null && !(validClass.isInstance(message.getLogMessage())))) {
            throw new IOException(
                    String.format("Expected class %s, but found: LogKey class: %s, LogMessage class: %s",
                            validClass,
                            message.getLogKey() != null ? message.getLogKey().getClass() : "n/a",
                            message.getLogMessage() != null ? message.getLogMessage().getClass() : "n/a"));
        }
    }
}
