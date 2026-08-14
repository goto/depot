package com.gotocompany.depot.message.proto;

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.gotocompany.depot.config.SinkConfig;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.gotocompany.depot.exception.DeserializerException;
import lombok.AllArgsConstructor;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONWriter;

import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class implements JSONProvider which will be used by JSONPath to query/update input data.
 * This implementation accepts proto message as input and provides a way to operate on ProtoMessage.
 *
 * <p>A Jayway JSONPath {@link JsonProvider} that allows JSONPath expressions to be evaluated and applied
 * directly over Protobuf {@link DynamicMessage} trees without first serializing them to JSON.</p>
 *
 * <p>This implementation accepts a proto message as input and provides a way to query and update a
 * proto message: it treats a {@link DynamicMessage} as a JSON object whose keys are the message's
 * field names and wraps individual field values in the private {@link ProtoFieldValue} helper so that
 * scalars, repeated fields and nested messages can be navigated, read and written through the
 * JSONPath API. Operations that do not involve proto types are delegated to a backing
 * {@link JsonOrgJsonProvider}.</p>
 *
 * <p>Whenever a value must be materialised as JSON the configured {@link JsonFormat.Printer} is used.
 * Unsigned 32-bit and 64-bit integers, byte strings and enums receive the special handling defined
 * by the canonical Protobuf JSON mapping: unsigned values are rendered as their unsigned decimal
 * string, bytes are base64-encoded, and {@code google.protobuf.NullValue} is rendered as
 * {@code null}.</p>
 *
 * <p>The provider is configured once from a {@link SinkConfig} and may be reused across JSONPath
 * evaluations.</p>
 *
 * @see ProtoParsedMessage#getFieldByName(String)
 */
public class ProtoJsonProvider implements JsonProvider {

    /**
     * Bit mask ({@code 0x00000000FFFFFFFF}) used to reinterpret the lower 32 bits of a negative
     * signed {@code int} as an unsigned value when rendering unsigned 32-bit fields.
     */
    private static final Long LONG_MASK = 0x00000000FFFFFFFFL;
    /**
     * Configured Protobuf-to-JSON printer used to materialise messages and field values; it preserves
     * proto field names, omits insignificant whitespace and, when enabled, includes default values.
     */
    private final JsonFormat.Printer printer;
    /**
     * Backing provider for plain (non-proto) JSON values; every operation that does not involve a
     * proto type is delegated to this {@link JsonOrgJsonProvider}.
     */
    private static final JsonOrgJsonProvider JSON_P = new JsonOrgJsonProvider();

    /**
     * Builds a provider whose JSON output is configured from the supplied sink configuration.
     *
     * <p>The internal {@link JsonFormat.Printer} always preserves proto field names and omits
     * insignificant whitespace. When {@link SinkConfig#getSinkDefaultFieldValueEnable()} is
     * {@code true}, fields that hold their default value are also emitted.</p>
     *
     * @param sinkConfig the sink configuration controlling whether default field values are included
     *     in the JSON output
     */
    public ProtoJsonProvider(SinkConfig sinkConfig) {

        JsonFormat.Printer tempPrinter = JsonFormat.printer()
                .preservingProtoFieldNames()
                .omittingInsignificantWhitespace();
        if (sinkConfig.getSinkDefaultFieldValueEnable()) {
            tempPrinter = tempPrinter.includingDefaultValueFields();
        }
        this.printer = tempPrinter;
    }


    /**
     * Serialises a {@link DynamicMessage} to its JSON string form using the configured printer.
     *
     * @param msg the dynamic message to serialise
     * @return the JSON representation of {@code msg}
     * @throws DeserializerException if the message cannot be converted to JSON
     */
    private String printMessage(DynamicMessage msg) {
        try {
            return printer.print(msg);
        } catch (InvalidProtocolBufferException e) {
            String name = msg.getDescriptorForType().getFullName();
            throw new DeserializerException("Unable to convert message to JSON" + name, e);
        }
    }

    /**
     * Indicates whether the given field holds a scalar (non-message) value.
     *
     * @param fd the field descriptor to inspect
     * @return {@code true} if the field's Java type is anything other than
     *     {@link Descriptors.FieldDescriptor.JavaType#MESSAGE}, {@code false} otherwise
     */
    private boolean isPrimitive(Descriptors.FieldDescriptor fd) {
        return !fd.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.MESSAGE);
    }

    /**
     * Converts a scalar Protobuf field value into the Java representation expected in JSON output.
     *
     * <p>The conversion mirrors the canonical Protobuf JSON mapping for the scalar types that need
     * special treatment:</p>
     * <ul>
     *   <li>{@code UINT32} and {@code FIXED32} values are rendered as their unsigned decimal string.</li>
     *   <li>{@code UINT64} and {@code FIXED64} values are rendered as their unsigned decimal string.</li>
     *   <li>{@code BYTES} values are base64-encoded.</li>
     *   <li>Enum values are rendered as their name, except {@code google.protobuf.NullValue} which
     *       maps to {@code null}.</li>
     *   <li>All other scalar types are returned unchanged.</li>
     * </ul>
     *
     * @param fd the descriptor of the field being converted
     * @param value the raw field value as returned by the Protobuf API
     * @return the JSON-friendly representation of {@code value}, or {@code null} for a
     *     {@code google.protobuf.NullValue} enum
     */
    private Object getPrimitiveValue(Descriptors.FieldDescriptor fd, Object value) {
        switch (fd.getType()) {
            case UINT32:
            case FIXED32:
                return unsignedToString((Integer) value);
            case UINT64:
            case FIXED64:
                return unsignedToString((Long) value);
            case BYTES:
                return BaseEncoding.base64().encode(((ByteString) value).toByteArray());
            case ENUM:
                // Special-case google.protobuf.NullValue (it's an Enum).
                if (fd.getEnumType().getFullName().equals("google.protobuf.NullValue")) {
                    return null;
                }
                return value.toString();
            default:
                return value;
        }
    }

    /**
     * Convert an unsigned 32-bit integer to a string.
     *
     * <p>Non-negative inputs are formatted directly. A negative input, whose sign bit actually
     * carries magnitude for an unsigned value, is widened to a {@code long} by masking with
     * {@link #LONG_MASK} so that the full 32-bit magnitude is preserved.</p>
     *
     * @param value the signed {@code int} holding the unsigned 32-bit value to format
     * @return the unsigned decimal string representation of {@code value}
     */
    private String unsignedToString(final int value) {
        if (value >= 0) {
            return Integer.toString(value);
        } else {
            return Long.toString(value & LONG_MASK);
        }
    }

    /**
     * Convert an unsigned 64-bit integer to a string.
     *
     * <p>Non-negative inputs are formatted directly. For a negative input the most-significant bit is
     * cleared before constructing a {@link BigInteger} (so it is not interpreted as a sign) and then
     * set again with {@link BigInteger#setBit(int)}, which yields the correct unsigned magnitude.</p>
     *
     * @param value the signed {@code long} holding the unsigned 64-bit value to format
     * @return the unsigned decimal string representation of {@code value}
     */
    private String unsignedToString(final long value) {
        if (value >= 0) {
            return Long.toString(value);
        } else {
            // Pull off the most-significant bit so that BigInteger doesn't think
            // the number is negative, then set it again using setBit().
            return BigInteger.valueOf(value & Long.MAX_VALUE).setBit(Long.SIZE - 1).toString();
        }
    }

    /**
     * Parses a JSON document supplied as a string into the backing provider's object model.
     *
     * @param json the JSON text to parse
     * @return the parsed JSON object model
     * @throws InvalidJsonException if {@code json} is not valid JSON
     */
    public Object parse(String json) throws InvalidJsonException {
        return JSON_P.parse(json);
    }

    /**
     * Parses a JSON document read from a stream into the backing provider's object model.
     *
     * @param jsonStream the stream supplying the JSON text
     * @param charset the name of the character set used to decode {@code jsonStream}
     * @return the parsed JSON object model
     * @throws InvalidJsonException if the stream content is not valid JSON
     */
    public Object parse(InputStream jsonStream, String charset) throws InvalidJsonException {
        return JSON_P.parse(jsonStream, charset);
    }

    /**
     * Renders an object managed by this provider as a JSON string.
     *
     * <p>A {@link ProtoFieldValue} is rendered through its own {@link ProtoFieldValue#toString()}, a
     * {@link DynamicMessage} is rendered with the configured printer, and any other object is
     * delegated to the backing JSON provider.</p>
     *
     * @param obj the object to render
     * @return the JSON string representation of {@code obj}
     */
    public String toJson(Object obj) {
        if (obj instanceof ProtoFieldValue) {
            return obj.toString();
        } else if (obj instanceof DynamicMessage) {
            return printMessage((DynamicMessage) obj);
        }
        return JSON_P.toJson(obj);
    }

    /**
     * Creates a new, empty JSON array instance in the backing provider's model.
     *
     * @return a new empty array object
     */
    public Object createArray() {
        return JSON_P.createArray();
    }

    /**
     * Creates a new, empty JSON object (map) instance in the backing provider's model.
     *
     * @return a new empty map object
     */
    @Override
    public Object createMap() {
        return JSON_P.createMap();
    }

    /**
     * Determines whether the given object should be treated as a JSON array.
     *
     * <p>A {@link ProtoFieldValue} reports itself as an array when it wraps a repeated field;
     * otherwise the decision is delegated to the backing JSON provider.</p>
     *
     * @param obj the object to test
     * @return {@code true} if {@code obj} represents an array, {@code false} otherwise
     */
    public boolean isArray(Object obj) {
        if (obj instanceof ProtoFieldValue) {
            return ((ProtoFieldValue) obj).isArray();
        }
        return JSON_P.isArray(obj);
    }

    /**
     * Returns the length of the given container object.
     *
     * <p>For a {@link DynamicMessage} this is the number of fields declared by its type; for a
     * {@link ProtoFieldValue} wrapping a repeated value, or a plain {@link List}, it is the number of
     * elements; any other object is delegated to the backing JSON provider.</p>
     *
     * @param obj the container whose length is required
     * @return the number of fields or elements contained in {@code obj}
     */
    @Override
    public int length(Object obj) {
        if (obj instanceof DynamicMessage) {
            return ((DynamicMessage) obj).getDescriptorForType().getFields().size();
        } else if (obj instanceof ProtoFieldValue) {
            return ((List) ((ProtoFieldValue) obj).value).size();
        } else if (obj instanceof List) {
            return ((List) obj).size();
        }
        return JSON_P.length(obj);
    }

    /**
     * Adapts the given object to an {@link Iterable}.
     *
     * <p>The object is cast directly to {@link Iterable}; a {@link ProtoFieldValue} supports this
     * because it implements {@link Iterable} over the elements of a repeated field.</p>
     *
     * @param obj the object to view as iterable
     * @return {@code obj} viewed as an {@link Iterable}
     * @throws ClassCastException if {@code obj} is not an {@link Iterable}
     */
    @Override
    public Iterable<?> toIterable(Object obj) {
        return (Iterable<?>) obj;
    }

    /**
     * Returns the property (field) names exposed by the given object.
     *
     * <p>A {@link ProtoFieldValue} is unwrapped to its underlying value first. For a
     * {@link DynamicMessage} the keys are the names of the fields that are actually set; any other
     * object is delegated to the backing JSON provider.</p>
     *
     * @param obj the object whose property keys are required
     * @return the collection of property names exposed by {@code obj}
     */
    @Override
    public Collection<String> getPropertyKeys(Object obj) {
        Object val = obj;
        if (obj instanceof ProtoFieldValue) {
            return getPropertyKeys(((ProtoFieldValue) obj).value);
        }
        if (val instanceof DynamicMessage) {
            DynamicMessage msg = (DynamicMessage) val;
            return msg.getAllFields().entrySet().stream().map(fde -> fde.getKey().getName())
                    .collect(Collectors.toList());
        }
        return JSON_P.getPropertyKeys(obj);
    }

    /**
     * Returns the element at the given index of an array-like object.
     *
     * <p>A plain {@link List} is indexed directly, while a {@link ProtoFieldValue} returns the
     * indexed element of its repeated value wrapped as a new {@link ProtoFieldValue}; any other
     * object is delegated to the backing JSON provider.</p>
     *
     * @param obj the array-like object to index into
     * @param idx the zero-based index of the desired element
     * @return the element at position {@code idx}
     */
    @Override
    public Object getArrayIndex(Object obj, int idx) {
        if (obj instanceof List) {
            return ((List) obj).get(idx);
        } else if (obj instanceof ProtoFieldValue) {
            return ((ProtoFieldValue) obj).getListValue(idx);
        }
        return JSON_P.getArrayIndex(obj, idx);
    }

    /**
     * Returns the element at the given index of an array-like object, ignoring the unwrap flag.
     *
     * <p>This overload exists to satisfy the {@link JsonProvider} contract; it always delegates to
     * {@link #getArrayIndex(Object, int)} regardless of the value of {@code unwrap}.</p>
     *
     * @param obj the array-like object to index into
     * @param idx the zero-based index of the desired element
     * @param unwrap ignored by this implementation
     * @return the element at position {@code idx}
     */
    @Override
    public Object getArrayIndex(Object obj, int idx, boolean unwrap) {
        return getArrayIndex(obj, idx);
    }

    /**
     * Inserts or sets a value at the given index of an array.
     *
     * <p>When the target is a {@link JSONArray} the operation is delegated to the backing provider,
     * first converting a {@link ProtoFieldValue} to its JSON value; otherwise the value is inserted
     * into the target {@link List} at position {@code idx}.</p>
     *
     * @param array the array to modify
     * @param idx the index at which to place {@code newValue}
     * @param newValue the value to store, possibly a {@link ProtoFieldValue}
     */
    @Override
    public void setArrayIndex(Object array, int idx, Object newValue) {
        if (array instanceof JSONArray) {
            if (newValue instanceof ProtoFieldValue) {
                JSON_P.setArrayIndex(array, idx, ((ProtoFieldValue) newValue).getJsonValue());
            } else {
                JSON_P.setArrayIndex(array, idx, newValue);
            }
        } else {
            ((List) array).add(idx, newValue);
        }
    }

    /**
     * Sets a property on a JSON object, converting proto-backed values to JSON first.
     *
     * <p>A {@link ProtoFieldValue} is replaced by its JSON value before being stored; all other
     * values are passed through to the backing provider unchanged.</p>
     *
     * @param obj the JSON object to modify
     * @param key the property key to set
     * @param value the value to associate with {@code key}, possibly a {@link ProtoFieldValue}
     */
    @Override
    public void setProperty(Object obj, Object key, Object value) {
        if (value instanceof ProtoFieldValue) {
            JSON_P.setProperty(obj, key, ((ProtoFieldValue) value).getJsonValue());
        } else {
            JSON_P.setProperty(obj, key, value);
        }
    }

    /**
     * Removes a property from a JSON object by delegating to the backing provider.
     *
     * @param obj the JSON object to modify
     * @param key the property key to remove
     */
    @Override
    public void removeProperty(Object obj, Object key) {
        JSON_P.removeProperty(obj, key);
    }

    /**
     * Determines whether the given object should be treated as a JSON object (map).
     *
     * <p>A {@link ProtoFieldValue} reports itself as a map when it wraps a non-repeated message
     * field; otherwise an object is considered a map when it is a {@link DynamicMessage} or when the
     * backing provider regards it as a map.</p>
     *
     * @param obj the object to test
     * @return {@code true} if {@code obj} represents a map, {@code false} otherwise
     */
    @Override
    public boolean isMap(Object obj) {
        if (obj instanceof ProtoFieldValue) {
            return ((ProtoFieldValue) obj).isMap();
        }
        return obj instanceof DynamicMessage || JSON_P.isMap(obj);
    }

    /**
     * Unwraps a provider-specific wrapper to the plain value it carries.
     *
     * <p>A {@link ProtoFieldValue} is unwrapped to its underlying field value; any other object is
     * returned unchanged.</p>
     *
     * @param obj the object to unwrap
     * @return the unwrapped value, or {@code obj} itself if it is not a {@link ProtoFieldValue}
     */
    @Override
    public Object unwrap(Object obj) {
        if (obj instanceof ProtoFieldValue) {
            return ((ProtoFieldValue) obj).value;
        }
        return obj;
    }

    /**
     * Returns the value of a named property of a map-like object.
     *
     * <p>For a {@link DynamicMessage} the named field is resolved through its descriptor and the value
     * is returned wrapped in a {@link ProtoFieldValue}. A {@link ProtoFieldValue} is unwrapped and
     * resolved recursively, and any other object is delegated to the backing provider.</p>
     *
     * @param obj the map-like object to read from
     * @param key the name of the property to read
     * @return the value associated with {@code key}, wrapped as a {@link ProtoFieldValue} for proto
     *     messages
     * @throws PathNotFoundException if {@code obj} is a {@link DynamicMessage} that declares no field
     *     named {@code key}
     */
    @Override
    public Object getMapValue(Object obj, String key) {
        if (obj instanceof DynamicMessage) {
            DynamicMessage msg = (DynamicMessage) obj;
            Descriptors.FieldDescriptor fd = msg.getDescriptorForType().findFieldByName(key);
            if (fd == null) {
                throw new PathNotFoundException(String.format("\"%s\" not found", key));
            }
            Object value = msg.getField(fd);
            return new ProtoFieldValue(fd, value);
        }
        if (obj instanceof ProtoFieldValue) {
            return getMapValue(unwrap(obj), key);
        }
        return JSON_P.getMapValue(obj, key);
    }

    /**
     * Lightweight adapter that lets a single Protobuf field value take part in JSONPath navigation.
     *
     * <p>Each instance pairs a {@link Descriptors.FieldDescriptor} with the raw value of that field.
     * It can report whether the value behaves as a map (a non-repeated message), as an array (a
     * repeated field) or as a scalar, and it can lazily materialise the value as JSON through
     * {@link #getJsonValue()}. The class implements {@link Iterable} so that repeated fields can be
     * iterated element by element, with each element re-wrapped as a {@link ProtoFieldValue}.</p>
     */
    @AllArgsConstructor
    private class ProtoFieldValue implements Iterable {
        /**
         * Descriptor of the Protobuf field whose value is wrapped by this instance.
         */
        private Descriptors.FieldDescriptor fd;
        /**
         * Raw value of the wrapped field as returned by the Protobuf API; may be a scalar, a nested
         * message, or a {@link List} for repeated fields.
         */
        private Object value;

        /**
         * Indicates whether the wrapped field should be treated as a JSON object.
         *
         * @return {@code true} if the field is a non-repeated message field, {@code false} otherwise
         */
        private boolean isMap() {
            return fd.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE) && !isArray();
        }

        /**
         * Indicates whether the wrapped value should be treated as a JSON array.
         *
         * @return {@code true} if the wrapped value is a {@link List}, which is how repeated fields are
         *     represented, {@code false} otherwise
         */
        private boolean isArray() {
            return value instanceof List;
        }

        /**
         * Materialises the wrapped field value as a JSON-compatible value.
         *
         * <p>The conversion handles the different field shapes as follows:</p>
         * <ul>
         *   <li>An empty repeated value is returned unchanged.</li>
         *   <li>Scalar (primitive) fields are converted with
         *       {@link ProtoJsonProvider#getPrimitiveValue(Descriptors.FieldDescriptor, Object)},
         *       producing a {@link JSONArray} for repeated scalars.</li>
         *   <li>Message fields are serialised by building a temporary {@link DynamicMessage} on the
         *       containing type, printing it with the configured printer and extracting the field's
         *       JSON value; a single value of a repeated field is unwrapped from the resulting
         *       array.</li>
         * </ul>
         *
         * @return the JSON representation of the wrapped field value
         * @throws DeserializerException if a message field cannot be printed to JSON
         */
        private Object getJsonValue() {
            Descriptors.Descriptor parent = fd.getContainingType();
            if (isArray() && ((List) value).isEmpty()) {
                return value;
            }
            if (isPrimitive(fd)) {
                if (isArray()) {
                    return new JSONArray(((List) value).stream().map(a -> getPrimitiveValue(fd, a)).toArray());
                }
                return getPrimitiveValue(fd, value);
            }
            Object newValue = value;
            if (fd.isRepeated() && !isArray()) {
                newValue = new ArrayList() {{
                    add(value);
                }};
            }
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(parent).setField(fd, newValue);
            if (!fd.isRepeated() && !builder.hasField(fd)) {
                return value;
            }
            String jsonValue;
            try {
                jsonValue = printer.print(builder);
            } catch (InvalidProtocolBufferException e) {
                throw new DeserializerException("Unable to get JSON value at " + fd.getFullName(), e);
            }
            JSONObject js = new JSONObject(new JSONTokener(jsonValue));
            if (fd.isRepeated() && !(value instanceof List)) {
                return js.getJSONArray(fd.getName()).get(0);
            }
            return js.get(fd.getName());
        }

        /**
         * Returns the element at the given index of a repeated field, wrapped for further navigation.
         *
         * @param idx the zero-based index of the desired element
         * @return a new {@link ProtoFieldValue} wrapping the element at position {@code idx}
         */
        private Object getListValue(int idx) {
            return new ProtoFieldValue(fd, ((List) value).get(idx));
        }

        /**
         * Returns the JSON string form of the wrapped value.
         *
         * @return the wrapped value rendered as a JSON string via {@link #getJsonValue()}
         */
        @Override
        public String toString() {
            return JSONWriter.valueToString(getJsonValue());
        }

        /**
         * Returns an iterator over the elements of a repeated field.
         *
         * <p>Each element is wrapped in its own {@link ProtoFieldValue} so that nested navigation
         * remains possible.</p>
         *
         * @return an iterator yielding each element wrapped as a {@link ProtoFieldValue}
         */
        @Override
        public Iterator iterator() {
            return ((List) value).stream().map(o -> new ProtoFieldValue(fd, o)).iterator();
        }
    }

}
