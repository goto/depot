package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Base64;
import java.util.Date;

/**
 * {@link RequestBody} that emits the original serialized payloads, Base64-encoded, with metadata.
 *
 * <p>Selected for {@link com.gotocompany.depot.http.enums.HttpRequestBodyType#RAW}. Rather than
 * decoding the record, it requires the raw key and value to be {@code byte[]} payloads, Base64-encodes
 * each, and places them under the {@code log_key} and {@code log_message} fields of a JSON object,
 * appending any configured metadata. This is the most faithful representation of the source bytes.</p>
 *
 * @see RequestBody
 * @see com.gotocompany.depot.http.enums.HttpRequestBodyType#RAW
 */
public class RawBody implements RequestBody {
    /**
     * Sink configuration supplying the metadata settings.
     */
    private final HttpSinkConfig config;

    /**
     * Creates a raw body serializer bound to the given configuration.
     *
     * @param config the HTTP sink configuration
     */
    public RawBody(HttpSinkConfig config) {
        this.config = config;
    }

    /**
     * Serializes the raw key and value bytes (Base64-encoded) plus metadata into a JSON object string.
     *
     * <p>The message's key and value are first validated to be {@code byte[]} instances, then
     * Base64-encoded into the {@code log_key} and {@code log_message} fields. Configured metadata
     * (with timestamp columns rendered via {@link java.util.Date}) is merged in under its own keys.</p>
     *
     * @param messageContainer the lazily-parsed view of the message
     * @return the JSON object rendered as a string
     * @throws IOException if the key or value payloads are not {@code byte[]} instances
     */
    @Override
    public String build(MessageContainer messageContainer) throws IOException {
        Message message = messageContainer.getMessage();
        JSONObject payload = new JSONObject();
        MessageUtils.validate(message, byte[].class);
        payload.put("log_key", encodedSerializedStringFrom((byte[]) message.getLogKey()));
        payload.put("log_message", encodedSerializedStringFrom((byte[]) message.getLogMessage()));
        MessageUtils.getMetaData(message, config, Date::new).forEach(payload::put);
        return payload.toString();
    }

    /**
     * Base64-encodes a byte array into its string form.
     *
     * @param bytes the bytes to encode; may be {@code null} or empty
     * @return the Base64-encoded string, or an empty string when {@code bytes} is {@code null} or
     *     empty
     */
    private String encodedSerializedStringFrom(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        return new String(Base64.getEncoder().encode(bytes));
    }
}
