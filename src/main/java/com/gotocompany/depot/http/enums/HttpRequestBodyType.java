package com.gotocompany.depot.http.enums;

/**
 * Enumerates how the HTTP sink serializes a message into the body of an outbound request.
 *
 * <p>The value is resolved from the {@code SINK_HTTPV2_REQUEST_BODY_MODE} configuration key and
 * selects which {@link com.gotocompany.depot.http.request.body.RequestBody} implementation
 * {@link com.gotocompany.depot.http.request.body.RequestBodyFactory} creates. Each constant trades
 * off fidelity to the raw bytes against producing a structured, human-readable JSON document.</p>
 *
 * @see com.gotocompany.depot.http.request.body.RequestBodyFactory
 * @see com.gotocompany.depot.http.request.body.RequestBody
 */
public enum HttpRequestBodyType {
    /**
     * Emits the original serialized payloads, Base64-encoded, alongside metadata.
     *
     * <p>Backed by {@link com.gotocompany.depot.http.request.body.RawBody}. The raw key and value
     * bytes are Base64-encoded under {@code log_key} and {@code log_message} keys, making this the
     * most faithful but least readable representation.</p>
     */
    RAW,
    /**
     * Emits the fully parsed key and value as nested JSON objects alongside metadata.
     *
     * <p>Backed by {@link com.gotocompany.depot.http.request.body.JsonBody}. The decoded key and
     * value are rendered as JSON under {@code logKey} and {@code logMessage} keys.</p>
     */
    JSON,
    /**
     * Emits a single parsed message portion rendered directly as JSON.
     *
     * <p>Backed by {@link com.gotocompany.depot.http.request.body.MessageBody}. Either the key or the
     * value is serialized to JSON depending on the configured schema message mode, without any
     * enclosing envelope or metadata.</p>
     */
    MESSAGE,
    /**
     * Emits JSON produced by substituting message fields into a user-supplied template.
     *
     * <p>Backed by {@link com.gotocompany.depot.http.request.body.TemplatizedJsonBody}. The template
     * provided through {@code SINK_HTTPV2_JSON_BODY_TEMPLATE} has its placeholders replaced with
     * fields from the parsed message, giving full control over the body shape.</p>
     */
    TEMPLATIZED_JSON
}
