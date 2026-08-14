package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.message.MessageContainer;

import java.io.IOException;

/**
 * Strategy that serializes a single message into the string payload of an HTTP request body.
 *
 * <p>The HTTP sink supports several body encodings, each captured by a concrete implementation:
 * {@link RawBody} (Base64-encoded raw bytes with metadata), {@link JsonBody} (parsed key and value
 * as nested JSON with metadata), {@link MessageBody} (a single parsed portion as JSON) and
 * {@link TemplatizedJsonBody} (JSON produced from a user template). The implementation in use is
 * chosen by {@link RequestBodyFactory} from the configured body type.</p>
 *
 * @see RequestBodyFactory
 * @see com.gotocompany.depot.http.enums.HttpRequestBodyType
 */
public interface RequestBody {

    /**
     * Serializes the given message into the request body payload.
     *
     * @param messageContainer the lazily-parsed view of the message to serialize
     * @return the serialized body as a string
     * @throws IOException if the message cannot be parsed or serialized
     */
    String build(MessageContainer messageContainer) throws IOException;
}
