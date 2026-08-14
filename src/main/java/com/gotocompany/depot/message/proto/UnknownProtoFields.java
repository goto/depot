package com.gotocompany.depot.message.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import lombok.extern.slf4j.Slf4j;

/**
 * Try to convert raw proto bytes to some meaningful representation that is good enough for debug.
 *
 * <p>It attempts to decode an arbitrary byte array as a Protobuf {@link UnknownFieldSet}, which yields
 * a best-effort textual view of the wire-format fields even when the concrete message type is not
 * known. This is primarily used to surface the contents of messages that failed validation, for
 * example when unknown fields are detected. The class exposes a single static helper and is not
 * instantiated.</p>
 */
@Slf4j
public class UnknownProtoFields {
    /**
     * Converts raw Protobuf bytes into a best-effort textual representation for debugging.
     *
     * <p>The bytes are parsed as a Protobuf {@link UnknownFieldSet} and rendered with its
     * {@code toString()}. If the bytes are not a valid Protobuf message, the failure is logged at warn
     * level and an empty string is returned instead of propagating the error.</p>
     *
     * @param message the raw Protobuf bytes to convert
     * @return the decoded textual representation, or an empty string if the bytes cannot be parsed
     */
    public static String toString(byte[] message) {
        String convertedFields = "";
        try {
            convertedFields = UnknownFieldSet.parseFrom(message).toString();
        } catch (InvalidProtocolBufferException e) {
            log.warn("invalid byte representation of a protobuf message: {}", new String(message));
        }
        return convertedFields;
    }
}
