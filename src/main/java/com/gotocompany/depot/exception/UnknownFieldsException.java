package com.gotocompany.depot.exception;


import com.google.protobuf.Message;

/**
 * UnknownFieldsException is thrown when unknown fields is detected on the log message although the proto message was succesfuly parsed.
 * Unknown fields error can happen because multiple causes, and can be handled differently depends on the use case.
 * Unknown fields error by default should be handled by retry the processing because there is a probability that, message deserializer is not updated to the latest schema
 * When consumer is deliberately process message using different schema and intentionally ignore extra fields that missing from descriptor the error handling can be disabled.
 * On some use case that need zero data loss, for example data warehousing unknown fields error should be handled properly to prevent missing fields.
 *
 * <p>A Protobuf payload can be decoded without error yet carry fields that are absent from the
 * descriptor used to parse it. This happens when the producer serialized data using a newer schema
 * than the one available to the consumer, leaving extra (unknown) fields in the parsed message. Depot
 * detects this after parsing and raises this exception so the situation can be handled according to
 * the use case.
 *
 * <p>This is a specialization of {@link DeserializerException} and is therefore an unchecked
 * exception.
 */
public class UnknownFieldsException extends DeserializerException {

    /**
     * Creates an {@code UnknownFieldsException} describing the message that carried unknown fields.
     *
     * <p>The detail message embeds the string representation of the offending message to aid
     * debugging.
     *
     * @param dynamicMessage the parsed Protobuf {@link Message} in which unknown fields were detected;
     *                        its {@code toString()} form is included in the exception message
     */
    public UnknownFieldsException(Message dynamicMessage) {
        super(String.format("unknown fields found, message : %s", dynamicMessage));
    }
}
