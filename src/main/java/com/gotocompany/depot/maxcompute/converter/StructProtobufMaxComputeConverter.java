package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

/**
 * Converts protobuf struct to JSON string.
 *
 * <p>Because a Protobuf struct is schemaless it cannot be mapped onto a fixed MaxCompute struct type. Instead
 * this converter declares the field as a MaxCompute {@code STRING} and renders the value as compact JSON using a
 * shared {@link JsonFormat.Printer} that preserves the original proto field names. If serialization fails the
 * value is converted to an empty string.</p>
 *
 * @see ProtobufMaxComputeConverter
 */
public class StructProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    /**
     * Reusable JSON printer configured to preserve proto field names and emit compact output.
     */
    private final JsonFormat.Printer printer = JsonFormat.printer()
            .preservingProtoFieldNames()
            .omittingInsignificantWhitespace();

    /**
     * Returns the MaxCompute {@code STRING} type used to store the struct as JSON.
     *
     * @param protoPayload the payload wrapper for the field; not inspected because the type is always {@code STRING}
     * @return the MaxCompute {@code STRING} {@link TypeInfo}
     */
    @Override
    public TypeInfo convertSingularTypeInfo(ProtoPayload protoPayload) {
        return TypeInfoFactory.STRING;
    }

    /**
     * Serializes the struct value to a JSON string.
     *
     * <p>Prints the parsed message with the shared printer; if the message cannot be serialized an empty string
     * is returned instead.</p>
     *
     * @param protoPayload the payload wrapper carrying the parsed {@code google.protobuf.Struct} message
     * @return the JSON representation of the struct, or an empty string if serialization fails
     */
    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        try {
            return printer.print((Message) protoPayload.getParsedObject());
        } catch (InvalidProtocolBufferException e) {
            return "";
        }
    }

}
