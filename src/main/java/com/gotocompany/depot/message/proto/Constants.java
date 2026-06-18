package com.gotocompany.depot.message.proto;


/**
 * Container for the compile-time string constants shared across the Protobuf message-parsing layer.
 *
 * <p>The constants are organised into nested holder classes by purpose. {@link Config} exposes
 * configuration keys used while generating the JSON column-mapping representation of a Protobuf
 * schema, whereas {@link ProtobufTypeName} exposes the fully qualified, leading-dot Protobuf type
 * names of the well-known wrapper types that the parser treats specially (timestamp, struct and
 * duration).</p>
 *
 * <p>This type is a pure constant holder: it is never instantiated and declares only
 * {@code static} members.</p>
 */
public class Constants {
    /**
     * Configuration keys used while building the JSON column-mapping representation of a Protobuf schema.
     */
    public static class Config {
        /**
         * Key under which a nested message's own field name is recorded inside a generated
         * column-mapping object, allowing a nested record to be associated with the name of the
         * field that contains it.
         *
         * @see ProtoMapper
         */
        public static final String RECORD_NAME = "record_name";
    }

    /**
     * Fully qualified Protobuf type names, in the leading-dot form reported by descriptors, for the
     * well-known types that receive dedicated handling during parsing and nesting detection.
     */
    public static class ProtobufTypeName {
        /**
         * Leading-dot fully qualified type name of {@code google.protobuf.Timestamp}.
         *
         * <p>Fields declared with this type are treated as logical timestamps rather than as ordinary
         * nested messages, for example when {@link ProtoField#isNested()} decides whether to recurse
         * into a field.</p>
         */
        public static final String TIMESTAMP_PROTOBUF_TYPE_NAME = ".google.protobuf.Timestamp";

        /**
         * Leading-dot fully qualified type name of {@code google.protobuf.Struct}.
         *
         * <p>Fields declared with this type are treated as dynamic, JSON-like structures rather than
         * as ordinary nested messages during schema parsing.</p>
         */
        public static final String STRUCT_PROTOBUF_TYPE_NAME = ".google.protobuf.Struct";

        /**
         * Leading-dot fully qualified type name of {@code google.protobuf.Duration}, used to recognise
         * duration-typed fields during conversion.
         */
        public static final String DURATION_PROTOBUF_TYPE_NAME = ".google.protobuf.Duration";
    }
}
