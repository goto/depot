package com.gotocompany.depot.message.proto;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.ProtoNotFoundException;

import java.util.Map;

/**
 * Builds the recursive {@link ProtoField} schema tree for a Protobuf message type by walking its
 * descriptor and that of every nested message type.
 *
 * <p>Starting from a root type name, the parser visits each field of the corresponding descriptor
 * and, for nested message fields, recurses into the referenced type. Recursion is bounded by a
 * maximum nesting depth (see {@link #MAX_NESTED_SCHEMA_LEVEL}) to guard against unbounded or
 * self-referential schemas. Descriptors are resolved through a {@link DescriptorCache}, so both class
 * names and fully qualified type names are supported.</p>
 *
 * @see ProtoField
 * @see DescriptorCache
 * @see ProtoMessageParser#getProtoField(String)
 */
public class ProtoFieldParser {
    /**
     * We support nested data type of 15 or less level.
     * We limit the fields to only contain schema upto 15 levels deep
     *
     * <p>The parser limits the schema tree to fields nested at most this many levels deep, so
     * recursion into a self-referential type stops once this depth is reached and infinite expansion
     * is prevented. As originally noted, the parser supports nested data types of 15 or fewer levels
     * and limits the fields to only contain schema up to 15 levels deep.</p>
     */
    private static final int MAX_NESTED_SCHEMA_LEVEL = 15;
    /**
     * Cache used to resolve descriptors by class name or fully qualified type name during traversal.
     */
    private final DescriptorCache descriptorCache = new DescriptorCache();

    /**
     * Populates the given {@link ProtoField} with the schema tree of the named Protobuf type.
     *
     * <p>This is the public entry point; it begins traversal at nesting level {@code 1} and delegates
     * to the private recursive overload.</p>
     *
     * @param protoField the (typically empty) root field to populate with child fields
     * @param protoSchema the name of the Protobuf type to describe, resolvable through the descriptor
     *     cache as either a class name or a fully qualified type name
     * @param allDescriptors map of all known descriptors keyed by class or package name
     * @param typeNameToPackageNameMap alias map from fully qualified type name to descriptor key
     * @return the supplied {@code protoField}, now populated with its child fields
     * @throws ProtoNotFoundException if no descriptor can be resolved for {@code protoSchema}
     */
    public ProtoField parseFields(ProtoField protoField, String protoSchema, Map<String, Descriptors.Descriptor> allDescriptors,
                                  Map<String, String> typeNameToPackageNameMap) {
        return parseFields(protoField, protoSchema, allDescriptors, typeNameToPackageNameMap, 1);
    }

    /**
     * Recursively populates a {@link ProtoField} with the schema tree of the named Protobuf type.
     *
     * <p>The descriptor for {@code protoSchema} is resolved through the {@link DescriptorCache}. Each
     * of its fields is wrapped in a new {@link ProtoField}; nested message fields are expanded by
     * recursing into the referenced type at {@code level + 1}. Recursion is bounded by
     * {@link #MAX_NESTED_SCHEMA_LEVEL}: when the descriptor resolved for {@code protoSchema} matches
     * that name as a fully qualified type and {@code level} has reached the maximum, the nested field
     * is skipped (via {@code continue}) instead of being expanded further. Every processed field is
     * appended to {@code protoField}.</p>
     *
     * @param protoField the field to populate with child fields
     * @param protoSchema the name of the Protobuf type to describe at this level
     * @param allDescriptors map of all known descriptors keyed by class or package name
     * @param typeNameToPackageNameMap alias map from fully qualified type name to descriptor key
     * @param level the current nesting depth, starting at {@code 1} for the root type
     * @return the supplied {@code protoField}, now populated with its child fields
     * @throws ProtoNotFoundException if no descriptor can be resolved for {@code protoSchema}
     */
    private ProtoField parseFields(ProtoField protoField, String protoSchema, Map<String, Descriptors.Descriptor> allDescriptors,
                                   Map<String, String> typeNameToPackageNameMap, int level) {

        Descriptors.Descriptor currentProto = descriptorCache.fetch(allDescriptors, typeNameToPackageNameMap, protoSchema);
        if (currentProto == null) {
            throw new ProtoNotFoundException("No Proto found for class " + protoSchema);
        }
        for (Descriptors.FieldDescriptor field : currentProto.getFields()) {
            ProtoField fieldModel = new ProtoField(field.toProto());
            if (fieldModel.isNested()) {
                if (protoSchema.substring(1).equals(currentProto.getFullName())) {
                    if (level >= MAX_NESTED_SCHEMA_LEVEL) {
                        continue;
                    }
                }
                fieldModel = parseFields(fieldModel, field.toProto().getTypeName(), allDescriptors, typeNameToPackageNameMap, level + 1);
            }
            protoField.addField(fieldModel);
        }
        return protoField;
    }
}
