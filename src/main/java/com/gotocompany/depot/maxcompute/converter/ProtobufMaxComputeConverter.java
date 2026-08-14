package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Strategy that converts a single Protobuf field into its MaxCompute type and value representation.
 *
 * <p>Implementations are the building blocks used by the MaxCompute sink to translate a Protobuf schema and
 * its messages into MaxCompute records. Each implementation handles one category of field (for example
 * primitives, nested messages/structs, and the well-known timestamp and duration types). The interface
 * separates two concerns:</p>
 * <ul>
 *     <li><em>type</em> conversion, which derives the MaxCompute {@link TypeInfo} of a field through
 *     {@link #convertTypeInfo(ProtoPayload)} and {@link #convertSingularTypeInfo(ProtoPayload)};</li>
 *     <li><em>value</em> conversion, which maps an actual field value to the object the MaxCompute SDK expects
 *     through {@link #convertPayload(ProtoPayload)} and {@link #convertSingularPayload(ProtoPayload)}.</li>
 * </ul>
 *
 * <p>The two {@code default} methods transparently handle {@code repeated} fields by wrapping the singular
 * result in an array type, or by mapping each element; implementations therefore only need to provide the
 * singular variants.</p>
 *
 * @see ProtoPayload
 * @see ProtobufConverterOrchestrator
 */
public interface ProtobufMaxComputeConverter {

    /**
     * Converts a Protobuf field descriptor to a MaxCompute TypeInfo.
     * This method wraps the singular type conversion with array type handling if the field is repeated.
     *
     * @param protoPayload the Protobuf payload wrapper containing field descriptor to convert
     * @return the corresponding MaxCompute TypeInfo
     */
    default TypeInfo convertTypeInfo(ProtoPayload protoPayload) {
        TypeInfo typeInfo = convertSingularTypeInfo(protoPayload);
        return protoPayload.getFieldDescriptor().isRepeated() ? TypeInfoFactory.getArrayTypeInfo(typeInfo) : typeInfo;
    }

    /**
     * Converts a singular Protobuf field descriptor to a MaxCompute TypeInfo.
     * This method should be implemented by subclasses to handle specific field types.
     *
     * @param protoPayload the Protobuf payload wrapper containing field descriptor to convert
     * @return the corresponding MaxCompute TypeInfo for the singular field
     */
    TypeInfo convertSingularTypeInfo(ProtoPayload protoPayload);

    /**
     * Converts a proto payload to a format that can be used by the MaxCompute SDK.
     * @param protoPayload the proto payload to convert, containing field descriptor, the actual object and level
     * @return the converted object
     */
    default Object convertPayload(ProtoPayload protoPayload) {
        if (!protoPayload.getFieldDescriptor().isRepeated()) {
            return convertSingularPayload(protoPayload);
        }
        return ((List<?>) protoPayload.getParsedObject()).stream()
                .map(o -> convertSingularPayload(new ProtoPayload(protoPayload.getFieldDescriptor(), o, protoPayload.getLevel())))
                .collect(Collectors.toList());
    }

    /**
     * Converts a singular proto payload to a format that can be used by the MaxCompute SDK.
     * @param protoPayload the proto payload to convert, containing field descriptor, the actual object and level
     * @return the converted object
     */
    Object convertSingularPayload(ProtoPayload protoPayload);

}
