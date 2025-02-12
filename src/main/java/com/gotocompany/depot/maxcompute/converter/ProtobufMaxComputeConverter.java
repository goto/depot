package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.util.List;
import java.util.stream.Collectors;

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
