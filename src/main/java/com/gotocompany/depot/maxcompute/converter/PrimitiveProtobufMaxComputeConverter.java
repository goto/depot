package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.mapper.ProtoPrimitiveDataTypeMapperFactory;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.util.Map;
import java.util.function.Function;

/**
 * Handle the conversion of primitive protobuf types to MaxCompute compatible format.
 *
 * <p>It is built from two lookup tables produced by {@link ProtoPrimitiveDataTypeMapperFactory}: one mapping
 * each Protobuf primitive type to a MaxCompute {@link TypeInfo}, and one mapping each type to a value-mapping
 * function. The exact mappings depend on configuration (for example whether integer types are widened to
 * {@code BIGINT} or floating-point types are converted to {@code DECIMAL}). A single instance is shared by
 * every supported primitive type registered in the {@link MaxComputeProtobufConverterCache}.</p>
 *
 * @see ProtoPrimitiveDataTypeMapperFactory
 * @see ProtobufMaxComputeConverter
 */
public class PrimitiveProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    /**
     * Maps each supported Protobuf primitive type to its MaxCompute {@link TypeInfo}.
     */
    private final Map<Descriptors.FieldDescriptor.Type, TypeInfo> typeInfoMap;
    /**
     * Maps each supported Protobuf primitive type to the function that converts its value for MaxCompute.
     */
    private final Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> payloadMapperMap;

    /**
     * Builds the converter by materializing the type and value lookup tables from configuration.
     *
     * @param maxComputeSinkConfig the sink configuration that selects the primitive type and value mappings
     */
    public PrimitiveProtobufMaxComputeConverter(MaxComputeSinkConfig maxComputeSinkConfig) {
        ProtoPrimitiveDataTypeMapperFactory protoPrimitiveDataTypeMapperFactory = new ProtoPrimitiveDataTypeMapperFactory(maxComputeSinkConfig);
        this.typeInfoMap = protoPrimitiveDataTypeMapperFactory.getProtoTypeMap();
        this.payloadMapperMap = protoPrimitiveDataTypeMapperFactory.getProtoPayloadMapperMap();
    }

    /**
     * Returns the MaxCompute type mapped to the field's primitive Protobuf type.
     *
     * <p>The type is looked up in the configured type table, whose contents reflect options such as integer
     * widening or decimal conversion.</p>
     *
     * @param protoPayload the payload wrapper carrying the field descriptor
     * @return the MaxCompute {@link TypeInfo} mapped to the field's primitive type, or {@code null} if it is not mapped
     */
    @Override
    public TypeInfo convertSingularTypeInfo(ProtoPayload protoPayload) {
        return this.typeInfoMap.get(protoPayload.getFieldDescriptor().getType());
    }

    /**
     * Converts a primitive field value using the function registered for its Protobuf type.
     *
     * @param protoPayload the payload wrapper carrying the field descriptor and the parsed value
     * @return the converted value produced by the registered mapping function
     */
    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        return this.payloadMapperMap.get(protoPayload.getFieldDescriptor().getType()).apply(protoPayload.getParsedObject());
    }

}
