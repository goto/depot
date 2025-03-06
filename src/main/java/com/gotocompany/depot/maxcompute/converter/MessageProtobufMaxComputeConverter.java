package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.SchemaMismatchException;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import com.gotocompany.depot.utils.ProtoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts google.protobuf.Message to MaxCompute Struct.
 */
@Setter
@Slf4j
public class MessageProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private final MaxComputeProtobufConverterCache maxComputeProtobufConverterCache;
    private final int maxNestedMessageDepth;

    public MessageProtobufMaxComputeConverter(MaxComputeProtobufConverterCache maxComputeProtobufConverterCache,
                                              MaxComputeSinkConfig maxComputeSinkConfig) {
        this.maxComputeProtobufConverterCache = maxComputeProtobufConverterCache;
        if (maxComputeSinkConfig.getMaxNestedMessageDepth() < 1) {
            throw new IllegalArgumentException(String.format("Max nested message depth config (SINK_MAXCOMPUTE_PROTO_MAX_NESTED_MESSAGE_DEPTH) should be greater than 0. Current value: %d",
                    maxComputeSinkConfig.getMaxNestedMessageDepth()));
        }
        this.maxNestedMessageDepth = maxComputeSinkConfig.getMaxNestedMessageDepth() - 1;
    }

    @Override
    public TypeInfo convertTypeInfo(ProtoPayload protoPayload) {
        return maxComputeProtobufConverterCache.getOrCreateTypeInfo(protoPayload, () -> ProtobufMaxComputeConverter.super.convertTypeInfo(protoPayload));
    }

    @Override
    public StructTypeInfo convertSingularTypeInfo(ProtoPayload protoPayload) {
        List<String> fieldNames = protoPayload.getFieldDescriptor().getMessageType().getFields().stream()
                .filter(fd -> shouldIncludeField(protoPayload, fd))
                .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toList());
        List<TypeInfo> typeInfos = protoPayload.getFieldDescriptor()
                .getMessageType()
                .getFields()
                .stream()
                .filter(fd -> shouldIncludeField(protoPayload, fd))
                .map(fd -> {
                    ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fd);
                    return converter.convertTypeInfo(new ProtoPayload(fd, protoPayload.getLevel() + 1));
                })
                .collect(Collectors.toList());
        return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        StructTypeInfo structTypeInfo = (StructTypeInfo) protoPayload.getMaxComputeTypeInfo();
        return new SimpleStruct(structTypeInfo, buildValues((protoPayload)));
    }

    private List<Object> buildValues(ProtoPayload protoPayload) {
        Message dynamicMessage = (Message) protoPayload.getParsedObject();
        StructTypeInfo structTypeInfo = (StructTypeInfo) protoPayload.getMaxComputeTypeInfo();
        if (structTypeInfo.getFieldNames().size() != dynamicMessage.getDescriptorForType().getFields().size()) {
            throw new SchemaMismatchException(String.format("Field count mismatch field name:%s level:%d", structTypeInfo.getTypeName(), protoPayload.getLevel()));
        }
        Object[] values = new Object[structTypeInfo.getFieldCount()];
        Map<String, Integer> fieldNameToIndexMap = maxComputeProtobufConverterCache.getStructFieldTypeIndexMap(structTypeInfo);
        dynamicMessage.getDescriptorForType()
                .getFields()
                .stream()
                .filter(fieldDescriptor -> shouldIncludeField(protoPayload, fieldDescriptor))
                .forEach(innerFieldDescriptor -> {
                    if (ProtoUtils.isNonRepeatedProtoMessage(innerFieldDescriptor) && !dynamicMessage.hasField(innerFieldDescriptor)) {
                        return;
                    }
                    if (ProtoUtils.isNonRepeatedString(innerFieldDescriptor) && !dynamicMessage.hasField(innerFieldDescriptor)) {
                        return;
                    }
                    Integer index = fieldNameToIndexMap.get(innerFieldDescriptor.getName().toLowerCase());
                    if (index == null) {
                        throw new SchemaMismatchException(String.format("Field name mismatch field name:%s level:%d", innerFieldDescriptor.getName(), protoPayload.getLevel()));
                    }
                    TypeInfo typeInfo = structTypeInfo.getFieldTypeInfos().get(index);
                    Object mappedInnerValue = maxComputeProtobufConverterCache.getConverter(innerFieldDescriptor)
                            .convertPayload(new ProtoPayload(innerFieldDescriptor, dynamicMessage.getField(innerFieldDescriptor), protoPayload.getLevel() + 1, typeInfo));
                    values[index] = mappedInnerValue;
                });
        return Arrays.asList(values);
    }

    private boolean shouldIncludeField(ProtoPayload protoPayload, Descriptors.FieldDescriptor fd) {
        boolean shouldInclude = protoPayload.getLevel() != maxNestedMessageDepth || fd.getType() != Descriptors.FieldDescriptor.Type.MESSAGE;
        if (!shouldInclude) {
            log.warn("Skipping field {} at level {} because it exceeds the max nested message depth", fd.getName(), protoPayload.getLevel());
        }
        return shouldInclude;
    }

}
