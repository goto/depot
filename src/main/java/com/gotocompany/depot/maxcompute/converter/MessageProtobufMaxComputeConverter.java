package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import com.gotocompany.depot.utils.ProtoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
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
        Message dynamicMessage = (Message) protoPayload.getParsedObject();
        List<Object> values = new ArrayList<>();
        protoPayload.getFieldDescriptor()
                .getMessageType()
                .getFields()
                .stream()
                .filter(fd -> shouldIncludeField(protoPayload, fd))
                .forEach(innerFieldDescriptor -> {
                    if (ProtoUtils.isNonRepeatedProtoMessage(innerFieldDescriptor) && !dynamicMessage.hasField(innerFieldDescriptor)) {
                        values.add(null);
                        return;
                    }
                    if (ProtoUtils.isNonRepeatedString(innerFieldDescriptor) && !dynamicMessage.hasField(innerFieldDescriptor)) {
                        values.add(null);
                        return;
                    }
                    Object mappedInnerValue = maxComputeProtobufConverterCache.getConverter(innerFieldDescriptor)
                            .convertPayload(new ProtoPayload(innerFieldDescriptor, dynamicMessage.getField(innerFieldDescriptor), protoPayload.getLevel() + 1));
                    values.add(mappedInnerValue);
                });
        TypeInfo typeInfo = convertTypeInfo(protoPayload);
        StructTypeInfo structTypeInfo = (StructTypeInfo) (typeInfo instanceof ArrayTypeInfo ? ((ArrayTypeInfo) typeInfo).getElementTypeInfo() : typeInfo);
        return new SimpleStruct(structTypeInfo, values);
    }

    private boolean shouldIncludeField(ProtoPayload protoPayload, Descriptors.FieldDescriptor fd) {
        boolean shouldInclude = protoPayload.getLevel() != maxNestedMessageDepth || fd.getType() != Descriptors.FieldDescriptor.Type.MESSAGE;
        if (!shouldInclude) {
            log.warn("Skipping field {} at level {} because it exceeds the max nested message depth", fd.getName(), protoPayload.getLevel());
        }
        return shouldInclude;
    }

}
