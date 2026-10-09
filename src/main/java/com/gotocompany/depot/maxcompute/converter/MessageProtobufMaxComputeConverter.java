package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.ReorderableStruct;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.ProtoFieldConversionException;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import com.gotocompany.depot.utils.ProtoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Converts google.protobuf.Message to MaxCompute Struct.
 */
@Setter
@Slf4j
public class MessageProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private final MaxComputeProtobufConverterCache maxComputeProtobufConverterCache;
    private static final Set<String> WELL_KNOWN_TYPES = new HashSet<>(Arrays.asList(
            "google.protobuf.Timestamp", "google.protobuf.Duration", "google.protobuf.Struct"));
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
        List<Descriptors.FieldDescriptor> includedFields = getIncludedFields(protoPayload);
        List<String> fieldNames = includedFields.stream()
                .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toList());
        List<TypeInfo> typeInfos = includedFields
                .stream()
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
        List<Descriptors.FieldDescriptor> includedFields = getIncludedFields(protoPayload);
        IntStream.range(0, includedFields.size())
                .forEach(index -> {
                    Descriptors.FieldDescriptor innerFieldDescriptor = includedFields.get(index);
                    try {
                        values.add(convertInnerField(protoPayload, dynamicMessage, innerFieldDescriptor));
                    } catch (RuntimeException e) {
                        throw ProtoFieldConversionException.wrap(innerFieldDescriptor.getName(), index, protoPayload.getLevel(), e);
                    }
                });
        TypeInfo typeInfo = convertTypeInfo(protoPayload);
        StructTypeInfo structTypeInfo = (StructTypeInfo) (typeInfo instanceof ArrayTypeInfo ? ((ArrayTypeInfo) typeInfo).getElementTypeInfo() : typeInfo);
        return new ReorderableStruct(structTypeInfo, values);
    }

    private Object convertInnerField(ProtoPayload protoPayload, Message dynamicMessage, Descriptors.FieldDescriptor innerFieldDescriptor) {
        if (dynamicMessage.getField(innerFieldDescriptor).toString().isEmpty()) {
            return null;
        }
        if (ProtoUtils.isNonRepeatedProtoMessage(innerFieldDescriptor) && !dynamicMessage.hasField(innerFieldDescriptor)) {
            return null;
        }
        if (ProtoUtils.isNonRepeatedString(innerFieldDescriptor) && !dynamicMessage.hasField(innerFieldDescriptor)) {
            return null;
        }
        return maxComputeProtobufConverterCache.getConverter(innerFieldDescriptor)
                .convertPayload(new ProtoPayload(innerFieldDescriptor, dynamicMessage.getField(innerFieldDescriptor), protoPayload.getLevel() + 1));
    }

    /**
     * Fields of the message that make it into the struct at this level.
     * Message fields are cut off at max depth, and message fields whose struct would be
     * empty after the cut-off are dropped too, because MaxCompute rejects empty structs.
     */
    private List<Descriptors.FieldDescriptor> getIncludedFields(ProtoPayload protoPayload) {
        List<Descriptors.FieldDescriptor> included = new ArrayList<>();
        List<String> cutOff = new ArrayList<>();
        for (Descriptors.FieldDescriptor fd : protoPayload.getFieldDescriptor().getMessageType().getFields()) {
            if (isRepresentable(fd, protoPayload.getLevel())) {
                included.add(fd);
            } else if (fd.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                cutOff.add(fd.getName());
            }
        }
        if (!cutOff.isEmpty() && log.isDebugEnabled()) {
            log.debug("Omitted fields {} under '{}' at level {}: beyond max nested message depth {} or would be an empty struct",
                    cutOff, protoPayload.getFieldDescriptor().getFullName(), protoPayload.getLevel(), maxNestedMessageDepth + 1);
        }
        return included;
    }

    /**
     * A field is representable if it is not a message, or it is a message that is within depth
     * and still has at least one representable field of its own.
     *
     * @param fd the field
     * @param parentLevel level of the message that owns the field
     */
    private boolean isRepresentable(Descriptors.FieldDescriptor fd, int parentLevel) {
        if (fd.getType() != Descriptors.FieldDescriptor.Type.MESSAGE) {
            return true;
        }
        if (parentLevel >= maxNestedMessageDepth) {
            return false;
        }
        if (WELL_KNOWN_TYPES.contains(fd.getMessageType().getFullName())) {
            return true;
        }
        return fd.getMessageType().getFields().stream().anyMatch(child -> isRepresentable(child, parentLevel + 1));
    }

}
