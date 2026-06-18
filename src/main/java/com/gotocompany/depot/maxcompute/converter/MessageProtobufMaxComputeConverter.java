package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.ReorderableStruct;
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
 *
 * <p>A message field becomes a {@link StructTypeInfo} whose members mirror the message's fields, and a message
 * value becomes a {@link ReorderableStruct}. The converter recurses into sub-messages by delegating to the
 * converters held in the {@link MaxComputeProtobufConverterCache}, and it caps recursion at the configured
 * maximum nesting depth: message-typed fields beyond that depth are dropped (and logged) so that mutually
 * recursive schemas cannot produce an unbounded type.</p>
 *
 * <p>During value conversion, fields that are absent or empty are mapped to {@code null}; this includes unset
 * singular sub-messages and unset singular strings.</p>
 *
 * @see MaxComputeProtobufConverterCache
 * @see ProtobufMaxComputeConverter
 */
@Setter
@Slf4j
public class MessageProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    /**
     * Cache used to resolve the converter for each sub-field and to memoize the derived struct types.
     */
    private final MaxComputeProtobufConverterCache maxComputeProtobufConverterCache;
    /**
     * Zero-based nesting level at which message-typed fields are dropped, computed as the configured maximum
     * nested message depth minus one.
     */
    private final int maxNestedMessageDepth;

    /**
     * Creates the message converter and validates the configured nesting depth.
     *
     * <p>The configured maximum nested message depth must be at least {@code 1}. The stored threshold is one less
     * than the configured value, because nesting levels are tracked zero-based.</p>
     *
     * @param maxComputeProtobufConverterCache the cache used to resolve sub-field converters and memoize types
     * @param maxComputeSinkConfig             the sink configuration providing the maximum nested message depth
     * @throws IllegalArgumentException if the configured maximum nested message depth is less than {@code 1}
     */
    public MessageProtobufMaxComputeConverter(MaxComputeProtobufConverterCache maxComputeProtobufConverterCache,
                                              MaxComputeSinkConfig maxComputeSinkConfig) {
        this.maxComputeProtobufConverterCache = maxComputeProtobufConverterCache;
        if (maxComputeSinkConfig.getMaxNestedMessageDepth() < 1) {
            throw new IllegalArgumentException(String.format("Max nested message depth config (SINK_MAXCOMPUTE_PROTO_MAX_NESTED_MESSAGE_DEPTH) should be greater than 0. Current value: %d",
                    maxComputeSinkConfig.getMaxNestedMessageDepth()));
        }
        this.maxNestedMessageDepth = maxComputeSinkConfig.getMaxNestedMessageDepth() - 1;
    }

    /**
     * Returns the struct type for this message field, memoizing it in the converter cache.
     *
     * <p>On a cache miss it delegates to the default
     * {@link ProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} behavior, so repeated message fields are
     * still wrapped as arrays.</p>
     *
     * @param protoPayload the payload wrapper carrying the message field descriptor and nesting level
     * @return the MaxCompute {@link TypeInfo} for the message field; an array type for repeated fields
     */
    @Override
    public TypeInfo convertTypeInfo(ProtoPayload protoPayload) {
        return maxComputeProtobufConverterCache.getOrCreateTypeInfo(protoPayload, () -> ProtobufMaxComputeConverter.super.convertTypeInfo(protoPayload));
    }

    /**
     * Builds the MaxCompute struct type describing this message.
     *
     * <p>Iterates the message's fields, skipping any that exceed the nesting-depth cap, and pairs each retained
     * field name with the {@link TypeInfo} produced by its converter (resolved from the cache) at the next
     * nesting level.</p>
     *
     * @param protoPayload the payload wrapper carrying the message field descriptor and nesting level
     * @return the {@link StructTypeInfo} whose members mirror the included message fields
     */
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

    /**
     * Builds a MaxCompute struct value from a Protobuf message.
     *
     * <p>For each included field it appends a converted value to the struct, mapping absent or empty fields to
     * {@code null} (including unset singular sub-messages and unset singular strings) and otherwise delegating to
     * the field's converter at the next nesting level. The collected values are wrapped in a
     * {@link ReorderableStruct} typed by the struct type derived from {@link #convertTypeInfo(ProtoPayload)},
     * unwrapping the element type when the field is an array.</p>
     *
     * @param protoPayload the payload wrapper carrying the message field descriptor, the parsed message, and the
     *                     nesting level
     * @return a {@link ReorderableStruct} holding the converted field values
     */
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
                    if (dynamicMessage.getField(innerFieldDescriptor).toString().isEmpty()) {
                        values.add(null);
                        return;
                    }
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
        return new ReorderableStruct(structTypeInfo, values);
    }

    /**
     * Decides whether a sub-field should be included given the nesting-depth cap.
     *
     * <p>A field is excluded only when it is a message type located at the maximum allowed nesting level; such a
     * field is skipped and a warning is logged. All other fields are included.</p>
     *
     * @param protoPayload the payload wrapper carrying the current nesting level
     * @param fd           the descriptor of the sub-field being considered
     * @return {@code true} if the field should be included, {@code false} if it exceeds the nesting-depth cap
     */
    private boolean shouldIncludeField(ProtoPayload protoPayload, Descriptors.FieldDescriptor fd) {
        boolean shouldInclude = protoPayload.getLevel() != maxNestedMessageDepth || fd.getType() != Descriptors.FieldDescriptor.Type.MESSAGE;
        if (!shouldInclude) {
            log.warn("Skipping field {} at level {} because it exceeds the max nested message depth", fd.getName(), protoPayload.getLevel());
        }
        return shouldInclude;
    }

}
