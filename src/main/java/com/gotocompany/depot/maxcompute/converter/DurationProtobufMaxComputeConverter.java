package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.ReorderableStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Message;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Converts google.protobuf.Duration to MaxCompute Struct.
 *
 * <p>The duration is represented as a fixed two-member struct, {@code seconds} and {@code nanos}, both typed as
 * MaxCompute {@code BIGINT}. During value conversion the {@code seconds} component is copied as-is and the
 * {@code nanos} component is widened from {@code int} to {@code long} so that both members share the
 * {@code BIGINT} type.</p>
 *
 * @see ProtobufMaxComputeConverter
 */
public class DurationProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    /**
     * Name of the {@code seconds} member within the protobuf duration message and the resulting struct.
     */
    private static final String SECONDS = "seconds";
    /**
     * Name of the {@code nanos} member within the protobuf duration message and the resulting struct.
     */
    private static final String NANOS = "nanos";
    /**
     * Ordered struct member names ({@code seconds} then {@code nanos}).
     */
    private static final List<String> FIELD_NAMES = Arrays.asList(SECONDS, NANOS);
    /**
     * Struct member types, both {@code BIGINT}, aligned positionally with {@link #FIELD_NAMES}.
     */
    private static final List<TypeInfo> TYPE_INFOS = Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.BIGINT);

    /**
     * Returns the MaxCompute struct type describing a duration.
     *
     * @param protoPayload the payload wrapper for the field; not inspected because the struct shape is fixed
     * @return a {@link StructTypeInfo} with {@code seconds} and {@code nanos} {@code BIGINT} members
     */
    @Override
    public TypeInfo convertSingularTypeInfo(ProtoPayload protoPayload) {
        return TypeInfoFactory.getStructTypeInfo(FIELD_NAMES, TYPE_INFOS);
    }

    /**
     * Builds a MaxCompute struct value from a protobuf duration.
     *
     * @param protoPayload the payload wrapper carrying the parsed {@code google.protobuf.Duration} message
     * @return a {@link ReorderableStruct} holding the {@code seconds} and {@code nanos} values
     */
    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        List<Object> values = getValues((Message) protoPayload.getParsedObject());
        return new ReorderableStruct((StructTypeInfo) convertSingularTypeInfo(protoPayload), values);
    }

    /**
     * Extracts the duration's {@code seconds} and {@code nanos} components as the struct's values.
     *
     * <p>The {@code nanos} component is widened from {@code int} to {@code long} so that both struct members are
     * {@code BIGINT}.</p>
     *
     * @param durationMessage the parsed {@code google.protobuf.Duration} message
     * @return a two-element list holding the seconds value followed by the nanos value as a {@code long}
     */
    private static List<Object> getValues(Message durationMessage) {
        List<Object> values = new ArrayList<>();
        Integer nanos = (Integer) durationMessage.getField(durationMessage.getDescriptorForType().findFieldByName(NANOS));
        values.add(durationMessage.getField(durationMessage.getDescriptorForType().findFieldByName(SECONDS)));
        values.add(nanos.longValue());
        return values;
    }

}
