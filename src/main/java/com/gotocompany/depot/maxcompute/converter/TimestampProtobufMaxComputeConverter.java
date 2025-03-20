package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Message;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import com.gotocompany.depot.maxcompute.util.LocalDateTimeValidator;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TimestampProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private static final String SECONDS = "seconds";
    private static final String NANOS = "nanos";

    private final LocalDateTimeValidator localDateTimeValidator;
    private final boolean isIgnoreNegativeSecondTimestampEnabled;

    public TimestampProtobufMaxComputeConverter(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.localDateTimeValidator = new LocalDateTimeValidator(maxComputeSinkConfig);
        this.isIgnoreNegativeSecondTimestampEnabled = maxComputeSinkConfig.isIgnoreNegativeSecondTimestampEnabled();
    }

    @Override
    public Object convertPayload(ProtoPayload protoPayload) {
        if (!protoPayload.getFieldDescriptor().isRepeated()) {
            return convertSingularPayload(protoPayload);
        }
        return ((List<?>) protoPayload.getParsedObject()).stream()
                .map(o -> convertSingularPayload(new ProtoPayload(protoPayload.getFieldDescriptor(), o, protoPayload.getLevel())))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public TypeInfo convertSingularTypeInfo(ProtoPayload protoPayload) {
        return TypeInfoFactory.TIMESTAMP;
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        Message message = (Message) protoPayload.getParsedObject();
        long seconds = (long) message.getField(message.getDescriptorForType().findFieldByName(SECONDS));
        if (seconds < 0 && isIgnoreNegativeSecondTimestampEnabled) {
            return null;
        }
        int nanos = (int) message.getField(message.getDescriptorForType().findFieldByName(NANOS));
        return Timestamp.valueOf(localDateTimeValidator.parseAndValidate(seconds, nanos, protoPayload.getFieldDescriptor().getName(), protoPayload.isRootLevel()));
    }

}
