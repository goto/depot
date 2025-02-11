package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import com.gotocompany.depot.maxcompute.util.LocalDateTimeValidator;

import java.sql.Timestamp;

public class TimestampProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private static final String SECONDS = "seconds";
    private static final String NANOS = "nanos";

    private final LocalDateTimeValidator localDateTimeValidator;

    public TimestampProtobufMaxComputeConverter(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.localDateTimeValidator = new LocalDateTimeValidator(maxComputeSinkConfig);
    }

    @Override
    public TypeInfo convertSingularTypeInfo(ProtoPayload protoPayload) {
        return TypeInfoFactory.TIMESTAMP;
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        Message message = (Message) protoPayload.getParsedObject();
        long seconds = (long) message.getField(message.getDescriptorForType().findFieldByName(SECONDS));
        int nanos = (int) message.getField(message.getDescriptorForType().findFieldByName(NANOS));
        return Timestamp.valueOf(localDateTimeValidator.parseAndValidate(seconds, nanos, protoPayload.getFieldDescriptor().getName(), protoPayload.isRootLevel()));
    }

}
