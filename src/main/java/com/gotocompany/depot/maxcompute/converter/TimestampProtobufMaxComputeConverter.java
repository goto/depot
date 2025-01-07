package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public class TimestampProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private final TimestampNTZProtobufMaxComputeConverter timestampNtzProtobufMaxComputeConverter;

    public TimestampProtobufMaxComputeConverter(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.timestampNtzProtobufMaxComputeConverter = new TimestampNTZProtobufMaxComputeConverter(maxComputeSinkConfig);
    }

    @Override
    public TypeInfo convertSingularTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        return TypeInfoFactory.TIMESTAMP;
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        return Timestamp.valueOf((LocalDateTime) timestampNtzProtobufMaxComputeConverter.convertSingularPayload(protoPayload));
    }

}
