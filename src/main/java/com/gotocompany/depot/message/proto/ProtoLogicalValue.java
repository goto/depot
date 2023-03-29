package com.gotocompany.depot.message.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.message.LogicalValue;
import com.gotocompany.depot.schema.LogicalType;
import com.gotocompany.depot.schema.proto.ProtoSchema;
import org.json.JSONObject;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtoLogicalValue implements LogicalValue {
    private final Message message;

    public ProtoLogicalValue(Message message) {
        this.message = message;
    }

    @Override
    public LogicalType getType() {
        return new ProtoSchema(message.getDescriptorForType()).logicalType();
    }

    @Override
    public Instant getTimestamp() {
        Timestamp timestamp = null;
        try {
            timestamp = Timestamp.parseFrom(message.toByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException(getErrMessage("Timestamp"), e);
        }
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private Object getValue(Value value) {
        switch (value.getKindCase()) {
            case BOOL_VALUE:
                return value.getBoolValue();
            case NUMBER_VALUE:
                return value.getNumberValue();
            case STRING_VALUE:
                return value.getStringValue();
            case STRUCT_VALUE:
                return getStructValue(value.getStructValue());
            case LIST_VALUE:
                return value.getListValue().getValuesList().stream().map(this::getValue).collect(Collectors.toList());
            default:
                return JSONObject.NULL;
        }
    }

    private Map<String, Object> getStructValue(Struct s) {
        Map<String, Value> fieldsMap = s.getFieldsMap();
        return fieldsMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> getValue(e.getValue())));
    }

    @Override
    public Map<String, Object> getStruct() {
        Struct s = null;
        try {
            s = Struct.parseFrom(message.toByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException(getErrMessage("Struct"), e);
        }
        return getStructValue(s);
    }

    @Override
    public Duration getDuration() {
        com.google.protobuf.Duration duration = null;
        try {
            duration = com.google.protobuf.Duration.parseFrom(message.toByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException(getErrMessage("Duration"), e);
        }
        return Duration.ofSeconds(duration.getSeconds(), duration.getNanos());
    }

    private String getErrMessage(String type) {
        return String.format("Failed while deserializing given \"%s\" to %s", message.getDescriptorForType().getFullName(), type);
    }
}
