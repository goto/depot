package com.gotocompany.depot.message.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.exception.UnknownFieldsException;
import com.gotocompany.depot.message.LogicalValue;
import com.gotocompany.depot.message.MessageUtils;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.proto.ProtoSchema;
import com.gotocompany.depot.schema.proto.ProtoSchemaField;
import com.gotocompany.depot.utils.ProtoUtils;
import com.jayway.jsonpath.Configuration;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Slf4j
public class ProtoParsedMessage implements ParsedMessage {

    private static final Configuration JSON_PATH_CONFIG = Configuration.builder()
            .jsonProvider(new ProtoJsonProvider())
            .build();
    private final Message dynamicMessage;

    public ProtoParsedMessage(DynamicMessage dynamicMessage) {
        this.dynamicMessage = dynamicMessage;
    }

    public ProtoParsedMessage(Message dynamicMessage) {
        this.dynamicMessage = dynamicMessage;
    }

    public String toString() {
        return dynamicMessage.toString();
    }

    @Override
    public Object getRaw() {
        return dynamicMessage;
    }

    @Override
    public void validate(SinkConfig config) {
        if (!config.getSinkConnectorSchemaProtoAllowUnknownFieldsEnable() && ProtoUtils.hasUnknownField(dynamicMessage)) {
            log.error("Unknown fields {}", UnknownProtoFields.toString(dynamicMessage.toByteArray()));
            throw new UnknownFieldsException(dynamicMessage);
        }
    }

    private Object getProtoValue(Descriptors.FieldDescriptor fd, Object value) {
        switch (fd.getJavaType()) {
            case ENUM:
                return value.toString();
            case MESSAGE:
                return new ProtoParsedMessage((Message) value);
            default:
                return value;
        }
    }

    @Override
    public Map<SchemaField, Object> getFields() {
        Map<Descriptors.FieldDescriptor, Object> allFields = new TreeMap<>(dynamicMessage.getAllFields());
        for (Descriptors.FieldDescriptor field : dynamicMessage.getDescriptorForType().getFields()) {
            if (!field.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.ENUM)) {
                continue;
            }
            if (!allFields.containsKey(field)) {
                allFields.put(field, dynamicMessage.getField(field));
            }
        }
        return allFields.entrySet().stream().collect(Collectors.toMap(e -> new ProtoSchemaField(e.getKey()), e -> {
            Object value = e.getValue();
            Descriptors.FieldDescriptor key = e.getKey();
            if (key.isRepeated()) {
                return ((List<Object>) value).stream().map(v -> getProtoValue(key, v)).collect(Collectors.toList());
            }
            return getProtoValue(key, value);
        }));
    }

    public Object getFieldByName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Invalid field config : name can not be empty");
        }
        return MessageUtils.getFieldFromJsonObject(name, dynamicMessage, JSON_PATH_CONFIG);
    }

    @Override
    public Schema getSchema() {
        return new ProtoSchema(dynamicMessage.getDescriptorForType());
    }

    @Override
    public LogicalValue getLogicalValue() {
        return new ProtoLogicalValue(dynamicMessage);
    }
}
