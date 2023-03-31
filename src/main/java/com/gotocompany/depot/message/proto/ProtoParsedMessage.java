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

    @Override
    public Map<SchemaField, Object> getFields() {
        return dynamicMessage.getAllFields().entrySet().stream().collect(Collectors.toMap(e -> new ProtoSchemaField(e.getKey()), e -> {
            Object value = e.getValue();
            Descriptors.FieldDescriptor key = e.getKey();
            if (key.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.MESSAGE)) {
                if (key.isRepeated()) {
                    return ((List<Message>) value).stream().map(v -> new ProtoParsedMessage(v)).collect(Collectors.toList());
                }
                return new ProtoParsedMessage((Message) value);
            }
            return value;
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
