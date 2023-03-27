package com.gotocompany.depot.message;

import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;

import java.io.IOException;
import java.util.Map;

public interface ParsedMessage {
    Object getRaw();

    void validate(SinkConfig config);

    Map<String, Object> getMapping() throws IOException;
    Map<SchemaField, Object> getFields();

    Object getFieldByName(String name);

    Schema getSchema();

    LogicalValue getLogicalValue();
}
