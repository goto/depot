package com.gotocompany.depot.message;

import com.gotocompany.depot.config.SinkConfig;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Map;

public interface ParsedMessage {
    Object getRaw();

    JSONObject toJson(SinkConfig config);

    void validate(SinkConfig config);

    Map<String, Object> getMapping() throws IOException;

    Object getFieldByName(String name);
}
