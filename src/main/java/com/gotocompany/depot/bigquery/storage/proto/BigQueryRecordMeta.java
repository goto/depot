package com.gotocompany.depot.bigquery.storage.proto;

import com.gotocompany.depot.error.ErrorInfo;
import lombok.AllArgsConstructor;

import java.util.Map;

@AllArgsConstructor
public class BigQueryRecordMeta {
    private final Map<String, Object> metadata;
    private final long index;
    private final ErrorInfo errorInfo;
    private final boolean isValid;
}
