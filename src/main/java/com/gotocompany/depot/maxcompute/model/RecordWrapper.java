package com.gotocompany.depot.maxcompute.model;

import com.aliyun.odps.data.Record;
import com.gotocompany.depot.error.ErrorInfo;
import lombok.Data;

@Data
public class RecordWrapper {
    private final Record record;
    private final long index;
    private final ErrorInfo errorInfo;
}
