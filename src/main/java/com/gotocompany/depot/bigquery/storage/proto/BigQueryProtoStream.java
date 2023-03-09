package com.gotocompany.depot.bigquery.storage.proto;

import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.gotocompany.depot.bigquery.storage.BigQueryStream;
import lombok.Getter;

public class BigQueryProtoStream implements BigQueryStream {

    @Getter
    private final StreamWriter streamWriter;

    public BigQueryProtoStream(StreamWriter streamWriter) {
        this.streamWriter = streamWriter;
    }
}
