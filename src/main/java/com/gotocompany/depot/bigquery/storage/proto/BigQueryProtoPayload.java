package com.gotocompany.depot.bigquery.storage.proto;

import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigQueryProtoPayload implements BigQueryPayload {
    private ProtoRows protoRows;

    public ProtoRows getPayload() {
        return protoRows;
    }
}
