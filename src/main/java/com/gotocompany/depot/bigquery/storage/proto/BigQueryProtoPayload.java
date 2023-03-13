package com.gotocompany.depot.bigquery.storage.proto;

import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;


public class BigQueryProtoPayload implements BigQueryPayload {
    private ProtoRows payload;

    @Override
    public Object getPayload() {
        return payload;
    }

    @Override
    public void setPayload(Object payload) {
        this.payload = (ProtoRows) payload;
    }

}
