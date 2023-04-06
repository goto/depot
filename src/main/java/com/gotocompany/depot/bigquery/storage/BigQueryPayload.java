package com.gotocompany.depot.bigquery.storage;

import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.gotocompany.depot.bigquery.storage.proto.BigQueryRecordMeta;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class BigQueryPayload implements Iterable<BigQueryRecordMeta> {
    private final List<BigQueryRecordMeta> recordMetadata = new ArrayList<>();
    private ProtoRows payload;

    public void addMetadataRecord(BigQueryRecordMeta record) {
        recordMetadata.add(record);
    }

    public Iterator<BigQueryRecordMeta> iterator() {
        return recordMetadata.iterator();
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = (ProtoRows) payload;
    }

}
