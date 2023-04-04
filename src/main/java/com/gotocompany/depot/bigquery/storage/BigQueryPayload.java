package com.gotocompany.depot.bigquery.storage;

import com.gotocompany.depot.bigquery.storage.proto.BigQueryRecordMeta;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public interface BigQueryPayload extends Iterable<BigQueryRecordMeta> {
    List<BigQueryRecordMeta> RECORD_METADATA = new ArrayList<>();

    default void addMetadataRecord(BigQueryRecordMeta record) {
        RECORD_METADATA.add(record);
    }

    default Iterator<BigQueryRecordMeta> iterator() {
        return RECORD_METADATA.iterator();
    }

    Object getPayload();

    void setPayload(Object payload);
}
