package com.gotocompany.depot.bigquery.storage;

import com.gotocompany.depot.bigquery.storage.proto.BigQueryRecordMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class BigQueryPayload implements Iterable<BigQueryRecordMeta> {
    private final List<BigQueryRecordMeta> recordMetadata = new ArrayList<>();
    private final Map<Long, Long> paylodIndexToInputIndexMapping = new HashMap<>();
    private Object payload;

    public void addMetadataRecord(BigQueryRecordMeta record) {
        recordMetadata.add(record);
    }

    public void putValidIndexToInputIndex(long validIndex, long inputIndex) {
        paylodIndexToInputIndexMapping.put(validIndex, inputIndex);
    }

    public long getInputIndex(long payloadIndex) {
        return paylodIndexToInputIndexMapping.get(payloadIndex);
    }

    public Iterator<BigQueryRecordMeta> iterator() {
        return recordMetadata.iterator();
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

}
