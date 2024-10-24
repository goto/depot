package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.data.Record;

import java.util.List;

public interface InsertManager {
    void insert(List<Record> records);
}
