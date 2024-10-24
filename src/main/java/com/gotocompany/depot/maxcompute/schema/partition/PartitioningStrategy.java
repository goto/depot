package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;

public interface PartitioningStrategy<T> {
    String getOriginalPartitionColumnName();
    boolean shouldReplaceOriginalColumn();
    Column getPartitionColumn();
    String getPartitionKey(T t);
}
