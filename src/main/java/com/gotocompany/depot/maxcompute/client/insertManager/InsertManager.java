package com.gotocompany.depot.maxcompute.client.insertManager;

import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;

import java.io.IOException;
import java.util.List;

public interface InsertManager {
    void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException;
}
