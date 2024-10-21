package com.gotocompany.depot.maxcompute.record;


import com.aliyun.odps.data.Record;
import com.gotocompany.depot.message.Message;

import java.io.IOException;

public abstract class RecordDecorator {
    private final RecordDecorator decorator;

    public RecordDecorator(RecordDecorator decorator) {
        this.decorator = decorator;
    }

    public void decorate(Record record, Message message) throws IOException {
        append(record, message);
        decorator.decorate(record, message);
    }

    public abstract void append(Record record, Message message) throws IOException;
}
