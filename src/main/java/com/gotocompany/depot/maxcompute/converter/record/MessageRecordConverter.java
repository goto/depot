package com.gotocompany.depot.maxcompute.converter.record;

import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.message.Message;

import java.util.List;

public interface MessageRecordConverter {
    List<RecordWrapper> convert(List<Message> messages);
}
