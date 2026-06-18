package com.gotocompany.depot.maxcompute.converter.record;

import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.message.Message;

import java.util.List;

/**
 * Converts a list of messages to RecordWrappers.
 * RecordWrappers encapsulates valid and invalid records.
 * Record is the object used by MaxCompute to represent a row in a table.
 *
 * <p>The result is a {@link RecordWrappers} bundle that separates the records that converted successfully from
 * those that failed, so that the sink can insert the former and report the latter. A MaxCompute {@code Record}
 * is the object that represents a single row of a table.</p>
 *
 * @see RecordWrappers
 * @see ProtoMessageRecordConverter
 */
public interface MessageRecordConverter {
    /**
     * Converts the given messages into MaxCompute records.
     *
     * @param messages the batch of messages to convert; each message keeps its positional index
     * @return a {@link RecordWrappers} bundle holding the valid and invalid records
     */
    RecordWrappers convert(List<Message> messages);
}
