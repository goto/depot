package com.gotocompany.depot.http.request;

import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.message.Message;

import java.util.List;

/**
 * Strategy that converts a batch of consumed messages into executable HTTP request records.
 *
 * <p>This abstraction is the heart of the HTTP sink's request-building stage. It hides whether the
 * sink emits one request per message or aggregates many messages into a single request: the concrete
 * implementations {@link SingleRequest} and {@link BatchRequest} embody those two modes, and
 * {@link RequestFactory} selects the appropriate one based on configuration. Implementations are also
 * responsible for capturing per-message construction failures as invalid
 * {@link HttpRequestRecord}s rather than throwing, so that one malformed message does not abort the
 * whole batch.</p>
 *
 * @see SingleRequest
 * @see BatchRequest
 * @see RequestFactory
 * @see HttpRequestRecord
 */
public interface Request {
    /**
     * Builds HTTP request records from the supplied messages.
     *
     * <p>The returned list contains a mix of valid records (carrying an executable request) and
     * invalid records (carrying error details for messages that could not be converted). Depending on
     * the implementation, the number of records may equal the number of messages (single mode) or be
     * far smaller (batch mode, where valid messages are coalesced into one record).</p>
     *
     * @param messages the batch of messages to convert into requests
     * @return the list of resulting request records, both valid and invalid
     */
    List<HttpRequestRecord> createRecords(List<Message> messages);

}
