package com.gotocompany.depot;

import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.message.Message;

import java.io.Closeable;
import java.util.List;

/**
 * Core write abstraction implemented by every Depot sink destination.
 *
 * <p>A {@code Sink} represents a single, fully configured connection to one downstream system such
 * as BigQuery, Bigtable, MaxCompute, Redis, an HTTP endpoint, or the application log. It is the
 * boundary through which a host application (for example a Firehose consumer) hands batches of
 * already-consumed records to Depot for delivery. Implementations are not created directly; each
 * sink ships a companion factory (such as {@code BigTableSinkFactory} or {@code LogSinkFactory})
 * that resolves configuration and wires the parser, client, and metrics before returning a
 * ready-to-use instance.</p>
 *
 * <p>The contract is intentionally narrow. Callers repeatedly invoke
 * {@link #pushToSink(java.util.List)} with batches of {@link Message}s and inspect the returned
 * {@link SinkResponse} to discover which records, if any, failed. Per-record problems are reported
 * inside the {@link SinkResponse} rather than thrown, so a partially successful batch never aborts
 * processing; only a failure that makes the whole batch undeliverable is raised as a
 * {@link SinkException}.</p>
 *
 * <p>Because {@code Sink} extends {@link Closeable}, an implementation also owns the lifecycle of the
 * resources it holds — data clients, network connections, and background threads. Callers must
 * invoke {@link #close()} once the sink is no longer needed so those resources are released.
 * Implementations are generally driven by a single delivery loop and are not required to be
 * thread-safe.</p>
 *
 * @see SinkResponse
 * @see Message
 * @see SinkException
 * @see Closeable
 */
public interface Sink extends Closeable {

    /**
     * Delivers a batch of records to the downstream destination and reports per-record outcomes.
     *
     * <p>The supplied messages are processed positionally: the implementation parses, transforms, and
     * writes each {@link Message}, and any record that cannot be delivered is recorded in the returned
     * {@link SinkResponse} keyed by its zero-based index within {@code messages}. A response that
     * reports no errors (see {@link SinkResponse#hasErrors()}) means the entire batch was accepted by
     * the destination.</p>
     *
     * <p>Recoverable, per-record problems — deserialization failures, schema violations,
     * rejected rows, and the like — are surfaced through the {@link SinkResponse} so the caller
     * can divert the offending records (for example to a dead-letter path) while still committing the
     * successes. A thrown {@link SinkException}, in contrast, signals a failure that prevents the
     * batch from being processed as a whole.</p>
     *
     * @param messages the ordered batch of records to write; the position of each message in this
     *     list is the key under which any resulting error is reported
     * @return a {@link SinkResponse} describing which message indexes failed and why, holding no
     *     errors when every record was delivered successfully
     * @throws SinkException if the batch cannot be delivered as a whole, for instance because of an
     *     unrecoverable failure while communicating with the destination
     */
    SinkResponse pushToSink(List<Message> messages) throws SinkException;
}


