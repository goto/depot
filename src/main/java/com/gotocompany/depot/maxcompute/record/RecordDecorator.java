package com.gotocompany.depot.maxcompute.record;


import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.message.Message;

import java.io.IOException;

/**
 * Decorator to process the record and message.
 *
 * <p>Concrete decorators follow the decorator / chain-of-responsibility pattern: each instance may
 * hold a reference to a nested (downstream) decorator supplied at construction time. Calling
 * {@link #decorate(RecordWrapper, Message)} runs this decorator's own
 * {@link #process(RecordWrapper, Message)} step and then forwards the result through the nested
 * decorator, if any, so a single call walks the entire chain. Depot wires the data-column decorator
 * and the optional metadata-column decorator together this way.</p>
 *
 * <p>Implementations are expected to hold no per-record state (the per-call state lives in the
 * {@link RecordWrapper}) and are reused across many messages.</p>
 *
 * @see ProtoDataColumnRecordDecorator
 * @see ProtoMetadataColumnRecordDecorator
 * @see RecordDecoratorFactory
 */
public abstract class RecordDecorator {

    /**
     * The next decorator in the chain to which processing is delegated after this decorator runs, or
     * {@code null} when this decorator is the last (innermost) link.
     */
    private final RecordDecorator decorator;

    /**
     * Creates a decorator that optionally delegates to a nested decorator once its own processing has
     * run.
     *
     * @param decorator the next decorator to invoke after this one, or {@code null} if this decorator
     *        terminates the chain
     */
    public RecordDecorator(RecordDecorator decorator) {
        this.decorator = decorator;
    }

    /**
     * Decorate the record with the message.
     * If a nested decorator is present, it will be called to decorate the record.
     *
     * @param recordWrapper record to be decorated
     * @param message depot message to be used for decoration
     * @return decorated record
     * @throws IOException if an error occurs while processing the record
     */
    public RecordWrapper decorate(RecordWrapper recordWrapper, Message message) throws IOException {
        if (decorator != null) {
            return decorator.decorate(process(recordWrapper, message), message);
        }
        return process(recordWrapper, message);
    }

    /**
     * Applies this decorator's specific transformation to the given record using the supplied
     * message.
     *
     * <p>Implementations populate one aspect of the record (for example its data columns or its
     * metadata columns) and return a wrapper holding the updated record. This method performs only
     * the local step; chaining to downstream decorators is handled by
     * {@link #decorate(RecordWrapper, Message)}.</p>
     *
     * @param recordWrapper the record to be processed
     * @param message the Depot message providing the data used for processing
     * @return a record wrapper containing the record updated by this decorator
     * @throws IOException if an error occurs while processing the record or message
     */
    public abstract RecordWrapper process(RecordWrapper recordWrapper, Message message) throws IOException;

}
