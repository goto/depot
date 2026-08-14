package com.gotocompany.depot.stencil;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.stencil.SchemaUpdateListener;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * Base class for Depot's bridge between the Stencil schema registry and a sink's message parser.
 *
 * <p>Depot resolves Protobuf schemas through Stencil, which can refresh those schemas at runtime.
 * {@code DepotStencilUpdateListener} implements Stencil's
 * {@link com.gotocompany.stencil.SchemaUpdateListener} so it is notified whenever new Protobuf
 * descriptors become available, and it holds the {@link MessageParser} that should observe those
 * updates. Concrete subclasses (one per supported schema source) implement {@link #updateSchema()}
 * to rebuild whatever parser-specific or sink-specific state depends on the current schema.</p>
 *
 * <p>The {@link MessageParser} is supplied after construction through the Lombok-generated setter,
 * which avoids a circular dependency between a parser and the listener it is wired to. Lombok's
 * {@code @Getter} exposes the parser to subclasses and collaborators.</p>
 *
 * @see com.gotocompany.stencil.SchemaUpdateListener
 * @see MessageParser
 */
@Getter
public abstract class DepotStencilUpdateListener implements SchemaUpdateListener {
    /**
     * Parser that should be refreshed when the backing schema changes.
     *
     * <p>Assigned after construction via the Lombok-generated {@code setMessageParser} setter and read
     * through the class-level {@code @Getter}. Holding it here lets {@link #updateSchema()} and
     * {@link #onSchemaUpdate(java.util.Map)} act on the parser when Stencil reports a new schema.</p>
     */
    @Setter
    private MessageParser messageParser;

    /**
     * Callback invoked by the Stencil runtime when Protobuf descriptors are refreshed.
     *
     * <p>The supplied map associates each fully qualified Protobuf class name with its newly resolved
     * {@link com.google.protobuf.Descriptors.Descriptor}. This base implementation is intentionally a
     * no-op; subclasses may override it to react to the raw descriptor set, though schema-dependent
     * rebuilding is more commonly performed in {@link #updateSchema()}.</p>
     *
     * @param newDescriptor the freshly resolved descriptors keyed by their fully qualified class name
     */
    public void onSchemaUpdate(final Map<String, Descriptors.Descriptor> newDescriptor) {
        // default implementation is empty
    }

    /**
     * Rebuilds the schema-dependent state managed by this listener.
     *
     * <p>Implementations are expected to (re)resolve the active schema and refresh whatever derived
     * state the sink relies on — for example reconfiguring the wired {@link MessageParser} — so that
     * subsequent processing uses the latest schema. It is invoked to perform the initial schema setup
     * and again whenever the schema must be reloaded.</p>
     */
    public abstract void updateSchema();
}
