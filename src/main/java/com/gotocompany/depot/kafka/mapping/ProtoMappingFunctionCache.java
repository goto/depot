package com.gotocompany.depot.kafka.mapping;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe holder for the currently active proto mapping function.
 *
 * <p>The cached function is replaced atomically whenever the source or sink proto schemas are refreshed,
 * so concurrent reads from the sink hot path always observe a fully built mapping function.
 */
public class ProtoMappingFunctionCache {

    private final AtomicReference<ProtoMappingFunction> mappingFunction = new AtomicReference<>();

    /**
     * Returns the currently cached mapping function.
     *
     * @return the active mapping function
     * @throws IllegalStateException if no mapping function has been initialized yet
     */
    public ProtoMappingFunction get() {
        ProtoMappingFunction function = mappingFunction.get();
        if (function == null) {
            throw new IllegalStateException("proto mapping function has not been initialized for the kafka sink");
        }
        return function;
    }

    /**
     * Atomically replaces the cached mapping function.
     *
     * @param function the new mapping function to cache
     */
    public void set(ProtoMappingFunction function) {
        mappingFunction.set(function);
    }
}
