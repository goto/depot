package com.gotocompany.depot;

import com.gotocompany.depot.error.ErrorInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SinkResponse {
    private final Map<Long, ErrorInfo> errors = new HashMap<>();

    /**
     * Returns error as a map whose keys are indexes of messages that failed to be pushed.
     * Each failed message index is associated with a {@link ErrorInfo}.
     */
    public Map<Long, ErrorInfo> getErrors() {
        return errors;
    }

    /**
     * Returns error for the provided message index. If no error exists returns {@code null}.
     */
    public ErrorInfo getErrorsFor(long index) {
        return errors.get(index);
    }

    /**
     * Adds an error for the index.
     */
    public void addErrors(long index, ErrorInfo errorInfo) {
        errors.put(index, errorInfo);
    }

    /**
     * Adds uniform error for the indexes.
     */
    public void addErrors(List<Long> indexes, ErrorInfo errorInfo) {
        indexes.forEach(index -> addErrors(index, errorInfo));
    }

    /**
     * Returns {@code true} if no row insertion failed, {@code false} otherwise. If {@code false}.
     * {@link #getErrors()} ()} returns an empty map.
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }

}
