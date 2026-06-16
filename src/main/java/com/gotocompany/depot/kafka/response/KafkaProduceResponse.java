package com.gotocompany.depot.kafka.response;

import lombok.Getter;

/**
 * Outcome of producing a single record to Kafka, holding the failure cause when the produce failed.
 */
public final class KafkaProduceResponse {

    @Getter
    private final Throwable error;

    /**
     * Creates a response with the given error.
     *
     * @param error the failure cause, or {@code null} for a successful response
     */
    private KafkaProduceResponse(Throwable error) {
        this.error = error;
    }

    /**
     * Creates a successful response.
     *
     * @return a response with no error
     */
    public static KafkaProduceResponse success() {
        return new KafkaProduceResponse(null);
    }

    /**
     * Creates a failed response.
     *
     * @param error the failure cause
     * @return a response carrying the error
     */
    public static KafkaProduceResponse failure(Throwable error) {
        return new KafkaProduceResponse(error);
    }

    /**
     * Returns whether the produce failed.
     *
     * @return {@code true} if the response carries an error, {@code false} otherwise
     */
    public boolean isFailed() {
        return error != null;
    }

    /**
     * Returns the failure message, or a success marker when there is no error.
     *
     * @return the error message, or {@code "success"} when the produce succeeded
     */
    public String getMessage() {
        return error == null ? "success" : error.getMessage();
    }
}
