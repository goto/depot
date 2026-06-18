package com.gotocompany.depot.bigquery.error;


import com.google.cloud.bigquery.BigQueryError;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ErrorParser determines the {@link ErrorDescriptor} classes error based on the
 * error string supplied.
 *
 * <p>Given the {@code reason} and {@code message} strings BigQuery reports for a failed
 * row, {@link #getError(String, String)} evaluates the known descriptors in a fixed
 * priority order and returns the first one that matches, falling back to
 * {@link UnknownError} when none apply. {@link #parseError(List)} applies the same
 * classification to every {@link BigQueryError} in a list.</p>
 *
 * <p>This class only exposes {@code static} helpers and is not intended to be
 * instantiated.</p>
 *
 * @see ErrorDescriptor
 */
public class ErrorParser {

    /**
     * Classifies a single BigQuery error described by its reason and message.
     *
     * <p>Builds the ordered list of candidate descriptors
     * ({@link InvalidSchemaError}, {@link OOBError} and {@link StoppedError}), returns
     * the first whose {@link ErrorDescriptor#matches()} method evaluates to
     * {@code true}, and otherwise returns an {@link UnknownError} wrapping the same
     * reason and message.</p>
     *
     * @param reasonText the {@code reason} field of the BigQuery error, for example
     *                   {@code "invalid"} or {@code "stopped"}
     * @param msgText    the human readable {@code message} field of the BigQuery error
     * @return the first {@link ErrorDescriptor} that matches the supplied reason and
     *         message, or an {@link UnknownError} when none match
     */
    public static ErrorDescriptor getError(String reasonText, String msgText) {
        List<ErrorDescriptor> errDescList = Arrays.asList(
                new InvalidSchemaError(reasonText, msgText),
                new OOBError(reasonText, msgText),
                new StoppedError(reasonText));

        return errDescList
                .stream()
                .filter(ErrorDescriptor::matches)
                .findFirst()
                .orElse(new UnknownError(reasonText, msgText));
    }

    /**
     * Classifies every error contained in a BigQuery response.
     *
     * <p>Delegates each element to {@link #getError(String, String)} using the error's
     * reason and message, preserving the original ordering.</p>
     *
     * @param bqErrors the list of {@link BigQueryError} instances reported by BigQuery
     * @return a list of {@link ErrorDescriptor} instances, one per input error, in the
     *         same order as {@code bqErrors}
     */
    public static List<ErrorDescriptor> parseError(List<BigQueryError> bqErrors) {
        return bqErrors.stream()
                .map(err -> getError(err.getReason(), err.getMessage()))
                .collect(Collectors.toList());
    }

}
