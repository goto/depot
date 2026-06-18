package com.gotocompany.depot.common;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Mutable value object pairing two {@link String} values.
 *
 * <p>{@code TupleString} is a string-typed counterpart to {@link Tuple} used where two related string
 * values must be carried together, such as a key/value pair produced from configuration parsing.
 * Lombok's {@link Data} generates the getters, setters, {@code equals}, {@code hashCode} and
 * {@code toString} methods, while {@link AllArgsConstructor} generates the constructor that accepts
 * both strings.
 */
@Data
@AllArgsConstructor
public class TupleString {
    /**
     * The first string of the pair.
     */
    private String first;
    /**
     * The second string of the pair.
     */
    private String second;
}
