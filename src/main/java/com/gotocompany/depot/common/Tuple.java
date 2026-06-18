package com.gotocompany.depot.common;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Generic, mutable pair holding two related values of independent types.
 *
 * <p>{@code Tuple} is a lightweight container used across Depot wherever a method needs to return or
 * carry two associated values without introducing a dedicated type. Lombok's {@link Data} generates
 * the getters, setters, {@code equals}, {@code hashCode} and {@code toString} methods, while
 * {@link AllArgsConstructor} generates the constructor that accepts both values.
 *
 * @param <T> the type of the first element
 * @param <V> the type of the second element
 */
@Data
@AllArgsConstructor
public class Tuple<T, V> {
    /**
     * The first element of the pair.
     */
    private T first;
    /**
     * The second element of the pair.
     */
    private V second;
}
