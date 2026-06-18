package com.gotocompany.depot.common;

/**
 * Functional interface representing a function that accepts three arguments and produces a result.
 *
 * <p>{@code Function3} is the three-arity extension of {@link java.util.function.BiFunction}, used in
 * Depot where a computation depends on three inputs. As a {@link FunctionalInterface} it can be
 * implemented with a lambda expression or method reference.
 *
 * @param <T> the type of the first argument to the function
 * @param <U> the type of the second argument to the function
 * @param <V> the type of the third argument to the function
 * @param <R> the type of the result of the function
 */
@FunctionalInterface
public interface Function3<T, U, V, R> {
    /**
     * Applies this function to the given three arguments.
     *
     * @param t the first function argument
     * @param u the second function argument
     * @param v the third function argument
     * @return the function result
     */
    R apply(T t, U u, V v);
}
