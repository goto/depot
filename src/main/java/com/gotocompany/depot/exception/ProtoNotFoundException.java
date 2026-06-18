package com.gotocompany.depot.exception;

/**
 * Signals that a Protobuf descriptor could not be located for a configured message class.
 *
 * <p>When Depot resolves a Protobuf schema it looks up the descriptor for the fully qualified message
 * class named in configuration. If no matching descriptor can be found, typically because the class
 * name is misspelled, the generated class is not on the classpath, or the Stencil schema registry
 * does not serve it, this exception is thrown to abort the lookup.
 *
 * <p>This is an unchecked exception that usually reflects a configuration or packaging problem and is
 * not resolved by retrying.
 */
public class ProtoNotFoundException extends RuntimeException {
    /**
     * Creates a {@code ProtoNotFoundException} with the supplied detail message.
     *
     * @param message human-readable description of the lookup failure, typically naming the Protobuf
     *                class or schema that could not be found
     */
    public ProtoNotFoundException(String message) {
        super(message);
    }
}
