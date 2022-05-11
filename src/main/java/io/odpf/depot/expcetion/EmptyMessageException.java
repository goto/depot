package io.odpf.depot.expcetion;

/**
 * Empty thrown when the message is contains zero bytes.
 */
public class EmptyMessageException extends DeserializerException {
    public EmptyMessageException() {
        super("log message is empty");
    }
}