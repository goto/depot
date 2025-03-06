package com.gotocompany.depot.exception;

public class SchemaMismatchException extends NonRetryableException {

    public SchemaMismatchException(String message) {
        super(message);
    }

}
