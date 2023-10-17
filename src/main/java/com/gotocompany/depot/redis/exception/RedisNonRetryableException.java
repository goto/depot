package com.gotocompany.depot.redis.exception;

public class RedisNonRetryableException extends RuntimeException {

    public RedisNonRetryableException(String messsage) {
        super(messsage);
    }

}
