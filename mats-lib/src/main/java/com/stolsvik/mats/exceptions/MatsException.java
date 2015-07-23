package com.stolsvik.mats.exceptions;

public class MatsException extends Exception {
    public MatsException(String message) {
        super(message);
    }

    public MatsException(String message, Throwable cause) {
        super(message, cause);
    }
}
