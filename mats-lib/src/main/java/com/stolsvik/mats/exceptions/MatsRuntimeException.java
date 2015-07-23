package com.stolsvik.mats.exceptions;

/**
 * Top of Exception hierarchy for exceptions of an non-handleable character.
 * 
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class MatsRuntimeException extends RuntimeException {
    public MatsRuntimeException(String message) {
        super(message);
    }

    public MatsRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
