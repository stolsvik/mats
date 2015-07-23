package com.stolsvik.mats.exceptions;

/**
 * Thrown if anything goes haywire with the backend implementation, e.g. that any of the numerous exception-throwing
 * methods of the JMS API actually does throw any (unexpected) Exception.
 * 
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class MatsBackendException extends MatsRuntimeException {
    public MatsBackendException(String message) {
        super(message);
    }

    public MatsBackendException(String message, Throwable cause) {
        super(message, cause);
    }
}
