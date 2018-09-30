package com.stolsvik.mats.impl.jms;

/**
 * Thrown if anything goes haywire with the backend implementation, e.g. that any of the numerous exception-throwing
 * methods of the JMS API actually does throw any (unexpected) Exception.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class JmsMatsJmsException extends Exception {
    public JmsMatsJmsException(String message) {
        super(message);
    }

    public JmsMatsJmsException(String message, Throwable cause) {
        super(message, cause);
    }
}
