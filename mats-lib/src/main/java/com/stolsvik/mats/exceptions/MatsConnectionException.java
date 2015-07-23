package com.stolsvik.mats.exceptions;

import com.stolsvik.mats.MatsInitiator;

/**
 * Thrown by the {@link MatsInitiator#initiate(MatsInitiator.InitiateLambda)}-method if it is not possible to establish
 * a connection to the underlying messaging system, e.g. to ActiveMQ if used in JMS implementation with ActiveMQ as JMS
 * Message Broker.
 * 
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class MatsConnectionException extends MatsRuntimeException {
    public MatsConnectionException(String message) {
        super(message);
    }

    public MatsConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}