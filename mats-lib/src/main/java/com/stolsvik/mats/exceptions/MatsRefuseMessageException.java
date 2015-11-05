package com.stolsvik.mats.exceptions;

import com.stolsvik.mats.MatsEndpoint.ProcessLambda;
import com.stolsvik.mats.MatsStage;

/**
 * Can be thrown by any of the {@link ProcessLambda}s of the {@link MatsStage}s to denote that it would prefer this
 * message to be instantly put on a <i>Dead Letter Queue</i>. This is just advisory - the message might still be
 * presented a number of times to the {@link MatsStage} in question (i.e. for the backend-configured number of retries,
 * e.g. default 1 delivery + 5 redeliveries for ActiveMQ).
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class MatsRefuseMessageException extends MatsException {
    public MatsRefuseMessageException(String message, Throwable cause) {
        super(message, cause);
    }

    public MatsRefuseMessageException(String message) {
        super(message);
    }
}
