package com.stolsvik.mats.impl.jms;

import javax.jms.Connection;
import javax.jms.Session;

import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;

/**
 * Some small specifics if ActiveMQ is the JMS Implementation.
 * <ul>
 * <li>Check for Connection liveliness: {@code ActiveMQConnection.is[Closed|Closing|TransportFailed]}.</li>
 * <li>Honor the {@link MatsRefuseMessageException} (i.e. insta-DLQing), by setting redelivery attempts to 0 when
 * rolling back Session: {@code ActiveMQSession.setRedeliveryPolicy(zeroAttemptsPolicy)}.</li>
 * </ul>
 */
public class JmsMatsActiveMQSpecifics {

    private JmsMatsActiveMQSpecifics() {
        /* utility class */
    }

    private static void initialize() {
        /* TODO: implement */
    }

    public static void isConnectionLive(Connection jmsConnection) throws JmsMatsJmsException {
        /* TODO: implement */
        // ActiveMQConnection.is[Closed|Closing|TransportFailed]
        // throw new JmsMatsJmsException("What was wrong") if any of these are not ok.
        return; // OK (throw if not).
    }

    public static void instaDlq(Session session, JmsMatsJmsExceptionThrowingRunnable runnable) throws JmsMatsJmsException {
        // NOTE: On ActiveMQSession, we have this method:
        // /**
        // * Sets the redelivery policy used when messages are redelivered
        // */
        // public void setRedeliveryPolicy(RedeliveryPolicy redeliveryPolicy) {
        // this.redeliveryPolicy = redeliveryPolicy;
        // }
        /* TODO: implement */
    }

    @FunctionalInterface
    interface JmsMatsJmsExceptionThrowingRunnable {
        void run() throws JmsMatsJmsException;
    }

}
