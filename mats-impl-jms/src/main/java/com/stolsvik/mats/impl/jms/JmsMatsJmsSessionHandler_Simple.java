package com.stolsvik.mats.impl.jms;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.impl.jms.JmsMatsStage.JmsMatsStageProcessor;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.JmsMatsTxContextKey;

/**
 * A dead simple implementation of {@link JmsMatsJmsSessionHandler} which does nothing of pooling nor connection
 * sharing. For StageProcessors (endpoints), this actually is one of the interesting options: Each StageProcessor has
 * its own Connection with a sole Session. But for Initiators, it is pretty bad: Each initiation constructs one
 * Connection (with a sole Session), and then closes the whole thing down.
 */
public class JmsMatsJmsSessionHandler_Simple implements JmsMatsJmsSessionHandler {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsJmsSessionHandler_Simple.class);

    private final JmsConnectionSupplier _jmsConnectionSupplier;

    public JmsMatsJmsSessionHandler_Simple(JmsConnectionSupplier jmsConnectionSupplier) {
        _jmsConnectionSupplier = jmsConnectionSupplier;
    }

    @Override
    public JmsSessionHolder getSessionHolder(JmsMatsInitiator<?> initiator) throws JmsMatsJmsException {
        JmsSessionHolder jmsSessionHolder = getSessionHolder_internal(initiator);
        if (log.isDebugEnabled()) log.debug("getSessionHolder(...) for Initiator [" + initiator + "], returning ["
                + jmsSessionHolder + "].");
        return jmsSessionHolder;
    }

    @Override
    public JmsSessionHolder getSessionHolder(JmsMatsStageProcessor<?, ?, ?, ?> stageProcessor)
            throws JmsMatsJmsException {
        JmsSessionHolder jmsSessionHolder = getSessionHolder_internal(stageProcessor);
        if (log.isDebugEnabled()) log.debug("getSessionHolder(...) for StageProcessor [" + stageProcessor
                + "], returning [" + jmsSessionHolder + "].");
        return jmsSessionHolder;
    }

    private AtomicInteger _numberOfOutstandingConnections = new AtomicInteger(0);

    @Override
    public int closeAllAvailableSessions() {
        /* nothing to do here, as each SessionHolder is an independent connection */
        // Directly return the number of outstanding connections.
        return _numberOfOutstandingConnections.get();
    }

    private JmsSessionHolder getSessionHolder_internal(JmsMatsTxContextKey txContextKey) throws JmsMatsJmsException {
        Connection jmsConnection;
        try {
            jmsConnection = _jmsConnectionSupplier.createJmsConnection(txContextKey);
        }
        catch (Throwable t) {
            throw new JmsMatsJmsException("Got problems when trying to create a new JMS Connection.", t);
        }
        // We now have an extra JMS Connection - "count it"
        _numberOfOutstandingConnections.incrementAndGet();

        // Starting it right away, as that could potentially also give "connection establishment" JMSExceptions
        try {
            jmsConnection.start();
        }
        catch (Throwable t) {
            try {
                jmsConnection.close();
                _numberOfOutstandingConnections.decrementAndGet();
            }
            catch (Throwable t2) {
                log.error("Got " + t2.getClass().getSimpleName() + " when trying to close a JMS Connection after it"
                        + " failed to start. [" + jmsConnection + "]. Ignoring.", t);
            }
            throw new JmsMatsJmsException("Got problems when trying to start a new JMS Connection.", t);
        }

        // ----- The JMS Connection is gotten and started.

        // :: Create JMS Session and stick it in a Simple-holder
        Session jmsSession;
        try {
            jmsSession = jmsConnection.createSession(true, Session.SESSION_TRANSACTED);
        }
        catch (Throwable t) {
            try {
                _numberOfOutstandingConnections.decrementAndGet();
                jmsConnection.close();
            }
            catch (Throwable t2) {
                log.error("Got " + t2.getClass().getSimpleName() + " when trying to close a JMS Connection after it"
                        + " failed to create a new Session. [" + jmsConnection + "]. Ignoring.", t2);
            }
            throw new JmsMatsJmsException(
                    "Got problems when trying to create a new JMS Session from a new JMS Connection ["
                            + jmsConnection + "].", t);
        }

        // :: Create The default MessageProducer, and then stick the Session and MessageProducer in a SessionHolder.
        try {
            MessageProducer messageProducer = jmsSession.createProducer(null);
            return new JmsSessionHolder_Simple(jmsConnection, jmsSession, messageProducer);
        }
        catch (Throwable t) {
            try {
                _numberOfOutstandingConnections.decrementAndGet();
                jmsConnection.close();
            }
            catch (Throwable t2) {
                log.error("Got " + t2.getClass().getSimpleName() + " when trying to close a JMS Connection after it"
                        + " failed to create a new MessageProducer from a newly created JMS Session. [" + jmsConnection
                        + ", " + jmsSession + "]. Ignoring.", t2);
            }
            throw new JmsMatsJmsException(
                    "Got problems when trying to create a new MessageProducer from a new JMS Session [" + jmsSession
                            + "] created from a new JMS Connection [" + jmsConnection + "].", t);
        }
    }

    private static final Logger log_holder = LoggerFactory.getLogger(JmsSessionHolder_Simple.class);

    public class JmsSessionHolder_Simple implements JmsSessionHolder {

        private final Connection _jmsConnection;
        private final Session _jmsSession;
        private final MessageProducer _messageProducer;

        public JmsSessionHolder_Simple(Connection jmsConnection, Session jmsSession, MessageProducer messageProducer) {
            _jmsConnection = jmsConnection;
            _jmsSession = jmsSession;
            _messageProducer = messageProducer;
        }

        @Override
        public void isSessionOk() throws JmsMatsJmsException {
            JmsMatsActiveMQSpecifics.isConnectionLive(_jmsConnection);
        }

        @Override
        public Session getSession() {
            if (log_holder.isDebugEnabled()) log_holder.debug("getSession() on SessionHolder [" + this
                    + "], returning directly.");
            return _jmsSession;
        }

        @Override
        public MessageProducer getDefaultNoDestinationMessageProducer() {
            return _messageProducer;
        }

        @Override
        public void close() {
            if (log_holder.isDebugEnabled()) log_holder.debug("close() on SessionHolder [" + this
                    + "] - closing JMS Connection.");
            try {
                _numberOfOutstandingConnections.decrementAndGet();
                _jmsConnection.close();
            }
            catch (Throwable t) {
                log_holder.warn("Got problems when trying to close the JMS Connection.", t);
            }
        }

        @Override
        public void release() {
            if (log_holder.isDebugEnabled()) log_holder.debug("release() on SessionHolder [" + this
                    + "] - closing JMS Connection.");
            try {
                _numberOfOutstandingConnections.decrementAndGet();
                _jmsConnection.close();
            }
            catch (Throwable t) {
                log_holder.warn("Got problems when trying to close the JMS Connection.", t);
            }
        }

        @Override
        public void crashed(Throwable t) {
            if (log_holder.isDebugEnabled()) log_holder.debug("crashed() on SessionHolder [" + this
                    + "] - closing JMS Connection.",
                    t);
            try {
                _numberOfOutstandingConnections.decrementAndGet();
                _jmsConnection.close();
            }
            catch (Throwable t2) {
                log_holder.warn("Got problems when trying to close the JMS Connection due to a \"JMS Crash\" (" + t
                        .getClass().getSimpleName() + ": " + t.getMessage() + ").", t2);
            }
        }
    }

}
