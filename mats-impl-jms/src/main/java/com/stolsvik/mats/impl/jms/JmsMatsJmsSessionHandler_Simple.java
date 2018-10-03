package com.stolsvik.mats.impl.jms;

import javax.jms.Connection;
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

    private JmsSessionHolder getSessionHolder_internal(JmsMatsTxContextKey txContextKey) throws JmsMatsJmsException {
        Connection jmsConnection;
        try {
            jmsConnection = _jmsConnectionSupplier.createJmsConnection(txContextKey);
            // Starting it right away, as that could potentially also give "connection establishment" JMSExceptions
            jmsConnection.start();
        }
        catch (Throwable t) {
            throw new JmsMatsJmsException("Got problems when trying to create & start a new JMS Connection.", t);
        }
        try {
            Session jmsSession = jmsConnection.createSession(true, Session.SESSION_TRANSACTED);
            return new JmsSessionHolder_Simple(jmsConnection, jmsSession);
        }
        catch (Throwable t) {
            throw new JmsMatsJmsException("Got problems when trying to create a new JMS Session from JMS Connection ["
                    + jmsConnection + "].", t);
        }
    }

    public static class JmsSessionHolder_Simple implements JmsSessionHolder {
        private static final Logger log = LoggerFactory.getLogger(JmsSessionHolder_Simple.class);

        private final Connection _jmsConnection;
        private final Session _jmsSession;

        public JmsSessionHolder_Simple(Connection jmsConnection, Session jmsSession) {
            _jmsConnection = jmsConnection;
            _jmsSession = jmsSession;
        }

        @Override
        public void isSessionOk() throws JmsMatsJmsException {
            JmsMatsActiveMQSpecifics.isConnectionLive(_jmsConnection);
        }

        @Override
        public Session getSession() {
            if (log.isDebugEnabled()) log.debug("getSession() on SessionHolder [" + this + "]");
            return _jmsSession;
        }

        @Override
        public void close() {
            if (log.isDebugEnabled()) log.debug("close() on SessionHolder [" + this + "] - closing JMS Connection.");
            try {
                _jmsConnection.close();
            }
            catch (Throwable t) {
                log.warn("Got problems when trying to close the JMS Connection.", t);
            }
        }

        @Override
        public void release() {
            if (log.isDebugEnabled()) log.debug("release() on SessionHolder [" + this + "] - closing JMS Connection.");
            try {
                _jmsConnection.close();
            }
            catch (Throwable t) {
                log.warn("Got problems when trying to close the JMS Connection.", t);
            }
        }

        @Override
        public void crashed(Throwable t) {
            if (log.isDebugEnabled()) log.debug("crashed() on SessionHolder [" + this + "] - closing JMS Connection.",
                    t);
            try {
                _jmsConnection.close();
            }
            catch (Throwable t2) {
                log.warn("Got problems when trying to close the JMS Connection due to a \"JMS Crash\" (" + t
                        .getClass().getSimpleName() + ": " + t.getMessage() + ").", t2);
            }
        }
    }

}
