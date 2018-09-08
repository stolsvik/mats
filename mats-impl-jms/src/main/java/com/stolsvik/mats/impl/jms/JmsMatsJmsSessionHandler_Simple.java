package com.stolsvik.mats.impl.jms;

import javax.jms.Connection;
import javax.jms.Session;

import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.JmsMatsTxContextKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.impl.jms.JmsMatsStage.JmsMatsStageProcessor;

public class JmsMatsJmsSessionHandler_Simple implements JmsMatsJmsSessionHandler {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsJmsSessionHandler_Simple.class);

    private final JmsConnectionSupplier _jmsConnectionSupplier;

    public JmsMatsJmsSessionHandler_Simple(JmsConnectionSupplier jmsConnectionSupplier) {
        _jmsConnectionSupplier = jmsConnectionSupplier;
    }

    @Override
    public JmsSessionHolder getSessionHolder(JmsMatsInitiator<?> initiator) throws MatsBackendException {
        return getSessionHolder_internal(initiator);
    }

    @Override
    public JmsSessionHolder getSessionHolder(JmsMatsStageProcessor<?, ?, ?, ?> stageProcessor) throws MatsBackendException {
        return getSessionHolder_internal(stageProcessor);
    }

    private JmsSessionHolder getSessionHolder_internal(JmsMatsTxContextKey txContextKey) throws MatsBackendException {
        Connection jmsConnection;
        try {
            jmsConnection = _jmsConnectionSupplier.createJmsConnection(txContextKey);
            // Starting it right away, as that could potentially also give "connection establishment" JMSExceptions
            jmsConnection.start();
        }
        catch (Throwable t) {
            throw new MatsBackendException("Got problems when trying to create & start a new JMS Connection.", t);
        }
        try {
            Session jmsSession = jmsConnection.createSession(true, Session.SESSION_TRANSACTED);
            return new JmsSessionHolderImpl(jmsConnection, jmsSession);
        }
        catch (Throwable t) {
            throw new MatsBackendException("Got problems when trying to create a new JMS Session from JMS Connection ["
                    + jmsConnection + "].", t);
        }

    }

    public static class JmsSessionHolderImpl implements JmsSessionHolder {
        private static final Logger log = LoggerFactory.getLogger(JmsSessionHolderImpl.class);

        private final Connection _jmsConnection;
        private final Session _jmsSession;

        public JmsSessionHolderImpl(Connection jmsConnection, Session jmsSession) {
            _jmsConnection = jmsConnection;
            _jmsSession = jmsSession;
        }

        @Override
        public void isConnectionLive(boolean tryHard) throws MatsBackendException {
            JmsMatsActiveMQSpecifics.isConnectionLive(_jmsConnection);
        }

        @Override
        public Session getSession() {
            return _jmsSession;
        }

        @Override
        public void closeOrReturn() {
            try {
                _jmsConnection.close();
            }
            catch (Throwable t) {
                log.warn("Got problems when trying to close the JMS Connection.", t);
            }
        }

        @Override
        public void crashed(Throwable t) {
            try {
                _jmsConnection.close();
            }
            catch (Throwable t2) {
                log.warn("Got problems when trying to close the JMS Connection due to a \"Session Crash\" (" + t
                        .getClass().getSimpleName() + ": " + t.getMessage() + ").", t2);
            }
        }
    }

}
