package com.stolsvik.mats.impl.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.exceptions.MatsConnectionException;
import com.stolsvik.mats.exceptions.MatsRefuseMessageException;

/**
 * Implementation of {@link JmsMatsTransactionManager} handling both JMS Session and any provided JDBC DataSource, doing
 * all transactional handling "native", i.e. using only JMS and JDBC APIs (as opposed to using Spring). Each
 * {@link #getTransactionContext(JmsMatsStage) transactional context} (stages' processors, and initiators) gets its
 * own {@link Connection JMS Connection}.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class JmsMatsTransactionManager_Standalone implements JmsMatsTransactionManager, JmsMatsStatics {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsTransactionManager_Standalone.class);

    private final ConnectionFactory _jmsConnectionFactory;

    public static final JmsMatsTransactionManager create(ConnectionFactory jmsConnectionFactory) {
        return new JmsMatsTransactionManager_Standalone(jmsConnectionFactory);
    }

    private JmsMatsTransactionManager_Standalone(ConnectionFactory jmsConnectionFactory) {
        _jmsConnectionFactory = jmsConnectionFactory;
    }

    @Override
    public TransactionContext getTransactionContext(JmsMatsStage<?, ?, ?> stage) {
        try {
            Connection jmsConnection = _jmsConnectionFactory.createConnection();
            // Starting it right away, as that could potentially also give "connection establishment" JMSExceptions
            jmsConnection.start();
            return new TransactionalContext_Standalone(jmsConnection);
        }
        catch (JMSException e) {
            throw new MatsConnectionException("Got a JMS Exception when trying to createConnection()"
                    + " on the JMS ConnectionFactory [" + _jmsConnectionFactory + "].", e);
        }
    }

    public static class TransactionalContext_Standalone implements TransactionContext {

        private final Connection _jmsConnection;

        public TransactionalContext_Standalone(Connection jmsConnection) {
            _jmsConnection = jmsConnection;
        }

        @Override
        public Session getTransactionalJmsSession(boolean canBlock) {
            try {
                return _jmsConnection.createSession(true, 0);
            }
            catch (JMSException e) {
                throw new MatsBackendException("Got a JMS Exception when trying to createSession(true, 0)"
                        + " on the JMS Connection [" + _jmsConnection + "].", e);
            }
        }

        @Override
        public void performWithinTransaction(Session jmsSession, ProcessingLambda lambda) throws JMSException {
            try {
                try {
                    lambda.transact();
                    log.info(LOG_PREFIX + "Processing lambda finished, about to commit JMS Session.");
                    jmsSession.commit();
                    log.info(LOG_PREFIX + "JMS Session committed.");
                }
                catch (MatsBackendException e) {
                    /*
                     * This denotes that the ProcessContext has had problems doing JMS stuff, i.e. on initiate from from
                     * outside, or within a stage, requesting, replying, or adding binaries or strings. Sending this on
                     * to the outside catch block, as this means that we have an unstable JMS context.
                     */
                    throw e;
                }
                catch (MatsRefuseMessageException e) {
                    log.error(LOG_PREFIX + "Got a MatsRefuseMessageException when processing the stage or initiation"
                            + " (most probably from the user code)."
                            + " Rolling back the JMS transaction - trying to ensure that it goes directly to DLQ.",
                            e);
                    jmsSession.rollback();
                    // TODO: Make ActiveMQ directly to DLQ stuff
                    log.info(LOG_PREFIX + "JMS Session rolled back.");
                }
                catch (Throwable t) {
                    log.error(LOG_PREFIX + "Got some Throwable when processing the stage or initiation (should only"
                            + " be from user code). Rolling back the JMS transaction.", t);
                    jmsSession.rollback();
                    log.info(LOG_PREFIX + "JMS Session rolled back.");
                }
            }
            catch (JMSException e) {
                log.error(LOG_PREFIX + "Got an JMSException when processing the stage or initiation within a"
                        + " transaction."
                        + " Rolling back the JMS transaction, which probably won't work - and then denoting that"
                        + " this TransactionalContext has become broken.", e);
                try {
                    jmsSession.rollback();
                    log.info(LOG_PREFIX + "JMS Session rolled back.");
                }
                catch (Throwable t2) {
                    log.warn(LOG_PREFIX
                            + "Got some throwable when trying to rollback after an unexpected JMSException. This is"
                            + " probably what to expect since the JMS context is crashed.", t2);
                }
                throw e;
            }
        }

        @Override
        public void commitExt() {
            // TODO Auto-generated method stub
        }

        @Override
        public void close() {
            Connection jmsConnection = _jmsConnection;
            if (jmsConnection != null) {
                try {
                    jmsConnection.close();
                }
                catch (Throwable t) {
                    log.error("Got some unexpected exception when trying to close JMS Connection [" + jmsConnection
                            + "].", t);
                }
            }
        }
    }
}
