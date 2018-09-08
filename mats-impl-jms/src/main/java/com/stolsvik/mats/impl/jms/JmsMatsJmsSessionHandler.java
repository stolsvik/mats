package com.stolsvik.mats.impl.jms;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.impl.jms.JmsMatsStage.JmsMatsStageProcessor;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.JmsMatsTxContextKey;

/**
 * Interface for implementing JMS Connection and JMS Session handling. This can implement both different connection
 * sharing mechanisms, and should implement some kind of Session pooling for initiators. It can also implement logic for
 * "is this connection up?" mechanisms, to close the gap whereby a JMS Connection is in a bad state but the application
 * has not noticed this yet.
 * <p>
 * The reason for session pooling for initiators is that a JMS Session can only be used for one thread, and since
 * initiators are shared throughout the code base, one initiator might be used by several threads at the same time.
 */
public interface JmsMatsJmsSessionHandler {

    /**
     * Should be invoked every time an Initiator wants to send a message.
     * 
     * @param initiator
     *            the initiator in question.
     * @return a {@link JmsSessionHolder} instance - which is not the same as any other SessionHolders concurrently in
     *         use (but it may be pooled, so after a {@link JmsSessionHolder#closeOrReturn()}, it may be returned to
     *         another invocation again).
     * @throws MatsBackendException
     *             if there was a problem getting a Connection. Problems getting a Sessions (e.g. the current Connection
     *             is broken) should be internally handled (i.e. try to get a new Connection), except if it can be
     *             determined that the problem getting a Session is of a fundamental nature (i.e. the credentials can
     *             get a Connection, but cannot get a Session - which would be pretty absurd, but hey).
     */
    JmsSessionHolder getSessionHolder(JmsMatsInitiator<?> initiator) throws MatsBackendException;

    /**
     * Will be invoked before the StageProcessor goes into its consumer loop - it will be returned once the Stage is
     * stopped, or if the Session "crashes", i.e. a method on Session or some downstream API throws an Exception.
     * 
     * @param processor
     *            the StageProcessor in question.
     * @return a {@link JmsSessionHolder} instance - which is unique for each call.
     * @throws MatsBackendException
     *             if there was a problem getting a Connection. Problems getting a Sessions (e.g. the current Connection
     *             is broken) should be internally handled (i.e. try to get a new Connection), except if it can be
     *             determined that the problem getting a Session is of a fundamental nature (i.e. the credentials can
     *             get a Connection, but cannot get a Session - which would be pretty absurd, but hey).
     */
    JmsSessionHolder getSessionHolder(JmsMatsStageProcessor<?, ?, ?, ?> processor) throws MatsBackendException;

    /**
     * A "sidecar object" for the JMS Session, so that additional stuff can be bound to it.
     */
    interface JmsSessionHolder {
        /**
         * Will be invoked before committing any resources other than the JMS Session - this is to tighten the gap
         * between typically the DB commit and the JMS commit: Before the DB is committed, an invocation to this method
         * is performed. If this goes OK, then the DB is committed and then the JMS Session is committed.
         * <p>
         * If the connection to the server is not OK, the method shall throw a {@link MatsBackendException}.
         *
         * @param tryHard
         *            if {@code true}, the check should include some kind of round-trip to the server, e.g. send (and
         *            ensure that it is sent, e.g. commit) a message to a topic without consumers.
         */
        void isConnectionLive(boolean tryHard) throws MatsBackendException;

        /**
         * @return the JMS Session. It will be the same instance every time.
         */
        Session getSession();

        /**
         * For StageProcessors, this closes the Session (and when all Sessions for a given Connection is closed, the
         * Connection is closed). For Initiators, this returns the Session to the pool.
         */
        void closeOrReturn();

        /**
         * Notifies that a Session (or "downstream" consumer or producer) raised some exception - probably due to some
         * connectivity issues experienced as a JMSException while interacting with the JMS API, or because the
         * {@link JmsSessionHolder#isConnectionLive(boolean)} method raised {@link MatsBackendException}.
         * <p>
         * This should close and ditch the Session, and the JMS Connection too: Closing all other sessions to this
         * connection, so that the processors and initiators come back by {@link #closeSession() closing} their
         * sessions, and subsequently {@link JmsMatsJmsSessionContext#leaseTransactionalSession() asking for a new}.
         */
        void crashed(Throwable t);
    }

    /**
     * Utility interface for implementors: Abstracts away JMS Connection generation - useful if you need to provide
     * username and password, or some other connection parameters a la for IBM MQ.
     * <p>
     * Otherwise, the lambda can be as simple as
     * <code>(txContextKey) -> _jmsConnectionFactory.createConnection()</code>.
     */
    @FunctionalInterface
    interface JmsConnectionSupplier {
        Connection createJmsConnection(JmsMatsTxContextKey txContextKey) throws JMSException;
    }
}
