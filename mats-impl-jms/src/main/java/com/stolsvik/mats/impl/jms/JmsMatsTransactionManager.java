package com.stolsvik.mats.impl.jms;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsStage;
import com.stolsvik.mats.exceptions.MatsConnectionException;
import com.stolsvik.mats.exceptions.MatsRefuseMessageException;

/**
 * Transactional aspects of the JMS implementation. (It is the duty of the MATS implementation to ensure that the
 * transactional principles of MATS are honored).
 * <p>
 * The reason for this being an interface, is that the transactional aspects can be implemented through different means.
 * Specifically, there is a direct implementation, and the intention is to also have a Spring-specific implementation.
 *
 * @author Endre Stølsvik - 2015-11-04 - http://endre.stolsvik.com
 */
public interface JmsMatsTransactionManager {

    /**
     * A transactional context is created for each stage's "processor" - that is, if concurrency is 4, then 4
     * invocations per stage.
     * <p>
     * This invocation should create the JMS Connection, and any problems creating it should raise a
     * {@link MatsConnectionException} - which is considered fatal. Any reconnect-logic is assumed to be found in the
     * JMS implementation's {@link ConnectionFactory} - but handling retry even in face of DNS lookup or authentication
     * failures could conceivably be implemented. Note that in this case this method must nevertheless return a
     * {@link TransactionalContext}, but where the the creation of the JMS Connection is deferred to the
     * {@link TransactionalContext#getTransactionalJmsSession(boolean)} - which <i>is allowed</i> to block.
     * <p>
     * The amount of JMS Connections and how they are handled - i.e. per stage, or per endpoint - is up to the
     * implementation.
     *
     * @param stage
     *            for which {@link JmsMatsStage} this request for {@link TransactionalContext} is for. For
     *            {@link MatsInitiator}s, this is <code>null</code>.
     * @return a {@link TransactionalContext} for the supplied stage.
     */
    TransactionalContext getTransactionalContext(JmsMatsStage<?, ?, ?> stage);

    /**
     * One {@link TransactionalContext} per {@link JmsMatsStage}'s processors.
     */
    interface TransactionalContext {
        /**
         * Will be called at top (outside) of the "receive" loop in the {@link JmsMatsStage} to establish the
         * {@link Destination} and the {@link MessageConsumer}. This should not be called again unless
         * {@link #performWithinTransaction(Session, ProcessingLambda)} throws {@link JMSException}, in which case a new
         * JMS Session is in effect, and the {@link Destination} and {@link MessageConsumer} must be created anew.
         * <p>
         * Note: If deferred connectivity establishment is in effect (read
         * {@link JmsMatsTransactionManager#getTransactionalContext(JmsMatsStage)} - i.e. when JMS resources are not yet
         * available), this method is allowed to block <b>if</b> <code>canBlock</code> is true, as it then is invoked
         * within the stage's consume thread.
         *
         * @param canBlock
         *            whether this invocation is allowed to block, or rather should throw
         *            {@link MatsConnectionException} right away if there are any problems. Relevant for deferred
         *            connectivity establishment.
         *
         * @return a JMS {@link Session} instance that is set up with correct transactional settings.
         */
        Session getTransactionalJmsSession(boolean canBlock);

        /**
         * Shall open relevant transactions (that are not already opened by means of
         * {@link #getTransactionalJmsSession(boolean)}), perform the provided lambda, and then commit the transactions
         * (including the JMS {@link Session}).
         * <p>
         * If <i>any</i> Exception occurs when executing the lambda, then the transactions should be rolled back - but
         * if it is a {@link MatsRefuseMessageException}, then the implementation should also try to ensure that the
         * underlying JMS Message is not redelivered (no more retries), but instead put on the DLQ right away.
         *
         * @param jmsSession
         *            the JMS Session upon which this transaction should run. Gotten from
         *            {@link #getTransactionalJmsSession(boolean)}.
         * @param lambda
         *            the stuff that shall be done within transaction, i.e. the {@link MatsStage} or the
         *            {@link MatsInitiator}.
         */
        void performWithinTransaction(Session jmsSession, ProcessingLambda lambda) throws JMSException;

        /**
         * Will commit external resources (which in roughly 100% of setups means <i>the database(s)</i>), and then open
         * a new transaction for these resources right away. Meant to enable for the user code a way to create a
         * "channel" / "double delivery catcher".
         * <p>
         * TODO: Reconsider - probably just as well the user creates his own SQL connection (an extra DataSource) and
         * uses that for "gate keeping" on double-deliveries. ALSO, one of the best way to ensure this, is to have a
         * table with a unique-constraint on row with an ID being an UUID <i>that the requestor</i> generates. This way,
         * a double-delivery will be caught on the attempted insert of the row, before the e.g. email is sent out.
         */
        void commitExt();

        /**
         * Closes this {@link TransactionalContext}, invoked when the {@link JmsMatsStage} to which it was requested for
         * is closed. Should close e.g. relevant JMS Connections.
         */
        void close();
    }

    @FunctionalInterface
    interface ProcessingLambda {
        void transact() throws JMSException, MatsRefuseMessageException, JmsMatsUserProcessingRaisedExceptionException;
    }

    /**
     * If the {@link JmsMatsStage} must start the thread anew due to some unexpected stuff happening within the
     * transactional processing. Should only happen if {@link JMSException} is raised within
     * {@link TransactionalContext}, i.e. that some unexpected JMS-stuff happens, signifying that the processor/context
     * is fubarred.
     */
    // public static class JmsMatsProcessingContextBrokenException extends Exception {
    // public JmsMatsProcessingContextBrokenException(String message, Throwable cause) {
    // super(message, cause);
    // }
    // }

    /**
     * If the actual user-provided code of the mats stage raises exception.
     */
    public static class JmsMatsUserProcessingRaisedExceptionException extends Exception {
        public JmsMatsUserProcessingRaisedExceptionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
