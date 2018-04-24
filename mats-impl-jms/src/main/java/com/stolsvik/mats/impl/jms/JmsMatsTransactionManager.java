package com.stolsvik.mats.impl.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsStage;
import com.stolsvik.mats.exceptions.MatsConnectionException;
import com.stolsvik.mats.exceptions.MatsRefuseMessageException;
import com.stolsvik.mats.impl.jms.JmsMatsStage.StageProcessor;

/**
 * Transactional aspects of the JMS MATS implementation. (It is the duty of the MATS implementation to ensure that the
 * transactional principles of MATS are honored).
 * <p>
 * The reason for this being an interface, is that the transactional aspects can be implemented through different means.
 * Specifically, there is a direct implementation, and the intention is to also have a Spring-specific implementation.
 *
 * @author Endre Stølsvik - 2015-11-04 - http://endre.stolsvik.com
 */
public interface JmsMatsTransactionManager {

    /**
     * A transactional context is created for each stage, and then
     * {@link TransactionContext#getTransactionalJmsSession(boolean)} is invoked per {@link StageProcessor}.
     * <p>
     * This invocation should create the JMS Connection, and any problems creating it should raise a
     * {@link MatsConnectionException} - which is considered fatal. Any reconnect-logic is assumed to be found in the
     * JMS implementation's {@link ConnectionFactory} - but handling retry even in face of DNS lookup or authentication
     * failures could conceivably be implemented. Note that in this case this method must nevertheless return a
     * {@link TransactionContext}, but where the the creation of the JMS Connection is deferred to the
     * {@link TransactionContext#getTransactionalJmsSession(boolean)} - which <i>is allowed</i> to block.
     * <p>
     * The {@link JmsMatsStage stage} that is being called for, is provided. The transaction manager is free to
     * implement any kind of scheme it finds fit in regards to JMS Connections and how they are handled - i.e. per
     * stage, or per endpoint.
     *
     * @param stage
     *            for which {@link JmsMatsStage} this request for {@link TransactionContext} is for. For any
     *            {@link MatsInitiator}, this is <code>null</code>.
     * @return a {@link TransactionContext} for the supplied stage.
     */
    TransactionContext getTransactionContext(JmsMatsStage<?, ?, ?, ?> stage);

    /**
     * One {@link TransactionContext} per {@link JmsMatsStage} - each {@link StageProcessor} will currently not get its
     * own {@link TransactionContext}, read up on {@link #getTransactionalJmsSession(boolean)}.
     */
    interface TransactionContext {
        /**
         * Will be called at top (outside) of the receive()-loop in each of the {@link JmsMatsStage}'s
         * {@link StageProcessor}s. It will first establish the {@link Destination} and the {@link MessageConsumer}, and
         * then go into the receive()-loop. If any JMS method raises JMSException, the session will be closed, and this
         * method will be invoked again. The returned JMS {@link Session} instance will be provided back to the
         * {@link #performWithinTransaction(Session, ProcessingLambda)}.
         * <p>
         * Note: If deferred connectivity establishment is in effect (read
         * {@link JmsMatsTransactionManager#getTransactionContext(JmsMatsStage)} - i.e. when JMS resources are not yet
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
         * If <i>any</i> Exception occurs when executing the provided lambda, then the transactions should be rolled
         * back - but if it is the declared special {@link MatsRefuseMessageException}, then the implementation should
         * also try to ensure that the underlying JMS Message is not redelivered (no more retries), but instead put on
         * the DLQ right away. <i>(Beware of "sneaky throws": The JVM bytecode doesn't care whether a method declares an
         * exception or not: It is possible to throw a checked exception form a method that doesn't declare it in
         * several different ways. Groovy is nasty here (as effectively everything is RuntimeException in Groovy world),
         * and also google "sneakyThrows" for a way to do it using "pure java" that was invented with Generics.)</i>
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
         * Closes this {@link TransactionContext}, invoked when the {@link JmsMatsStage} to which it was requested for
         * is closed. Should close relevant JMS Connections.
         */
        void close();
    }

    /**
     * The lambda that is provided to the {@link JmsMatsTransactionManager} for it to provide transactional demarcation
     * around.
     */
    @FunctionalInterface
    interface ProcessingLambda {
        void performWithinTransaction() throws MatsRefuseMessageException;
    }
}
