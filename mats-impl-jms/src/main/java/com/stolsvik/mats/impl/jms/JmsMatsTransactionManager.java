package com.stolsvik.mats.impl.jms;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsStage;
import com.stolsvik.mats.exceptions.MatsConnectionException;
import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler.JmsSessionHolder;
import com.stolsvik.mats.impl.jms.JmsMatsStage.JmsMatsStageProcessor;

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
     * TODO: Fix or move JavaDoc
     *
     * Will be provided to {@link JmsMatsTransactionManager#getTransactionContext(JmsMatsTxContextKey)} to let it have a
     * key from which to decide which level it shall do JMS Connection sharing on (from which to make Sessions). Notice
     * that it is used both for {@link JmsMatsStageProcessor StageProcessors} (in which case {@link #getStage()} returns
     * the stage), and for {@link JmsMatsInitiator Initiators} (in which case said method returns {@code null}).
     * <p>
     * For reference: In the JCA spec, it is specified that one JMS Connection is used per one JMS Session (and in the
     * JMS spec v2.0, this is "formalized" in that in the "simplified JMS API" one have a new class JMSContext, which
     * combines a Connection and Session in one). This will here mean that for each {@link JmsMatsTxContextKey}, there
     * shall be a unique JMS Connection (i.e. each StageProcessor has its own Connection). It does makes some sense,
     * though, that JMS Connections at least are shared for all StageProcessors for a Stage - or even for all
     * StageProcessors for all Stages of an Endpoint. However, sharing one Connection for the entire application (i.e.
     * for all endpoints in the JVM) might be a bit too heavy burden for a single JMS Connection.
     */
    interface JmsMatsTxContextKey {
        /**
         * @return "this" if this is a StageProcessor, <code>null</code> if an Initiator.
         */
        JmsMatsStage<?, ?, ?, ?> getStage();

        /**
         * @return the {@link JmsMatsFactory} of the StageProcessor or Initiator (never <code>null</code>).
         */
        JmsMatsFactory<?> getFactory();
    }

    /**
     * TODO: FIX JAVADOC
     *
     * A transactional context is created for each txContextKey, and then
     * {@link TransactionContext#getTransactionalJmsSession(boolean)} is invoked per {@link JmsMatsStageProcessor}.
     * <p>
     * This invocation should create the JMS Connection, and any problems creating it should raise a
     * {@link MatsConnectionException} - which is considered fatal. Any reconnect-logic is assumed to be found in the
     * JMS implementation's {@link ConnectionFactory} - but handling retry even in face of DNS lookup or authentication
     * failures could conceivably be implemented. Note that in this case this method must nevertheless return a
     * {@link TransactionContext}, but where the the creation of the JMS Connection is deferred to the
     * {@link TransactionContext#getTransactionalJmsSession(boolean)} - which <i>is allowed</i> to block.
     * <p>
     * The {@link JmsMatsStage txContextKey} that is being called for, is provided. The transaction manager is free to
     * implement any kind of scheme it finds fit in regards to JMS Connections and how they are handled - i.e. per
     * txContextKey, or per endpoint.
     *
     * @param txContextKey
     *            for which {@link JmsMatsStage} or {@link JmsMatsInitiator} this request for {@link TransactionContext}
     *            is for.
     * @return a {@link TransactionContext} for the supplied txContextKey.
     */
    TransactionContext getTransactionContext(JmsMatsTxContextKey txContextKey);

    /**
     * Implementors shall do the transactional processing and handle any Throwable that comes out by rolling back.
     */
    @FunctionalInterface
    interface TransactionContext {
        /**
         * Shall open relevant transactions (that are not already opened by means of JMS's "always in transaction" for
         * transactional Connections), perform the provided lambda, and then commit the transactions (including the JMS
         * {@link Session}).
         * <p>
         * If <i>any</i> Exception occurs when executing the provided lambda, then the transactions should be rolled
         * back - but if it is the declared special {@link MatsRefuseMessageException}, then the implementation should
         * also try to ensure that the underlying JMS Message is not redelivered (no more retries), but instead put on
         * the DLQ right away. <i>(Beware of "sneaky throws": The JVM bytecode doesn't care whether a method declares an
         * exception or not: It is possible to throw a checked exception form a method that doesn't declare it in
         * several different ways. Groovy is nasty here (as effectively everything is RuntimeException in Groovy world),
         * and also google "sneakyThrows" for a way to do it using "pure java" that was invented with Generics.)</i>
         *  @param jmsSessionHolder
         *            the JMS Session upon which this transaction should run. Gotten from
         *            {@link JmsMatsJmsSessionHandler#getSessionHolder(JmsMatsStageProcessor)} or
         *            {@link JmsMatsJmsSessionHandler#getSessionHolder(JmsMatsInitiator)}.
         * @param lambda
         *            the stuff that shall be done within transaction, i.e. the {@link MatsStage} or the
         *            {@link MatsInitiator}.
         */
        void doTransaction(JmsSessionHolder jmsSessionHolder, ProcessingLambda lambda) throws JmsMatsJmsException;
    }

    /**
     * The lambda that is provided to the {@link JmsMatsTransactionManager} for it to provide transactional demarcation
     * around.
     */
    @FunctionalInterface
    interface ProcessingLambda {
        void performWithinTransaction() throws JmsMatsJmsException, MatsRefuseMessageException;
    }
}
