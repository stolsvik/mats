package com.stolsvik.mats.impl.jms;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler.JmsSessionHolder;
import com.stolsvik.mats.impl.jms.JmsMatsProcessContext.DoAfterCommitRunnableHolder;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.JmsMatsTxContextKey;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.TransactionContext;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager_Jms.JmsMatsMessageSendException;

/**
 * The JMS implementation of {@link MatsInitiator}.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
class JmsMatsInitiator<Z> implements MatsInitiator, JmsMatsTxContextKey, JmsMatsStatics {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsInitiator.class);

    private final String _name;
    private final JmsMatsFactory<Z> _parentFactory;
    private final JmsMatsJmsSessionHandler _jmsMatsJmsSessionHandler;
    private final TransactionContext _transactionContext;

    public JmsMatsInitiator(String name, JmsMatsFactory<Z> parentFactory,
            JmsMatsJmsSessionHandler jmsMatsJmsSessionHandler,
            JmsMatsTransactionManager jmsMatsTransactionManager) {
        // NOTICE! Due to multi-threading, whereby one Initiator might be used "globally" for e.g. a Servlet Container
        // having 200 threads, we cannot fetch a sole Session for the Initiator to be used for all initiations (as
        // it might be used concurrently by all the 200 Servlet Container threads). Thus, each initiation needs to
        // get hold of its own Session. However, the Sessions should be pooled.

        _name = name;
        _parentFactory = parentFactory;
        _jmsMatsJmsSessionHandler = jmsMatsJmsSessionHandler;
        _transactionContext = jmsMatsTransactionManager.getTransactionContext(this);
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public JmsMatsFactory<Z> getParentFactory() {
        return _parentFactory;
    }

    @Override
    public void initiate(InitiateLambda lambda) throws MatsBackendException, MatsMessageSendException {
        // NOTICE! Due to multi-threading, whereby one Initiator might be used "globally" for e.g. a Servlet Container
        // having 200 threads, we cannot fetch a sole Session for the Initiator to be used for all initiations (as
        // it might be used concurrently by all the 200 Servlet Container threads). Thus, each initiation needs to
        // get hold of its own Session. However, the Sessions should be pooled.

        // TODO / OPTIMIZE: Consider doing lazy init for TransactionContext too
        // as well as being able to "open" again after close? What about introspection/monitoring/instrumenting -
        // that is, "turn off" a MatsInitiator: Either hang requsts, or probably more interesting, fail them. And
        // that would be nice to use "close()" for: As long as it is closed, it can't be used. Need to evaluate.

        Instant startedInstant = Instant.now();
        long nanosStart = System.nanoTime();

        String existingTraceId = MDC.get(MDC_TRACE_ID);

        try { // :: try-finally: Remove MDC_MATS_INITIATE and MDC_TRACE_ID
            MDC.put(MDC_MATS_INITIATE, "true");

            JmsSessionHolder jmsSessionHolder;
            try {
                jmsSessionHolder = _jmsMatsJmsSessionHandler.getSessionHolder(this);
            }
            catch (JmsMatsJmsException e) {
                // Could not get hold of JMS *Connection* - Read the JavaDoc of
                // JmsMatsJmsSessionHandler.getSessionHolder()
                throw new MatsBackendException("Could not get hold of JMS Connection.", e);
            }
            try {
                DoAfterCommitRunnableHolder doAfterCommitRunnableHolder = new DoAfterCommitRunnableHolder();
                JmsMatsMessageContext jmsMatsMessageContext = new JmsMatsMessageContext(jmsSessionHolder, null);
                // ===== Going into Transactional Demarcation
                _transactionContext.doTransaction(jmsMatsMessageContext, () -> {
                    List<JmsMatsMessage<Z>> messagesToSend = new ArrayList<>();

                    JmsMatsInitiate<Z> init = new JmsMatsInitiate<>(_parentFactory, messagesToSend,
                            jmsMatsMessageContext, doAfterCommitRunnableHolder);
                    JmsMatsContextLocalCallback.bindResource(MatsInitiate.class, init);
                    _parentFactory.setCurrentThreadLocalMatsDemarcation(() -> init);

                    InitiateLambda lambdaToInvoke = lambda;

                    lambdaToInvoke.initiate(init);

                    // Trick to get the commit of transaction to contain TraceIds of all outgoing messages
                    // - which should handle if we get any Exceptions when committing.
                    String traceId = messagesToSend.stream()
                            .map(m -> m.getMatsTrace().getTraceId())
                            .distinct()
                            .collect(Collectors.joining(";"));
                    MDC.put(MDC_TRACE_ID, traceId);

                    sendMatsMessages(log, nanosStart, jmsSessionHolder, _parentFactory, messagesToSend);
                });
                // :: Handle the context.doAfterCommit(Runnable) lambda.
                try {
                    doAfterCommitRunnableHolder.runDoAfterCommitIfAny();
                }
                catch (RuntimeException re) {
                    log.error(LOG_PREFIX + "Got RuntimeException when running the doAfterCommit Runnable."
                            + " Ignoring.", re);
                }
            }
            catch (JmsMatsMessageSendException e) {
                // JmsMatsMessageSendException is a JmsMatsJmsException, and that indicates that there was a problem
                // with JMS - so we should "crash" the JmsSessionHolder to signal that the JMS Connection is probably
                // broken.
                jmsSessionHolder.crashed(e);
                // This is a special variant of JmsMatsJmsException which is the "VERY BAD!" scenario.
                // TODO: Do retries if it fails!
                throw new MatsMessageSendException("Evidently got problems sending out the JMS message after having"
                        + " run the process lambda and potentially committed other resources, typically database.", e);
            }
            catch (JmsMatsJmsException e) {
                // Catch any JmsMatsJmsException, as that indicates that there was a problem with JMS - so we should
                // "crash" the JmsSessionHolder to signal that the JMS Connection is probably broken.
                // Notice that we shall NOT have committed "external resources" at this point, meaning database.
                jmsSessionHolder.crashed(e);
                // .. then throw on. This is a lesser evil than JmsMatsMessageSendException, as it probably have
                // happened before we committed database etc.
                throw new MatsBackendException("Evidently have problems talking with our backend, which is a JMS"
                        + " Broker.", e);
            }
            finally {
                jmsSessionHolder.release();
                JmsMatsContextLocalCallback.unbindResource(MatsInitiate.class);
                _parentFactory.clearCurrentThreadLocalMatsDemarcation();
            }
        }
        finally {

            // :: Restore MDC
            MDC.remove(MDC_MATS_INITIATE);
            if (existingTraceId != null) {
                MDC.put(MDC_TRACE_ID, existingTraceId);
            }
            else {
                MDC.remove(MDC_TRACE_ID);
            }
        }
    }

    @Override
    public void initiateUnchecked(InitiateLambda lambda) throws MatsBackendRuntimeException,
            MatsMessageSendRuntimeException {
        try {
            initiate(lambda);
        }
        catch (MatsMessageSendException e) {
            throw new MatsMessageSendRuntimeException("Wrapping the MatsMessageSendException in a unchecked variant",
                    e);
        }
        catch (MatsBackendException e) {
            throw new MatsBackendRuntimeException("Wrapping the MatsBackendException in a unchecked variant", e);
        }
    }

    @Override
    public void close() {
        /*
         * Nothing to do in JMS Mats implementation, as we only "loan" JMS Sessions from the JmsMatsJmsSessionHandler,
         * which is the one that closes everything on shutdown.
         */
    }

    /**
     * Implementation of {@link JmsMatsTxContextKey}.
     */
    @Override
    public JmsMatsStage<?, ?, ?, ?> getStage() {
        // There is no stage, and contract is to return null.
        return null;
    }

    @Override
    public JmsMatsFactory<Z> getFactory() {
        return _parentFactory;
    }

    @Override
    public String toString() {
        return idThis();
    }

    static class MessageReferenceImpl implements MessageReference {
        private final String _matsMessageId;

        public MessageReferenceImpl(String matsMessageId) {
            _matsMessageId = matsMessageId;
        }

        @Override
        public String getMatsMessageId() {
            return _matsMessageId;
        }
    }

    /**
     * This wrapping MatsInitiator effectively works like <code>propagation=REQUIRED</code>, in that if there is an
     * ongoing stage or initiation, any initiation done with it is "hoisted" up to the ongoing process. Otherwise, it
     * acts as normal - which is to create a transaction.
     */
    static class MatsInitiator_TxRequired<Z> implements MatsInitiator {
        private final JmsMatsFactory<Z> _matsFactory;
        private final JmsMatsInitiator<Z> _matsInitiator;

        public MatsInitiator_TxRequired(JmsMatsFactory<Z> matsFactory, JmsMatsInitiator<Z> matsInitiator) {
            _matsFactory = matsFactory;
            _matsInitiator = matsInitiator;
        }

        @Override
        public String getName() {
            return _matsInitiator.getName();
        }

        @Override
        public MatsFactory getParentFactory() {
            return _matsFactory;
        }

        @Override
        public void initiate(InitiateLambda lambda) throws MatsMessageSendException, MatsBackendException {
            Optional<Supplier<MatsInitiate>> matsInitiateForNesting = _matsFactory
                    .getCurrentThreadLocalMatsDemarcation();
            // ?: Are we within a Mats demarcation?
            if (matsInitiateForNesting.isPresent()) {
                // -> Evidently within a Mats demarcation, so use the ThreadLocal MatsInitiate.
                lambda.initiate(matsInitiateForNesting.get().get());
            }
            else {
                // -> No, not within a Mats demarcation, so just forward the call directly to the MatsInitiator
                _matsInitiator.initiate(lambda);
            }
        }

        @Override
        public void initiateUnchecked(InitiateLambda lambda) throws MatsBackendRuntimeException,
                MatsMessageSendRuntimeException {
            Optional<Supplier<MatsInitiate>> matsInitiateForNesting = _matsFactory
                    .getCurrentThreadLocalMatsDemarcation();
            // ?: Are we within a Mats demarcation?
            if (matsInitiateForNesting.isPresent()) {
                // -> Evidently within a Mats demarcation, so use the ThreadLocal MatsInitiate.
                lambda.initiate(matsInitiateForNesting.get().get());
            }
            else {
                // -> No, not within a Mats demarcation, so just forward the call directly to the MatsInitiator
                _matsInitiator.initiateUnchecked(lambda);
            }
        }

        @Override
        public void close() {
            Optional<Supplier<MatsInitiate>> matsInitiateForNesting = _matsFactory
                    .getCurrentThreadLocalMatsDemarcation();
            // ?: Are we within a Mats demarcation?
            if (matsInitiateForNesting.isPresent()) {
                // -> Evidently within a Mats demarcation, so point this out pretty harshly.
                throw new IllegalStateException("This is the MatsFactory.getDefaultInitiator(), but it was gotten"
                        + " within a nested Mats demarcation, and is thus when within a Stage, a wrapper"
                        + " around the MatsInitiate from ProcessContext.initiate(..), or when within an initiation, "
                        + " a wrapper around the same MatsInitiate that you already have. It as such makes"
                        + " absolutely NO SENSE that you would want to close it: You've gotten the default "
                        + " MatsInitiator, *you are within a Mats processing context*, and then you invoke "
                        + " '.close()' on it?!");
            }
            else {
                // -> No, not within a Mats demarcation, so forward close call.
                _matsInitiator.close();
            }
        }

        @Override
        public String toString() {
            Optional<Supplier<MatsInitiate>> matsInitiateForNesting = _matsFactory
                    .getCurrentThreadLocalMatsDemarcation();
            return matsInitiateForNesting.isPresent()
                    ? "[nested-process wrapper of MatsFactory.getDefaultInitiator()]@" + Integer
                            .toHexString(System.identityHashCode(this))
                    : _matsInitiator.toString();
        }
    }

    /**
     * This wrapping MatsInitiator effectively works like <code>propagation=REQUIRES_NEW</code>, in that if there is an
     * ongoing stage or initiation, any initiation done with it is given a new transactional context (currently by brute
     * force: create a new Thread and run it there!). Otherwise, it acts as normal - which is to create a transaction.
     */
    static class MatsInitiator_TxRequiresNew<Z> implements MatsInitiator {
        private final JmsMatsFactory<Z> _matsFactory;
        private final JmsMatsInitiator<Z> _matsInitiator;

        public MatsInitiator_TxRequiresNew(JmsMatsFactory<Z> matsFactory, JmsMatsInitiator<Z> matsInitiator) {
            _matsFactory = matsFactory;
            _matsInitiator = matsInitiator;
        }

        @Override
        public String getName() {
            return _matsInitiator.getName();
        }

        @Override
        public MatsFactory getParentFactory() {
            return _matsFactory;
        }

        @Override
        public void initiate(InitiateLambda lambda) throws MatsMessageSendException, MatsBackendException {
            Optional<Supplier<MatsInitiate>> matsInitiateForNesting = _matsFactory
                    .getCurrentThreadLocalMatsDemarcation();
            // ?: Are we within a Mats demarcation?
            if (matsInitiateForNesting.isPresent()) {
                // -> Evidently within a Mats demarcation, so fire up a new thread to do the initiation.
                Throwable[] throwableResult = new Throwable[1];
                String threadName = Thread.currentThread().getName() + ":subInitiation_" + Integer.toString(
                        ThreadLocalRandom.current().nextInt(), 36);
                Map<String, String> copyOfContextMap = MDC.getCopyOfContextMap();
                Thread thread = new Thread(() -> {
                    MDC.setContextMap(copyOfContextMap);
                    try {
                        _matsInitiator.initiate(lambda);
                    }
                    catch (Throwable t) {
                        throwableResult[0] = t;
                    }
                }, threadName);
                thread.start();
                try {
                    thread.join();
                }
                catch (InterruptedException e) {
                    // Pass on the interruption.
                    thread.interrupt();
                    // Throw out.
                    throw new RuntimeException("Got interrupted.", e);
                }
                if (throwableResult[0] != null) {
                    Throwable t = throwableResult[0];
                    if (t instanceof RuntimeException) {
                        throw (RuntimeException) t;
                    }
                    if (t instanceof MatsBackendException) {
                        throw (MatsBackendException) t;
                    }
                    if (t instanceof MatsMessageSendException) {
                        throw (MatsMessageSendException) t;
                    }
                    if (t instanceof Error) {
                        throw (Error) t;
                    }
                    throw new RuntimeException("Got undeclared checked Exception [" + t.getClass().getSimpleName()
                            + "]!", t);
                }
            }
            else {
                // -> No, not within a Mats demarcation, so just forward the call directly to the MatsInitiator
                _matsInitiator.initiate(lambda);
            }
        }

        @Override
        public void initiateUnchecked(InitiateLambda lambda) throws MatsBackendRuntimeException,
                MatsMessageSendRuntimeException {
            Optional<Supplier<MatsInitiate>> matsInitiateForNesting = _matsFactory
                    .getCurrentThreadLocalMatsDemarcation();
            // ?: Are we within a Mats demarcation?
            if (matsInitiateForNesting.isPresent()) {
                // -> Evidently within a Mats demarcation, so fire up a new thread to do the initiation.
                Throwable[] throwableResult = new Throwable[1];
                String threadName = Thread.currentThread().getName() + ":subInitiation_" + Integer.toString(
                        ThreadLocalRandom.current().nextInt(), 36);
                Map<String, String> copyOfContextMap = MDC.getCopyOfContextMap();
                Thread thread = new Thread(() -> {
                    MDC.setContextMap(copyOfContextMap);
                    try {
                        _matsInitiator.initiateUnchecked(lambda);
                    }
                    catch (Throwable t) {
                        throwableResult[0] = t;
                    }
                }, threadName);
                thread.start();
                try {
                    thread.join();
                }
                catch (InterruptedException e) {
                    // Pass on the interruption.
                    thread.interrupt();
                    // Throw out.
                    throw new RuntimeException("Got interrupted.", e);
                }
                if (throwableResult[0] != null) {
                    Throwable t = throwableResult[0];
                    if (t instanceof RuntimeException) {
                        throw (RuntimeException) t;
                    }
                    if (t instanceof Error) {
                        throw (Error) t;
                    }
                    throw new RuntimeException("Got undeclared checked Exception [" + t.getClass().getSimpleName()
                            + "]!", t);
                }
            }
            else {
                // -> No, not within a Mats demarcation, so just forward the call directly to the MatsInitiator
                _matsInitiator.initiateUnchecked(lambda);
            }
        }

        @Override
        public void close() {
            Optional<Supplier<MatsInitiate>> matsInitiateForNesting = _matsFactory
                    .getCurrentThreadLocalMatsDemarcation();
            // ?: Are we within a Mats demarcation?
            if (matsInitiateForNesting.isPresent()) {
                // -> Evidently within a Mats demarcation, so point this out pretty harshly.
                throw new IllegalStateException("You are evidently within a Mats processing (stage or init),"
                        + " so it makes absolutely no sense to close the MatsInitiator now.");
            }
            else {
                // -> No, not within a Mats demarcation, so forward close call.
                _matsInitiator.close();
            }
        }

        @Override
        public String toString() {
            Optional<Supplier<MatsInitiate>> matsInitiateForNesting = _matsFactory
                    .getCurrentThreadLocalMatsDemarcation();
            return matsInitiateForNesting.isPresent()
                    ? "[nested-process wrapper of MatsFactory.getDefaultInitiator()]@" + Integer
                            .toHexString(System.identityHashCode(this))
                    : _matsInitiator.toString();
        }
    }
}
