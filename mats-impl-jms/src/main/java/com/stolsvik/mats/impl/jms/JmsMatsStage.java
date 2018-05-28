package com.stolsvik.mats.impl.jms;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsConfig;
import com.stolsvik.mats.MatsEndpoint.ProcessLambda;
import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsStage;
import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.exceptions.MatsRefuseMessageException;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.TransactionContext;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsSerializer.DeserializedMatsTrace;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.serial.MatsTrace.Call;

/**
 * The JMS implementation of {@link MatsStage}.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class JmsMatsStage<I, S, R, Z> implements MatsStage<I, S, R>, JmsMatsStatics {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsStage.class);

    private final JmsMatsEndpoint<S, R, Z> _parentEndpoint;
    private final String _stageId;
    private final boolean _queue;
    private final Class<S> _stateClass;
    private final Class<I> _incomingMessageClass;
    private final ProcessLambda<I, S, R> _processLambda;

    private final JmsMatsFactory<Z> _parentFactory;
    private final MatsSerializer<Z> _matsJsonSerializer;

    private final JmsStageConfig _stageConfig = new JmsStageConfig();

    public JmsMatsStage(JmsMatsEndpoint<S, R, Z> parentEndpoint, String stageId, boolean queue,
            Class<I> incomingMessageClass, Class<S> stateClass, ProcessLambda<I, S, R> processLambda) {
        _parentEndpoint = parentEndpoint;
        _stageId = stageId;
        _queue = queue;
        _stateClass = stateClass;
        _incomingMessageClass = incomingMessageClass;
        _processLambda = processLambda;

        _parentFactory = _parentEndpoint.getParentFactory();
        _matsJsonSerializer = _parentFactory.getMatsSerializer();

        log.info(LOG_PREFIX + "Created Stage [" + id(_stageId, this) + "].");
    }

    @Override
    public StageConfig<I, S, R> getStageConfig() {
        return _stageConfig;
    }

    private String _nextStageId;

    void setNextStageId(String nextStageId) {
        _nextStageId = nextStageId;
    }

    String getNextStageId() {
        return _nextStageId;
    }

    JmsMatsEndpoint<S, R, Z> getParentEndpoint() {
        return _parentEndpoint;
    }

    String getStageId() {
        return _stageId;
    }

    private volatile TransactionContext _transactionContext;

    private final List<StageProcessor> _stageProcessors = new CopyOnWriteArrayList<>();

    private CountDownLatch _anyProcessorMadeConsumerLatch = new CountDownLatch(1);

    @Override
    public synchronized void start() {
        log.info(JmsMatsStatics.LOG_PREFIX + "     \\-  Starting Stage [" + id(_stageId, this) + "].");
        if (_stageProcessors.size() > 1) {
            log.info(JmsMatsStatics.LOG_PREFIX + "     \\-  ALREADY STARTED! [" + id(_stageId, this) + "].");
            return;
        }

        _transactionContext = _parentFactory.getJmsMatsTransactionManager().getTransactionContext(this);

        // :: Fire up the actual stage processors, using the configured (or default) concurrency
        int numberOfProcessors = getStageConfig().getConcurrency();
        // ?: Is this a topic?
        if (!_queue) {
            /*
             * -> Yes, and in that case, there shall only be one StageProcessor for the endpoint. If the user chooses to
             * make more endpoints picking from the same topic, then so be it, but it generally makes no sense, as the
             * whole point of a MQ Topic is that all listeners to the topic will get the same messages.
             *
             * (Optimizations along the line of using a thread pool for the actual work of the processor must be done in
             * user code, as the MATS framework must acknowledge (commit/rollback) each message, and cannot decide what
             * code could potentially be done concurrently..)
             */
            numberOfProcessors = 1;
        }

        for (int i = 0; i < numberOfProcessors; i++) {
            _stageProcessors.add(new StageProcessor(i));
        }
    }

    @Override
    public void waitForStarted() {
        try {
            _anyProcessorMadeConsumerLatch.await();
        }
        catch (InterruptedException e) {
            throw new IllegalStateException("Got interrupted while waitForStarted().", e);
        }
    }

    @Override
    public synchronized void stop() {
        log.info(LOG_PREFIX + "Closing [" + _stageId + "], setting running to false,"
                + " stopping StageProcessors, closing TransactionContext.");
        _stageProcessors.forEach(StageProcessor::setRunFlagFalse);
        _stageProcessors.forEach(sp -> sp.stop(5000));
        _stageProcessors.clear();

        _transactionContext.close();
    }

    private class JmsStageConfig implements StageConfig<I, S, R> {
        private int _concurrency;

        @Override
        public MatsConfig setConcurrency(int numberOfThreads) {
            _concurrency = numberOfThreads;
            return this;
        }

        @Override
        public boolean isConcurrencyDefault() {
            return _concurrency == 0;
        }

        @Override
        public int getConcurrency() {
            if (_concurrency == 0) {
                return _parentEndpoint.getEndpointConfig().getConcurrency();
            }
            return _concurrency;
        }

        @Override
        public boolean isRunning() {
            return _stageProcessors.size() > 0;
        }

        @Override
        public int getRunningStageProcessors() {
            return _stageProcessors.size();
        }

        @Override
        public Class<I> getIncomingMessageClass() {
            return _incomingMessageClass;
        }
    }

    /**
     * MessageConsumer-class for the {@link JmsMatsStage} which is instantiated {@link StageConfig#getConcurrency()}
     * number of times, carrying the run-thread.
     * <p>
     * Package access so that it can be referred to from JavaDoc.
     */
    class StageProcessor implements JmsMatsStatics {

        private final int _processorNumber;

        private volatile boolean _processorRun = true; // Start off running.

        private volatile Thread _processorThread;

        private Session _processorJmsSession;

        private volatile boolean _processorInReceive;

        StageProcessor(int processorNumber) {
            _processorNumber = processorNumber;
            _processorThread = new Thread(this::runner, THREAD_PREFIX + _stageId + " " + id());
            _processorThread.start();
        }

        private String id() {
            return id("StageProcessor#" + _processorNumber, this);
        }

        /**
         * First invoke this on all StageProcessors to block any new message receptions.
         */
        void setRunFlagFalse() {
            _processorRun = false;
        }

        void stop(int gracefulWaitMillis) {
            // Start by setting the run-flag to false..
            _processorRun = false;
            Session jmsSessionToClose;
            synchronized (this) {
                jmsSessionToClose = _processorJmsSession;
            }
            /*
             * Trying to make very graceful: If we're in consumer.receive(), then close the Session, which makes the
             * receive()-call return null, causing the thread to loop and check run-flag. If not, then assume that the
             * thread is out doing work, and it will see that the run-flag is false upon next loop.
             *
             * We won't put too much effort in making this race-proof, as if it fails, which should be seldom, the only
             * problems are a couple of ugly stack traces in the log: Transactionality will keep integrity.
             */

            // ?: Has processorThread already exited?
            if (!_processorThread.isAlive()) {
                // -> Yes, thread already exited, so just close session (this is our responsibility).
                log.info(LOG_PREFIX + id() + "-thread has already exited, so just close JMS Session.");
                closeJmsSession(jmsSessionToClose);
                // Finished!
                return;
            }
            // E-> No, thread has not exited.

            boolean sessionClosed = false;
            // ?: Is thread currently waiting in consumer.receive()?
            if (_processorInReceive) {
                // -> Yes, waiting in receive(), so close session, thus making receive() return null.
                log.info(LOG_PREFIX + id() + "-thread is waiting in consumer.receive(), so we'll close the JMS Session,"
                        + " thereby making the receive() call return null, and the thread will exit.");
                closeJmsSession(jmsSessionToClose);
                sessionClosed = true;
            }
            else {
                // -> No, not in receive()
                log.info(LOG_PREFIX + id() + "-thread is NOT waiting in consumer.receive(), so we assume it is out"
                        + " doing work, and will come back and see the run-flag being false, thus exit.");
            }

            log.info(LOG_PREFIX + "Waiting for " + id() + "-thread to exit.");
            joinProcessorThread(gracefulWaitMillis);
            // ?: Did the thread exit?
            if (!_processorThread.isAlive()) {
                // -> Yes, thread exited.
                log.info(LOG_PREFIX + id() + "-thread exited nicely.");
            }
            else {
                // -> No, thread did not exit within graceful wait period.
                log.warn(LOG_PREFIX + id() + "-thread DID NOT exit after " + gracefulWaitMillis
                        + "ms, so interrupt it and wait some more.");
                // -> No, so interrupt it from whatever it is doing.
                _processorThread.interrupt();
                // Wait another graceful wait period
                joinProcessorThread(gracefulWaitMillis);
                // ?: Did the thread exit now? (Log only)
                if (!_processorThread.isAlive()) {
                    // -> Yes, thread exited.
                    log.info(LOG_PREFIX + id() + "-thread exited after being interrupted.");
                }
                else {
                    log.warn(LOG_PREFIX + id()
                            + "-thread DID NOT exit even after being interrupted. Giving up, closing JMS Session.");
                }
                // ----- At this point, if the thread has not exited, we'll just close the JMS Session and pray.
            }

            // ?: Have we already closed the session?
            if (!sessionClosed) {
                // -> No, so do it now.
                closeJmsSession(jmsSessionToClose);
            }
        }

        private void joinProcessorThread(int gracefulWaitMillis) {
            try {
                _processorThread.join(gracefulWaitMillis);
            }
            catch (InterruptedException e) {
                log.warn(LOG_PREFIX + "Got InterruptedException when waiting for " + id() + "-thread to join."
                        + " Dropping out.");
            }
        }

        private void runner() {
            // :: Outer run-loop, where we'll get a fresh JMS Session, Destination and MessageConsumer.
            while (_processorRun) {
                try {
                    log.info(LOG_PREFIX + "Getting JMS Session, Destination and Consumer for stage [" + _stageId
                            + "].");
                    { // Local-scope the 'newSession' variable.
                        Session newSession = _transactionContext.getTransactionalJmsSession(true);
                        // :: "Publish" the new JMS Session.
                        synchronized (this) {
                            // ?: Check the run-flag one more time!
                            if (_processorRun) {
                                // -> Yes, we're good! "Publish" the new JMS Session.
                                _processorJmsSession = newSession;
                            }
                            else {
                                // -> No, we're asked to exit.
                                // Since this JMS Session has not been "published" outside yet, we'll close directly.
                                closeJmsSession(newSession);
                                // Break out of run-loop.
                                break;
                            }
                        }
                    }
                    FactoryConfig factoryConfig = _parentEndpoint.getParentFactory().getFactoryConfig();
                    Destination destination = createJmsDestination(_processorJmsSession, factoryConfig);
                    MessageConsumer jmsConsumer = _processorJmsSession.createConsumer(destination);

                    // We've established the consumer, and hence will start to receive messages and process them.
                    // (Important for topics, where if we haven't established consumer, we won't get messages).
                    _anyProcessorMadeConsumerLatch.countDown();

                    // :: Inner run-loop, where we'll use the JMS Session and MessageConsumer.
                    while (_processorRun) {
                        _processorInReceive = true;
                        log.info(LOG_PREFIX + "Going into JMS consumer.receive() for [" + destination + "].");
                        Message message = jmsConsumer.receive();
                        _processorInReceive = false;
                        if (message == null) {
                            log.info(LOG_PREFIX + "!! Got null from JMS consumer.receive(), JMS Session"
                                    + " was probably closed due to shutdown. Looping to check run-flag.");
                            continue;
                        }

                        // :: Perform the work inside the TransactionContext
                        try {
                            _transactionContext.performWithinTransaction(_processorJmsSession, () -> {
                                long nanosStart = System.nanoTime();
                                // Assert that this is indeed a JMS MapMessage.
                                if (!(message instanceof MapMessage)) {
                                    String msg = "Got some JMS Message that is not instanceof MapMessage"
                                            + " - cannot be a MATS message! Refusing this message!";
                                    log.error(LOG_PREFIX + msg + "\n" + message);
                                    throw new MatsRefuseMessageException(msg);
                                }

                                MapMessage mapMessage = (MapMessage) message;

                                byte[] matsTraceBytes;
                                String matsTraceMeta;
                                try {
                                    matsTraceBytes = mapMessage.getBytes(factoryConfig.getMatsTraceKey());
                                    matsTraceMeta = mapMessage.getString(factoryConfig.getMatsTraceKey()
                                            + MatsSerializer.META_KEY_POSTFIX);
                                }
                                catch (JMSException e) {
                                    throw new MatsBackendException(
                                            "Got JMSException when doing mapMessage.getString(..). Pretty crazy.", e);
                                }

                                DeserializedMatsTrace<Z> matsTraceDeserialized = _matsJsonSerializer
                                        .deserializeMatsTrace(matsTraceBytes, matsTraceMeta);

                                MatsTrace<Z> matsTrace = matsTraceDeserialized.getMatsTrace();

                                // :: Current Call
                                Call<Z> currentCall = matsTrace.getCurrentCall();
                                // Assert that this is indeed a JMS Message meant for this Stage
                                if (!_stageId.equals(currentCall.getTo().getId())) {
                                    String msg = "The incoming MATS message is not to this Stage! this:[" + _stageId
                                            + "]," + " msg:[" + currentCall.getTo() + "]. Refusing this message!";
                                    log.error(LOG_PREFIX + msg + "\n" + mapMessage);
                                    throw new MatsRefuseMessageException(msg);
                                }

                                // :: Current State. If null, then make an empty object instead.
                                Z currentSerializedState = matsTrace.getCurrentState();
                                S currentSto = (currentSerializedState == null
                                        ? (_stateClass != Void.class
                                                ? _matsJsonSerializer.newInstance(_stateClass)
                                                : null)
                                        : _matsJsonSerializer.deserializeObject(currentSerializedState, _stateClass));

                                // :: Incoming DTO
                                I incomingDto = _matsJsonSerializer.deserializeObject(currentCall.getData(),
                                        _incomingMessageClass);

                                double nanosTaken = (System.nanoTime() - nanosStart) / 1_000_000d;

                                log.info(LOG_PREFIX + "RECEIVED message from [" + currentCall.getFrom()
                                        + "], recv:[" + matsTraceBytes.length
                                        + " B]->decomp:[" + matsTraceMeta
                                        + " " + matsTraceDeserialized.getMillisDecompression()
                                        + " ms]->deserialize:[" + matsTraceDeserialized.getSizeDecompressed() + " B, "
                                        + matsTraceDeserialized.getMillisDeserialization()
                                        + " ms]->MT - tot w/DTO&STO:[" + nanosTaken + " ms].");

                                // :: Invoke the process lambda (stage processor).
                                _processLambda.process(new JmsMatsProcessContext<>(JmsMatsStage.this, _processorJmsSession,
                                        mapMessage, matsTrace, currentSto), incomingDto, currentSto);
                            });
                        }
                        catch (RuntimeException e) {
                            log.info(LOG_PREFIX + "Got [" + e.getClass().getName()
                                    + "] when processing " + stageOrInit(JmsMatsStage.this)
                                    + ", which shall have been handled by the MATS TransactionManager (rollback)."
                                    + " Looping to fetch next message.");
                        }
                    }
                }
                catch (Throwable t) {
                    log.error(LOG_PREFIX + "Got " + t.getClass().getSimpleName() + ", closing JMS Session,"
                            + " looping to check run-flag.", t);
                    closeJmsSession(_processorJmsSession);
                    /*
                     * Doing a "chill-wait", so that if we're in a situation where this will tight-loop, we won't
                     * totally swamp both CPU and logs with meaninglessness.
                     */
                    try {
                        Thread.sleep(2500);
                    }
                    catch (InterruptedException e) {
                        log.info(LOG_PREFIX + "Got InterruptedException when chill-waiting."
                                + " Looping to check run-flag.");
                    }
                }
            }
            log.info(LOG_PREFIX + id() + " asked to exit, and that we do! Bye.");
        }

        private Destination createJmsDestination(Session jmsSession, FactoryConfig factoryConfig) throws JMSException {
            Destination destination;
            String destinationName = factoryConfig.getMatsDestinationPrefix() + _stageId;
            if (_queue) {
                destination = jmsSession.createQueue(destinationName);
            }
            else {
                destination = jmsSession.createTopic(destinationName);
            }
            log.info(LOG_PREFIX + "Created JMS " + (_queue ? "Queue" : "Topic") + ""
                    + " to receive from: [" + destination + "].");
            return destination;
        }
    }

    private static void closeJmsSession(Session sessionToClose) {
        if (sessionToClose != null) {
            try {
                sessionToClose.close();
            }
            catch (JMSException e) {
                log.warn(LOG_PREFIX + "Got JMSException when trying to close JMS Session.", e);
            }
        }
    }
}
