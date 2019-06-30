package com.stolsvik.mats.impl.jms;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
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
import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.MatsEndpoint.ProcessLambda;
import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsStage;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler.JmsSessionHolder;
import com.stolsvik.mats.impl.jms.JmsMatsProcessContext.DoAfterCommitRunnableHolder;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.JmsMatsTxContextKey;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.TransactionContext;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsSerializer.DeserializedMatsTrace;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.serial.MatsTrace.Call;
import com.stolsvik.mats.util.RandomString;

/**
 * The JMS implementation of {@link MatsStage}.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class JmsMatsStage<R, S, I, Z> implements MatsStage<R, S, I>, JmsMatsStatics {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsStage.class);

    private final JmsMatsEndpoint<R, S, Z> _parentEndpoint;
    private final String _stageId;
    private final boolean _queue;
    private final Class<S> _stateClass;
    private final Class<I> _incomingMessageClass;
    private final ProcessLambda<R, S, I> _processLambda;

    private final JmsMatsFactory<Z> _parentFactory;

    private final JmsStageConfig _stageConfig = new JmsStageConfig();

    public JmsMatsStage(JmsMatsEndpoint<R, S, Z> parentEndpoint, String stageId, boolean queue,
            Class<I> incomingMessageClass, Class<S> stateClass, ProcessLambda<R, S, I> processLambda) {
        _parentEndpoint = parentEndpoint;
        _stageId = stageId;
        _queue = queue;
        _stateClass = stateClass;
        _incomingMessageClass = incomingMessageClass;
        _processLambda = processLambda;

        _parentFactory = _parentEndpoint.getParentFactory();

        log.info(LOG_PREFIX + "Created Stage [" + id(_stageId, this) + "].");
    }

    @Override
    public StageConfig<R, S, I> getStageConfig() {
        return _stageConfig;
    }

    private String _nextStageId;

    void setNextStageId(String nextStageId) {
        _nextStageId = nextStageId;
    }

    String getNextStageId() {
        return _nextStageId;
    }

    JmsMatsEndpoint<R, S, Z> getParentEndpoint() {
        return _parentEndpoint;
    }

    String getStageId() {
        return _stageId;
    }

    private final List<JmsMatsStageProcessor<R, S, I, Z>> _stageProcessors = new CopyOnWriteArrayList<>();
    private CountDownLatch _anyProcessorMadeConsumerLatch = new CountDownLatch(1);

    @Override
    public synchronized void start() {
        log.info(JmsMatsStatics.LOG_PREFIX + "     \\-  Starting Stage [" + id(_stageId, this) + "].");
        if (_stageProcessors.size() > 1) {
            log.info(JmsMatsStatics.LOG_PREFIX + "     \\-  ALREADY STARTED! [" + id(_stageId, this) + "].");
            return;
        }

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
            _stageProcessors.add(new JmsMatsStageProcessor<>(this, i));
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
        log.info(LOG_PREFIX + "Closing [" + _stageId + "]: Setting run-flag to false, and"
                + " stopping StageProcessors.");
        _stageProcessors.forEach(JmsMatsStageProcessor::setRunFlagFalse);
        _stageProcessors.forEach(sp -> sp.stop(5000));
        _stageProcessors.clear();
    }

    @Override
    public String idThis() {
        return id(_stageId, this) + "@" + _parentFactory;
    }

    @Override
    public String toString() {
        return idThis();
    }

    private class JmsStageConfig implements StageConfig<R, S, I> {
        private int _concurrency;

        @Override
        public MatsConfig setConcurrency(int concurrency) {
            _concurrency = concurrency;
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
    static class JmsMatsStageProcessor<R, S, I, Z> implements JmsMatsStatics, JmsMatsTxContextKey {
        private final String _randomInstanceId;
        private final JmsMatsStage<R, S, I, Z> _jmsMatsStage;
        private final int _processorNumber;
        private final Thread _processorThread;
        private final TransactionContext _transactionContext;

        JmsMatsStageProcessor(JmsMatsStage<R, S, I, Z> jmsMatsStage, int processorNumber) {
            _randomInstanceId = RandomString.randomString(5) + "@" + jmsMatsStage._parentFactory;
            _jmsMatsStage = jmsMatsStage;
            _processorNumber = processorNumber;
            _processorThread = new Thread(this::runner, THREAD_PREFIX + ident());
            _processorThread.start();
            _transactionContext = jmsMatsStage._parentFactory
                    .getJmsMatsTransactionManager().getTransactionContext(this);
        }

        private volatile boolean _runFlag = true; // Start off running.

        private JmsSessionHolder _jmsSessionHolder;

        private String ident() {
            return _jmsMatsStage._stageId + '#' + _processorNumber + " {" + _randomInstanceId + '}';
        }

        @Override
        public JmsMatsStage<?, ?, ?, ?> getStage() {
            return _jmsMatsStage;
        }

        @Override
        public JmsMatsFactory<Z> getFactory() {
            return _jmsMatsStage.getParentEndpoint().getParentFactory();
        }

        /**
         * Upon Stage stop: First invoke this on all StageProcessors to block any new message receptions.
         */
        void setRunFlagFalse() {
            _runFlag = false;
        }

        private volatile boolean _processorInReceive;

        void stop(int gracefulWaitMillis) {
            // Start by setting the run-flag to false..
            _runFlag = false;
            // Fetch the JmsSessionHolder to close, so that if we're in a race, we won't change references while
            // doing stop-stuff.
            JmsSessionHolder jmsSessionHolderToClose;
            synchronized (this) {
                jmsSessionHolderToClose = _jmsSessionHolder;
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
                // 1. JavaDoc isAlive(): "A thread is alive if it has been started and has not yet died."
                // 2. The Thread is started in the constructor.
                // 3. Thus, if it is not alive, there is NO possibility that it is starting, or about to be started.
                log.info(LOG_PREFIX + ident() + " has already exited, so just close JMS Session.");
                closeJmsSessionHolder(jmsSessionHolderToClose);
                // Finished!
                return;
            }
            // E-> No, thread has not exited.

            boolean sessionClosed = false;
            // ?: Is thread currently waiting in consumer.receive()?
            if (_processorInReceive) {
                // -> Yes, waiting in receive(), so close session, thus making receive() return null.
                log.info(LOG_PREFIX + ident() + " is waiting in consumer.receive(), so we'll close the JMS Session,"
                        + " thereby making the receive() call return null, and the thread will exit.");
                closeJmsSessionHolder(jmsSessionHolderToClose);
                sessionClosed = true;
            }
            else {
                // -> No, not in receive()
                log.info(LOG_PREFIX + ident() + " is NOT waiting in consumer.receive(), so we assume it is out"
                        + " doing work, and will come back and see the run-flag being false, thus exit.");
            }

            log.info(LOG_PREFIX + "Waiting for " + ident() + " to exit.");
            joinProcessorThread(gracefulWaitMillis);
            // ?: Did the thread exit?
            if (!_processorThread.isAlive()) {
                // -> Yes, thread exited.
                log.info(LOG_PREFIX + ident() + " exited nicely.");
            }
            else {
                // -> No, thread did not exit within graceful wait period.
                log.warn(LOG_PREFIX + ident() + " DID NOT exit after " + gracefulWaitMillis
                        + "ms, so interrupt it and wait some more.");
                // -> No, so interrupt it from whatever it is doing.
                _processorThread.interrupt();
                // Wait another graceful wait period
                joinProcessorThread(gracefulWaitMillis);
                // ?: Did the thread exit now? (Log only)
                if (!_processorThread.isAlive()) {
                    // -> Yes, thread exited.
                    log.info(LOG_PREFIX + ident() + " exited after being interrupted.");
                }
                else {
                    log.warn(LOG_PREFIX + ident() + " DID NOT exit even after being interrupted."
                            + " Giving up, closing JMS Session.");
                }
                // ----- At this point, if the thread has not exited, we'll just close the JMS Session and pray.
            }

            // ?: Have we already closed the session?
            if (!sessionClosed) {
                // -> No, so do it now.
                closeJmsSessionHolder(jmsSessionHolderToClose);
            }
        }

        private void closeJmsSessionHolder(JmsSessionHolder jmsSessionHolderToClose) {
            if (jmsSessionHolderToClose == null) {
                log.info(LOG_PREFIX + "There was no JMS Session in place...");
            }
            else {
                jmsSessionHolderToClose.close();
            }
        }

        private void joinProcessorThread(int gracefulWaitMillis) {
            try {
                _processorThread.join(gracefulWaitMillis);
            }
            catch (InterruptedException e) {
                log.warn(LOG_PREFIX + "Got InterruptedException when waiting for " + ident() + " to join."
                        + " Dropping out.");
            }
        }

        private void runner() {
            // :: OUTER RUN-LOOP, where we'll get a fresh JMS Session, Destination and MessageConsumer.
            while (_runFlag) {
                log.info(LOG_PREFIX + "Getting JMS Session, Destination and Consumer for stage ["
                        + _jmsMatsStage._stageId + "].");
                { // Local-scope the 'newJmsSessionHolder' variable.
                    JmsSessionHolder newJmsSessionHolder;
                    try {
                        newJmsSessionHolder = _jmsMatsStage._parentFactory
                                .getJmsMatsJmsSessionHandler().getSessionHolder(this);
                    }
                    catch (JmsMatsJmsException | RuntimeException t) {
                        log.warn(LOG_PREFIX + "Got " + t.getClass().getSimpleName() + " while trying to get new"
                                + " JmsSessionHolder. Chilling a bit, then looping to check run-flag.", t);
                        /*
                         * Doing a "chill-wait", so that if we're in a situation where this will tight-loop, we won't
                         * totally swamp both CPU and logs with meaninglessness.
                         */
                        chillWait();
                        continue;
                    }
                    // :: "Publish" the new JMS Session.
                    synchronized (this) {
                        // ?: Check the run-flag one more time!
                        if (!_runFlag) {
                            // -> No, we're asked to exit.
                            // NOTICE! Since this JMS Session has not been "published" outside yet, we'll have to
                            // close it directly.
                            newJmsSessionHolder.close();
                            // Break out of run-loop.
                            break;
                        }
                        else {
                            // -> Yes, we're good! "Publish" the new JMS Session.
                            _jmsSessionHolder = newJmsSessionHolder;
                        }
                    }
                }
                try {
                    Session jmsSession = _jmsSessionHolder.getSession();
                    Destination destination = createJmsDestination(jmsSession, getFactory().getFactoryConfig());
                    MessageConsumer jmsConsumer = jmsSession.createConsumer(destination);

                    // We've established the consumer, and hence will start to receive messages and process them.
                    // (Important for topics, where if we haven't established consumer, we won't get messages).
                    // TODO: Handle ability to stop with subsequent re-start of endpoint.
                    _jmsMatsStage._anyProcessorMadeConsumerLatch.countDown();

                    // :: INNER RECEIVE-LOOP, where we'll use the JMS Session and MessageConsumer.receive().
                    while (_runFlag) {
                        _processorInReceive = true;
                        // Check whether Session/Connection is ok (per contract with JmsSessionHolder)
                        _jmsSessionHolder.isSessionOk();
                        // :: GET NEW MESSAGE!! THIS IS THE MESSAGE PUMP!
                        Message message;
                        try {
                            if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "Going into JMS consumer.receive() for ["
                                    + destination + "].");
                            message = jmsConsumer.receive();
                        }
                        finally {
                            _processorInReceive = false;
                        }
                        if (message == null) {
                            log.info(LOG_PREFIX + "!! Got null from JMS consumer.receive(), JMS Session"
                                    + " was probably closed due to shutdown or JMS error. Looping to check run-flag.");
                            continue;
                        }
                        // Check whether Session/Connection is still ok (per contract with JmsSessionHolder)
                        _jmsSessionHolder.isSessionOk();

                        // :: Perform the work inside the TransactionContext
                        DoAfterCommitRunnableHolder doAfterCommitRunnableHolder = new DoAfterCommitRunnableHolder();
                        long nanosStart = System.nanoTime();
                        try {
                            // :: Going into Mats Transaction

                            _transactionContext.doTransaction(_jmsSessionHolder, () -> {
                                // Assert that this is indeed a JMS MapMessage.
                                if (!(message instanceof MapMessage)) {
                                    String msg = "Got some JMS Message that is not instanceof JMS MapMessage"
                                            + " - cannot be a MATS message! Refusing this message!";
                                    log.error(LOG_PREFIX + msg + "\n" + message);
                                    throw new MatsRefuseMessageException(msg);
                                }

                                // ----- This is a MapMessage
                                MapMessage mapMessage = (MapMessage) message;

                                byte[] matsTraceBytes;
                                String matsTraceMeta;
                                String messageId;
                                try {
                                    String matsTraceKey = getFactory().getFactoryConfig().getMatsTraceKey();
                                    matsTraceBytes = mapMessage.getBytes(matsTraceKey);
                                    matsTraceMeta = mapMessage.getString(matsTraceKey
                                            + MatsSerializer.META_KEY_POSTFIX);
                                    messageId = mapMessage.getJMSMessageID();

                                    // :: Assert that we got some values
                                    if (matsTraceBytes == null) {
                                        String msg = "Got some JMS Message that is missing MatsTrace byte array on"
                                                + "JMS MapMessage key '" + matsTraceKey +
                                                "' - cannot be a MATS message! Refusing this message!";
                                        log.error(LOG_PREFIX + msg + "\n" + message);
                                        throw new MatsRefuseMessageException(msg);
                                    }

                                    if (matsTraceMeta == null) {
                                        String msg = "Got some JMS Message that is missing MatsTraceMeta String on"
                                                + "JMS MapMessage key '" + MatsSerializer.META_KEY_POSTFIX
                                                + "' - cannot be a MATS message! Refusing this message!";
                                        log.error(LOG_PREFIX + msg + "\n" + message);
                                        throw new MatsRefuseMessageException(msg);
                                    }
                                }
                                catch (JMSException e) {
                                    // This might be temporary, so just throw to rollback this and try again.
                                    throw new JmsMatsJmsException("Got JMSException when getting the MatsTrace"
                                            + " from the MapMessage by using mapMessage.get[Bytes|String](..)."
                                            + " Pretty crazy.", e);
                                }

                                MatsSerializer<Z> matsSerializer = getFactory().getMatsSerializer();
                                DeserializedMatsTrace<Z> matsTraceDeserialized = matsSerializer
                                        .deserializeMatsTrace(matsTraceBytes, matsTraceMeta);
                                MatsTrace<Z> matsTrace = matsTraceDeserialized.getMatsTrace();

                                // :: Current Call
                                Call<Z> currentCall = matsTrace.getCurrentCall();
                                // Assert that this is indeed a JMS Message meant for this Stage
                                if (!_jmsMatsStage._stageId.equals(currentCall.getTo().getId())) {
                                    String msg = "The incoming MATS message is not to this Stage! this:["
                                            + _jmsMatsStage._stageId
                                            + "]," + " msg:[" + currentCall.getTo() + "]. Refusing this message!";
                                    log.error(LOG_PREFIX + msg + "\n" + mapMessage);
                                    throw new MatsRefuseMessageException(msg);
                                }

                                // :: Current State: If null, make an empty object instead, unless Void, which is null.
                                Z currentSerializedState = matsTrace.getCurrentState();
                                S currentSto = (currentSerializedState == null
                                        ? (_jmsMatsStage._stateClass != Void.class
                                                ? matsSerializer.newInstance(
                                                        _jmsMatsStage._stateClass)
                                                : null)
                                        : matsSerializer.deserializeObject(currentSerializedState,
                                                _jmsMatsStage._stateClass));

                                // :: Incoming Message DTO
                                I incomingDto = matsSerializer.deserializeObject(currentCall
                                        .getData(), _jmsMatsStage._incomingMessageClass);

                                double nanosTaken = (System.nanoTime() - nanosStart) / 1_000_000d;

                                log.info(LOG_PREFIX + "RECEIVED message from [" + currentCall.getFrom()
                                        + "], recv:[" + matsTraceBytes.length
                                        + " B]->decomp:[" + matsTraceMeta
                                        + " " + matsTraceDeserialized.getMillisDecompression()
                                        + " ms]->deserialize:[" + matsTraceDeserialized.getSizeDecompressed() + " B, "
                                        + matsTraceDeserialized.getMillisDeserialization()
                                        + " ms]->MT - tot w/DTO&STO:[" + nanosTaken + " ms].");

                                // :: Getting the 'sideloads'; Byte-arrays and Strings from the MapMessage.
                                LinkedHashMap<String, byte[]> incomingBinaries = new LinkedHashMap<>();
                                LinkedHashMap<String, String> incomingStrings = new LinkedHashMap<>();
                                try {
                                    @SuppressWarnings("unchecked")
                                    Enumeration<String> mapNames = (Enumeration<String>) mapMessage.getMapNames();
                                    while (mapNames.hasMoreElements()) {
                                        String name = mapNames.nextElement();
                                        Object object = mapMessage.getObject(name);
                                        if (object instanceof byte[]) {
                                            incomingBinaries.put(name, (byte[]) object);
                                        }
                                        else if (object instanceof String) {
                                            incomingStrings.put(name, (String) object);
                                        }
                                        else {
                                            log.warn("Got some object in the MapMessage to [" + _jmsMatsStage._stageId
                                                    + "] which is neither byte[] nor String - which should not happen"
                                                    + " - Ignoring.");
                                        }
                                    }
                                }
                                catch (JMSException e) {
                                    throw new JmsMatsJmsException("Got JMSException when getting 'sideloads'"
                                            + " from the MapMessage by using mapMessage.get[Bytes|String](..)."
                                            + " Pretty crazy.", e);
                                }

                                // :: Invoke the process lambda (the actual user code).
                                List<JmsMatsMessage<Z>> messagesToSend = new ArrayList<>();

                                _jmsMatsStage._processLambda.process(new JmsMatsProcessContext<>(
                                        getFactory(),
                                        _jmsMatsStage.getParentEndpoint().getEndpointId(),
                                        _jmsMatsStage.getStageId(),
                                        messageId,
                                        _jmsMatsStage.getNextStageId(),
                                        matsTraceBytes, 0, matsTraceBytes.length, matsTraceMeta,
                                        matsTrace,
                                        currentSto,
                                        incomingBinaries, incomingStrings,
                                        messagesToSend, doAfterCommitRunnableHolder),
                                        currentSto, incomingDto);

                                sendMatsMessages(log, nanosStart, _jmsSessionHolder, getFactory(), messagesToSend);
                            }); // End: Mats Transaction
                        }
                        catch (RuntimeException e) {
                            log.info(LOG_PREFIX + "Got [" + e.getClass().getName()
                                    + "] inside transactional message processing, which shall have been handled by the"
                                    + " MATS TransactionManager (rollback). Looping to fetch next message.");
                            // No more to do, so loop. Notice that this code is not involved in initiations..
                            continue;
                        }

                        // :: Handle the DoAfterCommit lambda.
                        try {
                            doAfterCommitRunnableHolder.runDoAfterCommitIfAny();
                        }
                        catch (RuntimeException e) {
                            // Message processing is per definition finished here, so no way to DLQ or otherwise
                            // notify world except logging an error.
                            log.error(LOG_PREFIX + "Got [" + e.getClass().getSimpleName()
                                    + "] when running the doAfterCommit Runnable. Ignoring.", e);
                        }

                        double millisTotal = (System.nanoTime() - nanosStart) / 1_000_000d;
                        log.info(LOG_PREFIX + "Total time from received till finished processing: ["
                                + millisTotal + "].");

                    } // End: INNER RECEIVE-LOOP
                }
                catch (JmsMatsJmsException | JMSException | RuntimeException | Error e) {
                    log.warn(LOG_PREFIX + "Got [" + e.getClass().getSimpleName() + "] inside the message processing"
                            + " loop, crashing JmsSessionHolder, chilling a bit, then looping.", e);
                    _jmsSessionHolder.crashed(e);
                    // Quick-check the run flag before chilling, if the reason for Exception is closed Connection or
                    // Session due to shutdown. (ActiveMQ do not let you create Session if Connection is closed,
                    // and do not let you create a Consumer if Session is closed.)
                    if (!_runFlag) {
                        log.info("The run-flag was false, so we shortcut to exit.");
                    }
                    /*
                     * Doing a "chill-wait", so that if we're in a situation where this will tight-loop, we won't
                     * totally swamp both CPU and logs with meaninglessness.
                     */
                    chillWait();
                }
            } // END: OUTER RUN-LOOP
            log.info(LOG_PREFIX + ident() + " asked to exit, and that we do! Bye.");
        }

        private void chillWait() {
            try {
                // About 5 seconds..
                Thread.sleep(4500 + Math.round(Math.random() * 1000));
            }
            catch (InterruptedException e) {
                log.info(LOG_PREFIX + "Got InterruptedException when chill-waiting.");
            }
        }

        private Destination createJmsDestination(Session jmsSession, FactoryConfig factoryConfig) throws JMSException {
            Destination destination;
            String destinationName = factoryConfig.getMatsDestinationPrefix() + _jmsMatsStage._stageId;
            if (_jmsMatsStage._queue) {
                destination = jmsSession.createQueue(destinationName);
            }
            else {
                destination = jmsSession.createTopic(destinationName);
            }
            log.info(LOG_PREFIX + "Created JMS " + (_jmsMatsStage._queue ? "Queue" : "Topic") + ""
                    + " to receive from: [" + destination + "].");
            return destination;
        }

        @Override
        public String toString() {
            return idThis();
        }
    }
}
