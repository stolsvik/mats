package com.stolsvik.mats.impl.jms;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsStage.StageConfig;
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
 * MessageConsumer-class for the {@link JmsMatsStage} which is instantiated {@link StageConfig#getConcurrency()} number
 * of times, carrying the run-thread.
 * <p>
 * Package access so that it can be referred to from JavaDoc.
 *
 * @author Endre Stølsvik 2019-08-24 00:11 - http://stolsvik.com/, endre@stolsvik.com
 */
class JmsMatsStageProcessor<R, S, I, Z> implements JmsMatsStatics, JmsMatsTxContextKey, JmsMatsStartStoppable {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsStageProcessor.class);

    private final String _randomInstanceId;
    private final JmsMatsStage<R, S, I, Z> _jmsMatsStage;
    private final int _processorNumber;
    private final Thread _processorThread;
    private final TransactionContext _transactionContext;

    JmsMatsStageProcessor(JmsMatsStage<R, S, I, Z> jmsMatsStage, int processorNumber) {
        _randomInstanceId = RandomString.randomString(5) + "@" + jmsMatsStage.getParentFactory();
        _jmsMatsStage = jmsMatsStage;
        _processorNumber = processorNumber;
        _processorThread = new Thread(this::runner, THREAD_PREFIX + ident());
        _processorThread.start();
        _transactionContext = jmsMatsStage.getParentFactory()
                .getJmsMatsTransactionManager().getTransactionContext(this);
    }

    private volatile boolean _runFlag = true; // Start off running.

    private volatile JmsSessionHolder _jmsSessionHolder;

    private String ident() {
        return _jmsMatsStage.getStageId() + '#' + _processorNumber + " {" + _randomInstanceId + '}';
    }

    @Override
    public JmsMatsStage<?, ?, ?, ?> getStage() {
        return _jmsMatsStage;
    }

    @Override
    public JmsMatsFactory<Z> getFactory() {
        return _jmsMatsStage.getParentEndpoint().getParentFactory();
    }

    @Override
    public List<JmsMatsStartStoppable> getChildrenStartStoppable() {
        throw new AssertionError("This method should not have been called for [" + idThis() + "]");
    }

    private volatile boolean _processorInReceive;

    @Override
    public void start() {
        /* no-op */
    }

    @Override
    public void stopPhase0SetRunFlagFalseAndCloseSessionIfInReceive() {
        // Start by setting the run-flag to false..
        _runFlag = false;
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
            log.info(LOG_PREFIX + ident() + " has already exited, it should have closed JMS Session.");
            return;
        }

        // E-> ?: Is thread currently waiting in consumer.receive()?
        if (_processorInReceive) {
            // -> Yes, waiting in receive(), so close session, thus making receive() return null.
            log.info(LOG_PREFIX + ident() + " is waiting in consumer.receive(), so we'll close the current"
                    + " JmsSessionHolder thereby making the receive() call return null, and the thread will exit.");
            closeCurrentSessionHolder();
        }
        else {
            // -> No, not in receive()
            log.info(LOG_PREFIX + ident() + " is NOT waiting in consumer.receive(), so we assume it is out"
                    + " doing work, and will come back and see the run-flag being false, thus exit.");
        }
    }

    @Override
    public void stopPhase1GracefulWait(int gracefulShutdownMillis) {
        if (_processorThread.isAlive()) {
            log.info(LOG_PREFIX + "Thread " + ident() + " is running, waiting for it to exit gracefully for ["
                    + gracefulShutdownMillis + " ms].");
            joinProcessorThread(gracefulShutdownMillis);
            // ?: Did the thread exit?
            if (!_processorThread.isAlive()) {
                // -> Yes, thread exited.
                log.info(LOG_PREFIX + ident() + " exited nicely, it should have closed the JMS Session.");
            }
        }
    }

    @Override
    public void stopPhase2InterruptIfStillAlive() {
        if (_processorThread.isAlive()) {
            // -> No, thread did not exit within graceful wait period.
            log.warn(LOG_PREFIX + ident() + " DID NOT exit after grace period, so interrupt it and wait some more.");
            // -> No, so interrupt it from whatever it is doing.
            _processorThread.interrupt();
        }
    }

    @Override
    public boolean stopPhase3GracefulAfterInterrupt() {
        if (_processorThread.isAlive()) {
            // Wait a small time more after the interrupt.
            joinProcessorThread(EXTRA_GRACE_MILLIS);
            // ?: Did the thread exit now? (Log only)
            if (!_processorThread.isAlive()) {
                // -> Yes, thread exited.
                log.info(LOG_PREFIX + ident()
                        + " exited after being interrupted, it should have closed the JMS Session.");
            }
            else {
                log.warn(LOG_PREFIX + ident() + " DID NOT exit even after being interrupted."
                        + " Giving up, closing JMS Session.");
                closeCurrentSessionHolder();
            }
        }
        return !_processorThread.isAlive();
    }

    private void closeCurrentSessionHolder() {
        JmsSessionHolder currentJmsSessionHolder = _jmsSessionHolder;
        if (currentJmsSessionHolder != null) {
            currentJmsSessionHolder.close();
        }
        else {
            log.info(LOG_PREFIX + "There was no JMS Session in place...");
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
        OUTER: while (_runFlag) {
            log.info(LOG_PREFIX + "Getting JMS Session, Destination and Consumer for stage ["
                    + _jmsMatsStage.getStageId() + "].");
            { // Local-scope the 'newJmsSessionHolder' variable.
                JmsSessionHolder newJmsSessionHolder;
                try {
                    newJmsSessionHolder = _jmsMatsStage.getParentFactory()
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
                _jmsMatsStage.getAnyProcessorMadeConsumerLatch().countDown();

                // :: INNER RECEIVE-LOOP, where we'll use the JMS Session and MessageConsumer.receive().
                while (_runFlag) {
                    try { // :: Cleanup of MDC
                        MDC.put(MDC_MATS_INCOMING, "true");
                        // Check whether Session/Connection is ok (per contract with JmsSessionHolder)
                        _jmsSessionHolder.isSessionOk();
                        // :: GET NEW MESSAGE!! THIS IS THE MESSAGE PUMP!
                        Message message;
                        try {
                            _processorInReceive = true;
                            if (log.isDebugEnabled()) log.debug(LOG_PREFIX
                                    + "Going into JMS consumer.receive() for [" + destination + "].");
                            message = jmsConsumer.receive();
                        }
                        finally {
                            _processorInReceive = false;
                        }
                        // Need to check whether the JMS Message gotten is null, as that signals that the
                        // Consumer, Session or Connection was closed from another thread.
                        if (message == null) {
                            log.info(LOG_PREFIX + "!! Got null from JMS consumer.receive(), JMS Session"
                                    + " was probably closed due to shutdown or JMS error."
                                    + " Closing current JmsSessionHolder to clean up. Looping to check run-flag.");
                            closeCurrentSessionHolder();
                            continue OUTER;
                        }

                        // :: Perform the work inside the TransactionContext
                        DoAfterCommitRunnableHolder doAfterCommitRunnableHolder = new DoAfterCommitRunnableHolder();
                        long nanosStart = System.nanoTime();
                        try { // :: Going into Mats Transaction

                            JmsMatsMessageContext jmsMatsMessageContext = new JmsMatsMessageContext(_jmsSessionHolder);

                            _transactionContext.doTransaction(jmsMatsMessageContext, () -> {
                                // Assert that this is indeed a JMS MapMessage.
                                if (!(message instanceof MapMessage)) {
                                    String msg = "Got some JMS Message that is not instanceof JMS MapMessage"
                                            + " - cannot be a MATS message! Refusing this message!";
                                    log.error(LOG_PREFIX + msg + "\n" + message);
                                    throw new MatsRefuseMessageException(msg);
                                }

                                // ----- This is a MapMessage
                                MapMessage mapMessage = (MapMessage) message;

                                // :: Fetch Mats-specific message data from the JMS Message.

                                byte[] matsTraceBytes;
                                String matsTraceMeta;
                                String jmsMessageId;
                                try {
                                    String matsTraceKey = getFactory().getFactoryConfig().getMatsTraceKey();
                                    matsTraceBytes = mapMessage.getBytes(matsTraceKey);
                                    matsTraceMeta = mapMessage.getString(matsTraceKey
                                            + MatsSerializer.META_KEY_POSTFIX);
                                    jmsMessageId = mapMessage.getJMSMessageID();
                                    MDC.put(MDC_JMS_MESSAGE_ID_IN, jmsMessageId);

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

                                // :: Deserialize the MatsTrace from the message data.
                                MatsSerializer<Z> matsSerializer = getFactory().getMatsSerializer();
                                DeserializedMatsTrace<Z> matsTraceDeserialized = matsSerializer
                                        .deserializeMatsTrace(matsTraceBytes, matsTraceMeta);
                                MatsTrace<Z> matsTrace = matsTraceDeserialized.getMatsTrace();

                                // :: Setting MDC values from MatsTrace
                                MDC.put(MDC_TRACE_ID, matsTrace.getTraceId());
                                MDC.put(MDC_MATS_MESSAGE_ID_IN, matsTrace.getCurrentCall().getMatsMessageId());

                                // :: Current Call
                                Call<Z> currentCall = matsTrace.getCurrentCall();
                                // Assert that this is indeed a JMS Message meant for this Stage
                                if (!_jmsMatsStage.getStageId().equals(currentCall.getTo().getId())) {
                                    String msg = "The incoming MATS message is not to this Stage! this:["
                                            + _jmsMatsStage.getStageId()
                                            + "]," + " msg:[" + currentCall.getTo() + "]. Refusing this message!";
                                    log.error(LOG_PREFIX + msg + "\n" + mapMessage);
                                    throw new MatsRefuseMessageException(msg);
                                }

                                // :: Current State: If null, make an empty object instead, unless Void -> null.
                                Z currentSerializedState = matsTrace.getCurrentState();
                                S currentSto = (currentSerializedState == null
                                        ? (_jmsMatsStage.getStateClass() != Void.class
                                                ? matsSerializer.newInstance(_jmsMatsStage.getStateClass())
                                                : null)
                                        : matsSerializer.deserializeObject(currentSerializedState,
                                                _jmsMatsStage.getStateClass()));

                                // :: Incoming Message DTO
                                I incomingDto = handleIncomingMessageMatsObject(matsSerializer,
                                        _jmsMatsStage.getIncomingMessageClass(), currentCall.getData());

                                double millisTaken = (System.nanoTime() - nanosStart) / 1_000_000d;

                                log.info(LOG_PREFIX + "RECEIVED message from [" + currentCall.getFrom()
                                        + "@" + currentCall.getCallingAppName()
                                        + "{" + currentCall.getCallingAppVersion()
                                        + "}@" + currentCall.getCallingHost()
                                        + "], recv:[" + matsTraceBytes.length
                                        + " B]->decomp:[" + matsTraceMeta
                                        + " " + ms3(matsTraceDeserialized.getMillisDecompression())
                                        + " ms]->deserialize:[" + matsTraceDeserialized.getSizeDecompressed()
                                        + " B, " + ms3(matsTraceDeserialized.getMillisDeserialization())
                                        + " ms]->MT - tot w/DTO&STO:[" + ms3(millisTaken) + " ms].");

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
                                            log.warn("Got some object in the MapMessage to ["
                                                    + _jmsMatsStage.getStageId()
                                                    + "] which is neither byte[] nor String - which should not"
                                                    + " happen - Ignoring.");
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
                                JmsMatsProcessContext<R, S, Z> processContext = new JmsMatsProcessContext<>(
                                        getFactory(),
                                        _jmsMatsStage.getParentEndpoint().getEndpointId(),
                                        _jmsMatsStage.getStageId(),
                                        jmsMessageId,
                                        _jmsMatsStage.getNextStageId(),
                                        matsTraceBytes, 0, matsTraceBytes.length, matsTraceMeta,
                                        matsTrace,
                                        currentSto,
                                        incomingBinaries, incomingStrings,
                                        messagesToSend, jmsMatsMessageContext, doAfterCommitRunnableHolder);

                                _jmsMatsStage.getProcessLambda().process(processContext, currentSto, incomingDto);

                                // :: Send any outgoing Mats messages (replies, requests, new messages etc..)
                                sendMatsMessages(log, nanosStart, _jmsSessionHolder, getFactory(), messagesToSend);

                            }); // End: Mats Transaction
                        }
                        catch (RuntimeException e) {
                            log.info(LOG_PREFIX + "Got [" + e.getClass().getName()
                                    + "] inside transactional message processing, which shall have been handled by"
                                    + " the MATS TransactionManager (rollback). Looping to fetch next message.");
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

                        // :: Log final stats
                        double millisTotal = (System.nanoTime() - nanosStart) / 1_000_000d;
                        log.info(LOG_PREFIX + "Total time from received till finished processing: ["
                                + ms3(millisTotal) + "].");
                    }
                    finally {
                        MDC.clear();
                    }
                } // End: INNER RECEIVE-LOOP
            }

            catch (Throwable t) { // .. amongst which is JmsMatsJmsException & JMSException
                log.warn(LOG_PREFIX + "Got [" + t.getClass().getSimpleName() + "] inside the message processing"
                        + " loop, crashing JmsSessionHolder, chilling a bit, then looping.", t);
                _jmsSessionHolder.crashed(t);
                // Quick-check the run flag before chilling, if the reason for Exception is closed Connection or
                // Session due to shutdown. (ActiveMQ do not let you create Session if Connection is closed,
                // and do not let you create a Consumer if Session is closed.)
                if (!_runFlag) {
                    log.info("The run-flag was false, so we shortcut to exit.");
                    break;
                }
                /*
                 * Doing a "chill-wait", so that if we're in a situation where this will tight-loop, we won't totally
                 * swamp both CPU and logs with meaninglessness.
                 */
                chillWait();
            }
        } // END: OUTER RUN-LOOP
        log.info(LOG_PREFIX + ident() + " asked to exit, and that we do! Closing current JmsSessionHolder.");
        closeCurrentSessionHolder();
        _jmsMatsStage.removeStageProcessorFromList(this);
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
        String destinationName = factoryConfig.getMatsDestinationPrefix() + _jmsMatsStage.getStageId();
        if (_jmsMatsStage.isQueue()) {
            destination = jmsSession.createQueue(destinationName);
        }
        else {
            destination = jmsSession.createTopic(destinationName);
        }
        log.info(LOG_PREFIX + "Created JMS " + (_jmsMatsStage.isQueue() ? "Queue" : "Topic") + ""
                + " to receive from: [" + destination + "].");
        return destination;
    }

    @Override
    public String toString() {
        return idThis();
    }
}
