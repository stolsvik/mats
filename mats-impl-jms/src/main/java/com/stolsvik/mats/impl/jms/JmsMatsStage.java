package com.stolsvik.mats.impl.jms;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint.ProcessLambda;
import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsStage;
import com.stolsvik.mats.MatsTrace;
import com.stolsvik.mats.MatsTrace.Call;
import com.stolsvik.mats.exceptions.MatsRefuseMessageException;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.TransactionContext;
import com.stolsvik.mats.util.MatsStringSerializer;

public class JmsMatsStage<I, S, R> implements MatsStage, JmsMatsStatics {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsStage.class);

    private final JmsMatsEndpoint<S, R> _parentEndpoint;
    private final String _stageId;
    private final boolean _queue;
    private final Class<S> _stateClass;
    private final Class<I> _incomingMessageClass;
    private final ProcessLambda<I, S, R> _processLambda;

    private final MatsStringSerializer _matsJsonSerializer;
    private final JmsMatsFactory _parentFactory;

    public JmsMatsStage(JmsMatsEndpoint<S, R> parentEndpoint, String stageId, boolean queue,
            Class<I> incomingMessageClass, Class<S> stateClass, ProcessLambda<I, S, R> processLambda) {
        _parentEndpoint = parentEndpoint;
        _stageId = stageId;
        _queue = queue;
        _stateClass = stateClass;
        _incomingMessageClass = incomingMessageClass;
        _processLambda = processLambda;

        _parentFactory = _parentEndpoint.getParentFactory();
        _matsJsonSerializer = _parentFactory.getMatsStringSerializer();

        log.info(LOG_PREFIX + "Created Stage [" + id(_stageId, this) + "].");
    }

    @Override
    public StageConfig getStageConfig() {
        // TODO Auto-generated method stub
        return null;
    }

    private String _nextStageId;

    void setNextStageId(String nextStageId) {
        _nextStageId = nextStageId;
    }

    String getNextStageId() {
        return _nextStageId;
    }

    JmsMatsEndpoint<S, R> getParentEndpoint() {
        return _parentEndpoint;
    }

    String getStageId() {
        return _stageId;
    }

    private volatile TransactionContext _transactionContext;

    private final List<StageProcessor> _stageProcessors = new CopyOnWriteArrayList<>();

    @Override
    public void start() {
        log.info(JmsMatsStatics.LOG_PREFIX + "     \\-  Starting Stage [" + id(_stageId, this) + "].");
        if (_stageProcessors.size() > 1) {
            throw new IllegalStateException("Already started.");
        }

        _transactionContext = _parentFactory.getJmsMatsTransactionManager().getTransactionContext(this);
        int numberOfProcessors = 4;
        // ?: Is this a topic?
        if (!_queue) {
            /*
             * -> Yes, and in that case, there shall only be one StageProcessor for the endpoint. If the user chooses to
             * make more endpoints picking from the same topic, then so be it, but it generally makes no sense, as the
             * whole point of a MQ Topic is that all listeners will get the same messages.
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
        _stageProcessors.forEach(StageProcessor::start);
    }

    @Override
    public void close() {
        log.info(LOG_PREFIX + "Closing [" + _stageId + "], setting running to false,"
                + " stopping StageProcessors, closing TransactionalContext.");
        _stageProcessors.forEach(sp -> sp.stop(5000));
        _stageProcessors.clear();

        _transactionContext.close();
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

    /**
     * Package access so that I can refer to it in JavaDoc.
     */
    class StageProcessor implements JmsMatsStatics {

        private final int _processorNumber;

        private volatile boolean _run;

        private volatile Thread _processorThread;

        private Session _jmsSession;

        private volatile boolean _inReceive;

        public StageProcessor(int processorNumber) {
            _processorNumber = processorNumber;
        }

        void start() {
            _run = true;
            _processorThread = new Thread(this::runner, THREAD_PREFIX + _stageId + " " + id());
            _processorThread.start();
        }

        private String id() {
            return id("StageProcessor#" + _processorNumber, this);
        }

        void stop(int gracefulWaitMillis) {
            Session jmsSessionToClose;
            synchronized (this) {
                _run = false;
                jmsSessionToClose = _jmsSession;
            }
            /*
             * Trying to make very graceful: If we're in consumer.receive(), then close the Session, which makes the
             * receive()-call return null, causing the thread to loop and check run-flag. If not, then assume that the
             * thread is out doing work, and it will see that the run-flag is false upon next loop.
             *
             * We won't put too much effort in making this race-proof, as if it fails, which should be seldom, the only
             * problems are a couple of ugly stack traces in the log: Transactionality will keep integrity.
             */
            boolean sessionClosed = false;
            if (_inReceive) {
                log.info(LOG_PREFIX + id() + " is waiting in consumer.receive(), so we'll close the JMS Session,"
                        + " thereby making the receive() call return null, and the thread will exit.");
                closeJmsSession(jmsSessionToClose);
                sessionClosed = true;
            }
            else {
                log.info(LOG_PREFIX + id() + " is NOT waiting in consumer.receive(), so we assume it is out doing"
                        + "work, and will come back and see the run-flag being false, thus exit.");
            }
            // ?: Do we have a processor-thread?
            if (_processorThread != null) {
                // -> Yes, so wait for it to exit, with a max-time.
                log.info(LOG_PREFIX + "Waiting for " + id() + "-thread to exit.");
                try {
                    _processorThread.join(gracefulWaitMillis);
                }
                catch (InterruptedException e) {
                    log.warn(LOG_PREFIX + "Got InterruptedException when waiting for " + id() + "-thread to join."
                            + " Dropping out.");
                }
                // ?: Did the thread exit?
                if (_processorThread.isAlive()) {
                    log.warn(LOG_PREFIX + id() + "-thread did not exit, so interrupting it, and closing JMS Session.");
                    // -> No, so interrupt it from whatever it is doing.
                    _processorThread.interrupt();
                    // Close the JMS Session from under its feet.
                    closeJmsSession(jmsSessionToClose);
                    sessionClosed = true;
                }
            }
            // ?: Have we already closed the session?
            if (!sessionClosed) {
                // -> No, so do it now.
                closeJmsSession(jmsSessionToClose);
            }
        }

        private void runner() {
            while (_run) {
                try {
                    log.info(LOG_PREFIX + "Getting JMS Session, Destination and Consumer for stage [" + _stageId
                            + "].");
                    Session newSession = _transactionContext.getTransactionalJmsSession(true);
                    synchronized (this) {
                        if (_run) {
                            _jmsSession = newSession;
                        }
                        else {
                            closeJmsSession(newSession);
                            break;
                        }
                    }
                    FactoryConfig factoryConfig = _parentEndpoint.getParentFactory().getFactoryConfig();
                    Destination destination = createJmsDestination(_jmsSession, factoryConfig);
                    MessageConsumer jmsConsumer = _jmsSession.createConsumer(destination);

                    while (_run) {
                        _inReceive = true;
                        log.info(LOG_PREFIX + "Going into JMS consumer.receive() for [" + destination + "].");
                        Message message = jmsConsumer.receive();
                        _inReceive = false;
                        if (message == null) {
                            log.info(LOG_PREFIX + "!! Got null from JMS consumer.receive(), JMS Session"
                                    + " was probably closed due to shutdown. Looping to check run-flag.");
                            continue;
                        }

                        _transactionContext.performWithinTransaction(_jmsSession, () -> {
                            if (!(message instanceof MapMessage)) {
                                String msg = "Got some JMS Message that is not instanceof MapMessage"
                                        + " - cannot be a MATS message! Refusing this message!";
                                log.error(LOG_PREFIX + msg + "\n" + message);
                                throw new MatsRefuseMessageException(msg);
                            }

                            MapMessage matsMM = (MapMessage) message;

                            String matsTraceString = matsMM.getString(factoryConfig.getMatsTraceKey());
                            log.info("MatsTraceString:\n" + matsTraceString);
                            MatsTrace matsTrace = _matsJsonSerializer.deserializeMatsTrace(matsTraceString);

                            // :: Current Call
                            Call currentCall = matsTrace.getCurrentCall();
                            if (!_stageId.equals(currentCall.getTo())) {
                                String msg = "The incoming MATS message is not to this Stage! this:[" + _stageId + "],"
                                        + " msg:[" + currentCall.getTo() + "]. Refusing this message!";
                                log.error(LOG_PREFIX + msg + "\n" + matsMM);
                                throw new MatsRefuseMessageException(msg);
                            }

                            log.info(LOG_PREFIX + "RECEIVED message from:[" + currentCall.getFrom() + "].");

                            // :: Current State. If null, then make an empty object instead.
                            String currentStateString = matsTrace.getCurrentState();
                            currentStateString = (currentStateString != null ? currentStateString : "{}");
                            S currentSto = _matsJsonSerializer.deserializeObject(currentStateString, _stateClass);

                            // :: Incoming DTO
                            I incomingDto = _matsJsonSerializer.deserializeObject(currentCall.getData(),
                                    _incomingMessageClass);

                            // :: Invoke the process lambda (stage processor).
                            _processLambda.process(new JmsMatsProcessContext<S, R>(JmsMatsStage.this, _jmsSession,
                                    matsTrace, currentSto), incomingDto, currentSto);
                        });
                    }
                }
                catch (JMSException t) {
                    log.error(LOG_PREFIX + "Got " + t.getClass().getSimpleName() + ", closing JMS Session,"
                            + " looping to check run-flag.", t);
                    closeJmsSession(_jmsSession);
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
            log.info(LOG_PREFIX + id() + " asked to exit.");
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
