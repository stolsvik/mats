package com.stolsvik.mats.impl.jms;

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
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.TransactionalContext;
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
    }

    @Override
    public StageConfig getStageConfig() {
        // TODO Auto-generated method stub
        return null;
    }

    private volatile boolean _running;

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

    private volatile TransactionalContext _transactionalContext;

    @Override
    public void start() {
        log.info(JmsMatsStatics.LOG_PREFIX + "     \\-  Starting Stage [" + _stageId + "].");
        if (_running) {
            throw new IllegalStateException("Already started.");
        }

        _transactionalContext = _parentFactory.getJmsMatsTransactionManager().getTransactionalContext(this);

        _running = true;
        Thread thread = new Thread(this::runner, THREAD_PREFIX + _stageId);
        thread.start();
    }

    @Override
    public void close() {
        _running = false;

        TransactionalContext transactionalContext = _transactionalContext;
        if (_transactionalContext != null) {
            transactionalContext.close();
        }
    }

    private void runner() {
        while (_running) {
            try {
                Session jmsSession = _transactionalContext.getTransactionalJmsSession(true);
                FactoryConfig factoryConfig = _parentEndpoint.getParentFactory().getFactoryConfig();
                Destination destination = createJmsDestination(jmsSession, factoryConfig);
                MessageConsumer jmsConsumer = jmsSession.createConsumer(destination);

                while (_running) {
                    log.info(LOG_PREFIX + "Going into JMS consumer.receive() for [" + destination + "].");
                    Message message = jmsConsumer.receive();
                    if (message == null) {
                        log.info(LOG_PREFIX + "!! Got null from JMS consumer.receive(), Session was probably closed"
                                + " due to shutdown. Looping to check run-status.");
                        continue;
                    }

                    _transactionalContext.performWithinTransaction(jmsSession, () -> {
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

                        // :: Current State. If null, then make an empty object
                        // instead.
                        String currentStateString = matsTrace.getCurrentState();
                        currentStateString = (currentStateString != null ? currentStateString : "{}");
                        S currentSto = _matsJsonSerializer.deserializeObject(currentStateString, _stateClass);
                        log.info("State object: " + currentSto);

                        // :: Incoming DTO
                        I incomingDto = _matsJsonSerializer.deserializeObject(currentCall.getData(),
                                _incomingMessageClass);

                        // :: Invoke the process lambda (stage processor).
                        _processLambda.process(new JmsMatsProcessContext<S, R>(this, jmsSession, matsTrace, currentSto),
                                incomingDto, currentSto);
                    });
                }
            }
            catch (JMSException t) {
                log.error(LOG_PREFIX + "Got " + t.getClass().getSimpleName() + ", looping to check run-status.", t);
                // TODO: Stick in a "chillSomeSeconds", and ensure that the
                // entire thread is created afresh.
                continue;
            }
        }
        log.info(LOG_PREFIX + "Asked to exit, exiting.");
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
        log.info(LOG_PREFIX + "Created JMS " + (_queue ? "Queue" : "Topic") + " to receive from: [" + destination
                + "].");
        return destination;
    }

}
