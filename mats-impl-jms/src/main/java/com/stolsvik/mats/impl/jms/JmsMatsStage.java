package com.stolsvik.mats.impl.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
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
import com.stolsvik.mats.exceptions.MatsConnectionException;
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

    @Override
    public void start() {
        log.info(JmsMatsStatics.LOG_PREFIX + "     \\-  Starting Stage [" + _stageId + "].");
        if (_running) {
            throw new IllegalStateException("Already started.");
        }
        ConnectionFactory jmsConnectionFactory = _parentFactory.getJmsConnectionFactory();
        try {
            _jmsConnection = jmsConnectionFactory.createConnection();
        }
        catch (JMSException e) {
            throw new MatsConnectionException("Got a JMS Exception when trying to createConnection()"
                    + " on the JMS ConnectionFactory [" + jmsConnectionFactory + "].", e);
        }
        _running = true;
        Thread thread = new Thread(this::runner, THREAD_PREFIX + _stageId);
        thread.start();
    }

    @Override
    public void close() {
        _running = false;
        Session jmsSession = _jmsSession;
        if (jmsSession != null) {
            try {
                // From the JMS spec:
                // "This method is the only Session method that can be called concurrently."
                // "Closing a closed session must /not/ throw an exception."
                jmsSession.close();
            }
            catch (Throwable t) {
                log.error("When closing JMS Session due to invoked close() on " + this
                        + ", we got an unexpected exception.", t);
            }
        }
        Connection jmsConnection = _jmsConnection;
        if (jmsConnection != null) {
            try {
                jmsConnection.close();
            }
            catch (Throwable t) {
                log.error("Got some unexpected exception when trying to close JMS Connection ["
                        + jmsConnection + "].", t);
            }
        }
    }

    private Connection _jmsConnection;
    private volatile Session _jmsSession;

    private void runner() {
        while (_running) {
            try {
                _jmsSession = _jmsConnection.createSession(true, 0);
                FactoryConfig factoryConfig = _parentEndpoint.getParentFactory().getFactoryConfig();
                Destination destination = createJmsConsumer(factoryConfig);
                MessageConsumer jmsConsumer = _jmsSession.createConsumer(destination);
                _jmsConnection.start();

                while (_running) {
                    log.info(LOG_PREFIX + "Going into JMS consumer.receive() for [" + destination + "].");

                    MapMessage matsMM = receiveMessage(jmsConsumer);
                    // ?: Anything not OK?
                    if (matsMM == null) {
                        // -> Loop.
                        continue;
                    }

                    String matsTraceString = matsMM.getString(factoryConfig.getMatsTraceKey());
                    log.info("MatsTraceString:\n" + matsTraceString);
                    MatsTrace matsTrace = _matsJsonSerializer.deserializeMatsTrace(matsTraceString);

                    // :: Current Call
                    Call currentCall = matsTrace.getCurrentCall();
                    if (!_stageId.equals(currentCall.getTo())) {
                        log.error(LOG_PREFIX + "The incoming MATS message is not to this Stage! "
                                + "this:[" + _stageId + "], msg:[" + currentCall.getTo()
                                + "]. Refusing this message.\n" + matsMM);
                        // TODO: Ensure "RefuseMessge" semantics.
                        _jmsSession.rollback();
                        continue;
                    }

                    log.info(LOG_PREFIX + "RECEIVED message from:[" + currentCall.getFrom() + "].");

                    // :: Current State. If null, then make an empty object instead.
                    String currentStateString = matsTrace.getCurrentState();
                    currentStateString = (currentStateString != null ? currentStateString : "{}");
                    S currentSto = _matsJsonSerializer.deserializeObject(currentStateString, _stateClass);
                    log.info("State object: " + currentSto);

                    // :: Incoming DTO
                    I incomingDto = _matsJsonSerializer.deserializeObject(currentCall.getData(), _incomingMessageClass);

                    // :: Invoke the process lambda (stage processor).
                    try {
                        _processLambda.process(new JmsMatsProcessContext<S, R>(this, _jmsSession, matsTrace,
                                currentSto), incomingDto, currentSto);
                    }
                    catch (Exception e) {
                        log.error("The processing stage of [" + _stageId + "] raised an Exception."
                                + " Rolling back the JMS transaction, looping.", e);
                        _jmsSession.rollback();
                        continue;
                    }

                    _jmsSession.commit();
                }
            }
            catch (Throwable t) {
                log.error(LOG_PREFIX + "Got some unexpected Throwable. Closing JMS Session,"
                        + " looping to check run-status.", t);
                try {
                    _jmsSession.close();
                }
                catch (Throwable t2) {
                    log.error(LOG_PREFIX + "Got even more unexpected Throwable when trying to close JMS Session."
                            + " Looping to check run-status", t2);
                }
            }
        }
        log.info(LOG_PREFIX + "Asked to exit, exiting.");
    }

    private Destination createJmsConsumer(FactoryConfig factoryConfig) throws JMSException {
        Destination destination;
        String destinationName = factoryConfig.getMatsDestinationPrefix() + _stageId;
        if (_queue) {
            destination = _jmsSession.createQueue(destinationName);
        }
        else {
            destination = _jmsSession.createTopic(destinationName);
        }
        log.info(LOG_PREFIX + "Created destination to receive from: [" + destination + "].");
        return destination;
    }

    private MapMessage receiveMessage(MessageConsumer jmsConsumer) throws JMSException {
        Message message = jmsConsumer.receive();
        if (message == null) {
            log.info(LOG_PREFIX + "!! Got null from JMS consumer.receive(), Session was probably closed"
                    + " due to shutdown. Looping to check run-status.");
            return null;
        }

        if (!(message instanceof MapMessage)) {
            log.error(LOG_PREFIX + "Got some JMS Message that is not instanceof MapMessage"
                    + " - cannot be MATS. Looping to fetch next message.\n" + message);
            // TODO: Ensure "RefuseMessge" semantics.
            return null;
        }
        return (MapMessage) message;
    }

}
