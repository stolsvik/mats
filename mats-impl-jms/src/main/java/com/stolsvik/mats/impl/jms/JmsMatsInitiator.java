package com.stolsvik.mats.impl.jms;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.exceptions.MatsRuntimeException;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler.JmsSessionHolder;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.JmsMatsTxContextKey;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.TransactionContext;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager_JmsOnly.JmsMatsMessageSendException;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.serial.MatsTrace.Call.MessagingModel;
import com.stolsvik.mats.serial.MatsTrace.KeepMatsTrace;

class JmsMatsInitiator<Z> implements MatsInitiator, JmsMatsTxContextKey, JmsMatsStatics {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsInitiator.class);

    private final JmsMatsFactory<Z> _parentFactory;
    private final JmsMatsJmsSessionHandler _jmsMatsJmsSessionHandler;
    private final TransactionContext _transactionContext;

    public JmsMatsInitiator(JmsMatsFactory<Z> parentFactory, JmsMatsJmsSessionHandler jmsMatsJmsSessionHandler,
            JmsMatsTransactionManager jmsMatsTransactionManager) {
        // NOTICE! Due to multi-threading, whereby one Initiator might be used "globally" for e.g. a Servlet Container
        // having 200 threads, we cannot fetch a sole Session for the Initiator to be used for all initiations (as
        // it might be used concurrently by all the 200 Servlet Container threads). Thus, each initiation needs to
        // get hold of its own Session. However, the Sessions should be pooled.

        _parentFactory = parentFactory;
        _jmsMatsJmsSessionHandler = jmsMatsJmsSessionHandler;
        _transactionContext = jmsMatsTransactionManager.getTransactionContext(this);
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

        long nanosStart = System.nanoTime();
        JmsSessionHolder jmsSessionHolder;
        try {
            jmsSessionHolder = _jmsMatsJmsSessionHandler.getSessionHolder(this);
        }
        catch (JmsMatsJmsException e) {
            throw new MatsBackendException("Damn it.", e);
        }
        try {
            _transactionContext.doTransaction(jmsSessionHolder, () -> {
                List<JmsMatsMessage<Z>> messagesToSend = new ArrayList<>();
                lambda.initiate(new JmsMatsInitiate<>(_parentFactory, messagesToSend));
                sendMatsMessages(log, nanosStart, jmsSessionHolder.getSession(), _parentFactory, messagesToSend);
            });
            jmsSessionHolder.release();
        }
        catch (JmsMatsMessageSendException e) {
            // Catch any JmsMatsJmsException, as that indicates that there was a problem with JMS - so we should
            // "crash" the JmsSessionHolder to signal that the JMS Connection is probably broken.
            jmsSessionHolder.crashed(e);
            // This is a special variant of JmsMatsJmsException which is the "VERY BAD!" scenario.
            // TODO: Do retries if it fails!
            throw new MatsMessageSendException("Evidently got problems sending out the message after having run the"
                    + " process lambda and potentially committed other resources, typically database.", e);
        }
        catch (JmsMatsJmsException e) {
            // Catch any MatsBackendExceptions, as that indicates that there was a problem with JMS - so we should
            // "crash" the JmsSessionHolder to signal that the JMS Connection is probably broken.
            jmsSessionHolder.crashed(e);
            // .. then throw on. This is
            throw new MatsRuntimeException("Damn it.", e);
        }
    }

    @Override
    public void initiateUnchecked(InitiateLambda lambda) throws MatsBackendRuntimeException,
            MatsMessageSendRuntimeException {
        try {
            initiate(lambda);
        }
        catch (MatsBackendException e) {
            throw new MatsBackendRuntimeException("Wrapping the MatsBackendException in a unchecked variant", e);
        }
        catch (MatsMessageSendException e) {
            throw new MatsBackendRuntimeException("Wrapping the MatsMessageSendException in a unchecked variant", e);
        }
    }

    @Override
    public void close() {
        // TODO: Implement close semantics: Close all Sessions in any pool, and close Connection(s).
    }

    @Override
    public JmsMatsStage<?, ?, ?, ?> getStage() {
        // There is no stage, and contract is to return null.
        return null;
    }

    @Override
    public JmsMatsFactory<Z> getFactory() {
        return _parentFactory;
    }

    static class JmsMatsInitiate<Z> implements MatsInitiate, JmsMatsStatics {
        private static final Logger log = LoggerFactory.getLogger(JmsMatsInitiate.class);

        private final JmsMatsFactory<Z> _parentFactory;
        private final List<JmsMatsMessage<Z>> _messagesToSend;

        JmsMatsInitiate(JmsMatsFactory<Z> parentFactory, List<JmsMatsMessage<Z>> messagesToSend) {
            _parentFactory = parentFactory;
            _messagesToSend = messagesToSend;
        }

        private MatsTrace<Z> _existingMatsTrace;

        JmsMatsInitiate(JmsMatsFactory<Z> parentFactory, List<JmsMatsMessage<Z>> messagesToSend,
                MatsTrace<Z> existingMatsTrace, Map<String, Object> propsSetInStage) {
            _parentFactory = parentFactory;
            _messagesToSend = messagesToSend;

            _existingMatsTrace = existingMatsTrace;

            // Set the initial traceId, if the user chooses to not set it
            _traceId = existingMatsTrace.getTraceId();
            _from = existingMatsTrace.getCurrentCall().getFrom();

            // Copy over the properties which so far has been set in the stage (before this message is initiated).
            _props.putAll(propsSetInStage);
        }

        private String _traceId;
        private KeepMatsTrace _keepTrace;
        private boolean _nonPersistent;
        private boolean _interactive;
        private String _from;
        private String _to;
        private String _replyTo;
        private boolean _replyToSubscription;
        private Object _replySto;
        private final LinkedHashMap<String, Object> _props = new LinkedHashMap<>();
        private final LinkedHashMap<String, byte[]> _binaries = new LinkedHashMap<>();
        private final LinkedHashMap<String, String> _strings = new LinkedHashMap<>();

        @Override
        public MatsInitiate traceId(String traceId) {
            // ?: If we're an initiation from within a stage, append the traceId to the existing traceId, else set.
            _traceId = (_existingMatsTrace != null ? _existingMatsTrace.getTraceId() + '|' + traceId : traceId);
            return this;
        }

        @Override
        public MatsInitiate keepTrace(KeepTrace keepTrace) {
            if (keepTrace == KeepTrace.MINIMAL) {
                _keepTrace = KeepMatsTrace.MINIMAL;
            }
            else if (keepTrace == KeepTrace.COMPACT) {
                _keepTrace = KeepMatsTrace.COMPACT;
            }
            else if (keepTrace == KeepTrace.FULL) {
                _keepTrace = KeepMatsTrace.FULL;
            }
            else {
                throw new IllegalArgumentException("Unknown KeepTrace enum [" + keepTrace + "].");
            }
            return this;
        }

        @Override
        public MatsInitiate nonPersistent() {
            _nonPersistent = true;
            return this;
        }

        @Override
        public MatsInitiate interactive() {
            _interactive = true;
            return this;
        }

        @Override
        public MatsInitiate from(String initiatorId) {
            _from = initiatorId;
            return this;
        }

        @Override
        public MatsInitiate to(String endpointId) {
            _to = endpointId;
            return this;
        }

        @Override
        public MatsInitiate replyTo(String endpointId, Object replySto) {
            _replyTo = endpointId;
            _replySto = replySto;
            _replyToSubscription = false;
            return this;
        }

        @Override
        public MatsInitiate replyToSubscription(String endpointId, Object replySto) {
            _replyTo = endpointId;
            _replySto = replySto;
            _replyToSubscription = true;
            return this;
        }

        @Override
        public MatsInitiate setTraceProperty(String propertyName, Object propertyValue) {
            _props.put(propertyName, propertyValue);
            return this;
        }

        @Override
        public MatsInitiate addBytes(String key, byte[] payload) {
            _binaries.put(key, payload);
            return this;
        }

        @Override
        public MatsInitiate addString(String key, String payload) {
            _strings.put(key, payload);
            return this;
        }

        @Override
        public void request(Object requestDto) {
            request(requestDto, null);
        }

        @Override
        public void request(Object requestDto, Object initialTargetSto) {
            long nanosStart = System.nanoTime();
            String msg = "All of 'traceId', 'from', 'to' and 'replyTo' must be set when request(..)";
            checkCommon(msg);
            if (_replyTo == null) {
                throw new NullPointerException(msg + ": Missing 'replyTo'.");
            }
            MatsSerializer<Z> ser = _parentFactory.getMatsSerializer();
            long now = System.currentTimeMillis();
            MatsTrace<Z> matsTrace = ser.createNewMatsTrace(_traceId, _keepTrace, _nonPersistent, _interactive)
                    // TODO: Add debug info!
                    .setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                            _parentFactory.getFactoryConfig().getAppVersion(),
                            _parentFactory.getFactoryConfig().getNodename(), _from, now, "Tralala!")
                    .addRequestCall(_from,
                            _to, MessagingModel.QUEUE,
                            _replyTo, (_replyToSubscription ? MessagingModel.TOPIC : MessagingModel.QUEUE),
                            ser.serializeObject(requestDto),
                            ser.serializeObject(_replySto),
                            ser.serializeObject(initialTargetSto));

            copyOverAnyExistingTraceProperties(matsTrace);

            // TODO: Add debug info!
            matsTrace.getCurrentCall().setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                    _parentFactory.getFactoryConfig().getAppVersion(),
                    _parentFactory.getFactoryConfig().getNodename(), now, "Callalala!");

            // Produce the new REQUEST JmsMatsMessage to send
            JmsMatsMessage<Z> request = produceJmsMatsMessage(log, nanosStart, _parentFactory, matsTrace, _props,
                    _binaries, _strings, "new REQUEST");
            _messagesToSend.add(request);
        }

        @Override
        public void send(Object messageDto) {
            send(messageDto, null);
        }

        @Override
        public void send(Object messageDto, Object initialTargetSto) {
            long nanosStart = System.nanoTime();
            checkCommon("All of 'traceId', 'from' and 'to' must be set when send(..)");
            MatsSerializer<Z> ser = _parentFactory.getMatsSerializer();
            long now = System.currentTimeMillis();
            MatsTrace<Z> matsTrace = ser.createNewMatsTrace(_traceId, _keepTrace, _nonPersistent, _interactive)
                    // TODO: Add debug info!
                    .setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                            _parentFactory.getFactoryConfig().getAppVersion(),
                            _parentFactory.getFactoryConfig().getNodename(), _from, now, "Tralala!")
                    .addSendCall(_from,
                            _to, MessagingModel.QUEUE,
                            ser.serializeObject(messageDto), ser.serializeObject(initialTargetSto));

            copyOverAnyExistingTraceProperties(matsTrace);

            // TODO: Add debug info!
            matsTrace.getCurrentCall().setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                    _parentFactory.getFactoryConfig().getAppVersion(),
                    _parentFactory.getFactoryConfig().getNodename(), now, "Callalala!");

            // Produce the new SEND JmsMatsMessage to send
            JmsMatsMessage<Z> request = produceJmsMatsMessage(log, nanosStart, _parentFactory, matsTrace, _props,
                    _binaries, _strings, "new SEND");
            _messagesToSend.add(request);
        }

        @Override
        public void publish(Object messageDto) {
            publish(messageDto, null);
        }

        @Override
        public void publish(Object messageDto, Object initialTargetSto) {
            long nanosStart = System.nanoTime();
            checkCommon("All of 'traceId', 'from' and 'to' must be set when publish(..)");
            MatsSerializer<Z> ser = _parentFactory.getMatsSerializer();
            long now = System.currentTimeMillis();
            MatsTrace<Z> matsTrace = ser.createNewMatsTrace(_traceId, _keepTrace, _nonPersistent, _interactive)
                    // TODO: Add debug info!
                    .setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                            _parentFactory.getFactoryConfig().getAppVersion(),
                            _parentFactory.getFactoryConfig().getNodename(), _from, now, "Tralala!")
                    .addSendCall(_from,
                            _to, MessagingModel.TOPIC,
                            ser.serializeObject(messageDto), ser.serializeObject(initialTargetSto));

            copyOverAnyExistingTraceProperties(matsTrace);

            // TODO: Add debug info!
            matsTrace.getCurrentCall().setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                    _parentFactory.getFactoryConfig().getAppVersion(),
                    _parentFactory.getFactoryConfig().getNodename(), now, "Callalala!");

            // Produce the new PUBLISH JmsMatsMessage to send
            JmsMatsMessage<Z> request = produceJmsMatsMessage(log, nanosStart, _parentFactory, matsTrace, _props,
                    _binaries, _strings, "new PUBLISH");
            _messagesToSend.add(request);
        }

        private void copyOverAnyExistingTraceProperties(MatsTrace<Z> matsTrace) {
            // ?: Do we have an existing MatsTrace (implying that we are being initiated within a Stage)
            if (_existingMatsTrace != null) {
                // -> Yes, so copy over existing Trace Properties
                for (String key : _existingMatsTrace.getTracePropertyKeys()) {
                    matsTrace.setTraceProperty(key, _existingMatsTrace.getTraceProperty(key));
                }
            }
        }

        private void checkCommon(String msg) {
            if (_traceId == null) {
                throw new NullPointerException(msg + ": Missing 'traceId'.");
            }
            if (_from == null) {
                throw new NullPointerException(msg + ": Missing 'from'.");
            }
            if (_to == null) {
                throw new NullPointerException(msg + ": Missing 'to'.");
            }
        }
    }
}
