package com.stolsvik.mats.impl.jms;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Session;

import com.stolsvik.mats.serial.MatsTrace.Call.MessagingModel;
import com.stolsvik.mats.serial.MatsTrace.KeepMatsTrace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.TransactionContext;
import com.stolsvik.mats.serial.MatsSerializer;

class JmsMatsInitiator<Z> implements MatsInitiator, JmsMatsStatics {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsInitiator.class);

    private final JmsMatsFactory<Z> _parentFactory;
    private final TransactionContext _transactionContext;

    public JmsMatsInitiator(JmsMatsFactory<Z> parentFactory, TransactionContext transactionalContext) {
        _parentFactory = parentFactory;
        _transactionContext = transactionalContext;
    }

    @Override
    public void initiate(InitiateLambda lambda) {
        // TODO / OPTIMIZE: Do not create JMS Session for every initiation.
        // TODO / OPTIMIZE: Consider doing lazy init for TransactionContext too
        // as well as being able to "open" again after close? What about introspection/monitoring/instrumenting -
        // that is, "turn off" a MatsInitiator: Either hang requsts, or probably more interesting, fail them. And
        // that would be nice to use "close()" for: As long as it is closed, it can't be used. Need to evaluate.
        Session jmsSession = _transactionContext.getTransactionalJmsSession(false);
        try {
            _transactionContext.performWithinTransaction(jmsSession, () -> lambda.initiate(
                    new JmsMatsInitiate<>(_parentFactory, jmsSession)));
        }
        catch (JMSException e) {
            throw new MatsBackendException("Problems committing when performing MATS initiation via JMS API", e);
        }
        finally {
            try {
                jmsSession.close();
            }
            catch (JMSException e) {
                // Since the session should already have been committed, rollbacked, or whatever, we will just log this.
                log.warn(LOG_PREFIX + "Got JMSException when trying to close session used for MATS initiation.", e);
            }
        }
    }

    @Override
    public void close() {
        _transactionContext.close();
    }

    static class JmsMatsInitiate<Z> implements MatsInitiate, JmsMatsStatics {
        private static final Logger log = LoggerFactory.getLogger(JmsMatsInitiate.class);

        private final JmsMatsFactory<Z> _parentFactory;
        private final Session _jmsSession;

        JmsMatsInitiate(JmsMatsFactory<Z> parentFactory, Session jmsSession) {
            _parentFactory = parentFactory;
            _jmsSession = jmsSession;
        }

        private MatsTrace<Z> _existingMatsTrace;

        JmsMatsInitiate(JmsMatsFactory<Z> parentFactory, Session jmsSession, MatsTrace<Z> existingMatsTrace,
                        Map<String, Object> propsSetInStage) {
            _parentFactory = parentFactory;
            _jmsSession = jmsSession;

            _existingMatsTrace = existingMatsTrace;
            // Set the intial traceId, if the user chooses to not set it
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
                throw new IllegalArgumentException("Unknown KeepTrace enum ["+keepTrace+"].");
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
                            HOSTNAME, _from, now, "Tralala!")
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
                    HOSTNAME, now, "Callalala!");

            sendMatsMessage(log, nanosStart, _jmsSession, _parentFactory, matsTrace, _props, _binaries, _strings,
                    "new REQUEST");
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
                            HOSTNAME, _from, now, "Tralala!")
                    .addSendCall(_from,
                            _to, MessagingModel.QUEUE,
                            ser.serializeObject(messageDto), ser.serializeObject(initialTargetSto));

            copyOverAnyExistingTraceProperties(matsTrace);

            // TODO: Add debug info!
            matsTrace.getCurrentCall().setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                    _parentFactory.getFactoryConfig().getAppVersion(),
                    HOSTNAME, now, "Callalala!");

            sendMatsMessage(log, nanosStart, _jmsSession, _parentFactory, matsTrace, _props, _binaries, _strings,
                    "new SEND");
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
                            HOSTNAME, _from, now, "Tralala!")
                    .addSendCall(_from,
                            _to, MessagingModel.TOPIC,
                            ser.serializeObject(messageDto), ser.serializeObject(initialTargetSto));

            copyOverAnyExistingTraceProperties(matsTrace);

            // TODO: Add debug info!
            matsTrace.getCurrentCall().setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                    _parentFactory.getFactoryConfig().getAppVersion(),
                    HOSTNAME, now, "Callalala!");

            sendMatsMessage(log, nanosStart, _jmsSession, _parentFactory, matsTrace, _props, _binaries, _strings,
                    "new PUBLISH");
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
