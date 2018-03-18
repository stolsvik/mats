package com.stolsvik.mats.impl.jms;

import java.util.Collections;
import java.util.LinkedHashMap;

import javax.jms.JMSException;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.util.com.stolsvik.mats.impl.serial.MatsTrace;
import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.TransactionContext;
import com.stolsvik.mats.util.com.stolsvik.mats.impl.serial.MatsSerializer;

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

        private String _existingTraceId;

        JmsMatsInitiate(JmsMatsFactory<Z> parentFactory, Session jmsSession, String existingTraceId, String fromStageId) {
            _parentFactory = parentFactory;
            _jmsSession = jmsSession;

            _existingTraceId = existingTraceId;

            _from = fromStageId;
        }

        private String _traceId;
        private String _from;
        private String _to;
        private String _replyTo;
        private final LinkedHashMap<String, Object> _props = new LinkedHashMap<>();
        private final LinkedHashMap<String, byte[]> _binaries = new LinkedHashMap<>();
        private final LinkedHashMap<String, String> _strings = new LinkedHashMap<>();

        @Override
        public MatsInitiate traceId(String traceId) {
            // ?: If we're an initiation from within a stage, append the traceId to the existing traceId, else set.
            _traceId = (_existingTraceId != null ? _existingTraceId + '|' + traceId : traceId);
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
        public MatsInitiate replyTo(String endpointId) {
            _replyTo = endpointId;
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
        public void request(Object requestDto, Object replySto) {
            request(requestDto, replySto, null);
        }

        @Override
        public void request(Object requestDto, Object replySto, Object requestSto) {
            String msg = "All of 'traceId', 'from', 'to' and 'replyTo' must be set when request(..)";
            checkCommon(msg);
            if (_replyTo == null) {
                throw new NullPointerException(msg + ": Missing 'replyTo'.");
            }
            MatsSerializer<Z> ser = _parentFactory.getMatsSerializer();
            MatsTrace<Z> matsTrace = ser.createNewMatsTrace(_traceId).addRequestCall(_from, _to,
                    ser.serializeObject(requestDto), Collections.singletonList(_replyTo),
                    ser.serializeObject(replySto), ser.serializeObject(requestSto));

            sendMatsMessage(log, _jmsSession, _parentFactory, true, matsTrace, _props, _binaries, _strings, _to,
                    "new REQUEST");
        }

        @Override
        public void send(Object messageDto) {
            send(messageDto, null);
        }

        @Override
        public void send(Object messageDto, Object requestSto) {
            checkCommon("All of 'traceId', 'from' and 'to' must be set when send(..)");
            MatsSerializer<Z> ser = _parentFactory.getMatsSerializer();
            MatsTrace<Z> matsTrace = ser.createNewMatsTrace(_traceId).addSendCall(_from, _to,
                    ser.serializeObject(messageDto), Collections.emptyList(), ser.serializeObject(requestSto));

            sendMatsMessage(log, _jmsSession, _parentFactory, true, matsTrace, _props, _binaries, _strings, _to,
                    "new SEND");
        }

        @Override
        public void publish(Object messageDto) {
            publish(messageDto, null);
        }

        @Override
        public void publish(Object messageDto, Object requestSto) {
            checkCommon("All of 'traceId', 'from' and 'to' must be set when publish(..)");
            MatsSerializer<Z> ser = _parentFactory.getMatsSerializer();
            MatsTrace<Z> matsTrace = ser.createNewMatsTrace(_traceId).addSendCall(_from, _to,
                    ser.serializeObject(messageDto), Collections.emptyList(), ser.serializeObject(requestSto));

            sendMatsMessage(log, _jmsSession, _parentFactory, false, matsTrace, _props, _binaries, _strings, _to,
                    "new PUBLISH");
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
