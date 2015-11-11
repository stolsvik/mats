package com.stolsvik.mats.impl.jms;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsTrace;
import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.TransactionContext;
import com.stolsvik.mats.util.MatsStringSerializer;

class JmsMatsInitiator implements MatsInitiator, JmsMatsStatics {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsInitiator.class);

    private final JmsMatsFactory _parentFactory;
    private final TransactionContext _transactionContext;
    private final MatsStringSerializer _matsJsonSerializer;

    public JmsMatsInitiator(JmsMatsFactory parentFactory, TransactionContext transactionalContext,
            MatsStringSerializer matsJsonSerializer) {
        _parentFactory = parentFactory;
        _transactionContext = transactionalContext;
        _matsJsonSerializer = matsJsonSerializer;
    }

    @Override
    public void initiate(InitiateLambda lambda) {
        Session jmsSession = _transactionContext.getTransactionalJmsSession(false);
        try {
            _transactionContext.performWithinTransaction(jmsSession,
                    () -> lambda.initiate(new JmsMatsInitiate(_parentFactory, jmsSession, _matsJsonSerializer)));
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

    static class JmsMatsInitiate implements MatsInitiate, JmsMatsStatics {
        @SuppressWarnings("hiding")
        private static final Logger log = LoggerFactory.getLogger(JmsMatsInitiate.class);

        private final JmsMatsFactory _parentFactory;
        private final Session _jmsSession;
        private final MatsStringSerializer _matsStringSerializer;

        JmsMatsInitiate(JmsMatsFactory parentFactory, Session jmsSession, MatsStringSerializer matsJsonSerializer) {
            _parentFactory = parentFactory;
            _jmsSession = jmsSession;
            _matsStringSerializer = matsJsonSerializer;
        }

        private String _traceId;
        private String _from;
        private String _to;
        private String _replyTo;
        private Map<String, byte[]> _binaries = new LinkedHashMap<>();
        private Map<String, String> _strings = new LinkedHashMap<>();

        @Override
        public MatsInitiate traceId(String traceId) {
            _traceId = traceId;
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
        public MatsInitiate addBinary(String key, byte[] payload) {
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
            MatsTrace matsTrace = MatsTrace.createNew(_traceId).addRequestCall(_from, _to,
                    _matsStringSerializer.serializeObject(requestDto),
                    Collections.singletonList(_replyTo),
                    _matsStringSerializer.serializeObject(replySto),
                    _matsStringSerializer.serializeObject(requestSto));

            sendMessage(log, _jmsSession, _parentFactory.getFactoryConfig(), _matsStringSerializer,
                    true, matsTrace, _to, "new REQUEST");
            try {
                _jmsSession.commit();
            }
            catch (JMSException e) {
                throw new MatsBackendException("Problems committing when sending new REQUEST message to [" + _to
                        + "] via JMS API", e);
            }
        }

        @Override
        public void send(Object messageDto) {
            send(messageDto, null);
        }

        @Override
        public void send(Object messageDto, Object requestSto) {
            checkCommon("All of 'traceId', 'from' and 'to' must be set when send(..)");
            MatsTrace matsTrace = MatsTrace.createNew(_traceId).addSendCall(_from, _to,
                    _matsStringSerializer.serializeObject(messageDto),
                    Collections.emptyList(),
                    _matsStringSerializer.serializeObject(requestSto));

            sendMessage(log, _jmsSession, _parentFactory.getFactoryConfig(), _matsStringSerializer,
                    true, matsTrace, _to, "new SEND");
            try {
                _jmsSession.commit();
            }
            catch (JMSException e) {
                throw new MatsBackendException("Problems committing when sending new SEND message to [" + _to
                        + "] via JMS API", e);
            }
        }

        @Override
        public void publish(Object messageDto) {
            publish(messageDto, null);
        }

        @Override
        public void publish(Object messageDto, Object requestSto) {
            checkCommon("All of 'traceId', 'from' and 'to' must be set when publish(..)");
            MatsTrace matsTrace = MatsTrace.createNew(_traceId).addSendCall(_from, _to,
                    _matsStringSerializer.serializeObject(messageDto),
                    Collections.emptyList(),
                    _matsStringSerializer.serializeObject(requestSto));

            sendMessage(log, _jmsSession, _parentFactory.getFactoryConfig(), _matsStringSerializer,
                    false, matsTrace, _to, "new PUBLISH");
            try {
                _jmsSession.commit();
            }
            catch (JMSException e) {
                throw new MatsBackendException("Problems committing when sending new PUBLISH message to [" + _to
                        + "] via JMS API", e);
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
