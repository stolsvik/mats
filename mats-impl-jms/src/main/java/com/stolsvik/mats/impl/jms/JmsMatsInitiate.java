package com.stolsvik.mats.impl.jms;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;
import com.stolsvik.mats.MatsTrace;
import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.util.MatsStringSerializer;

class JmsMatsInitiate implements MatsInitiate, JmsMatsStatics {
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
        if (_from == null) {
            throw new NullPointerException(
                    "Both 'from', 'to' and 'replyTo' must be set when request(..): Missing 'from'.");
        }
        if (_to == null) {
            throw new NullPointerException(
                    "Both 'from', 'to' and 'replyTo' must be set when request(..): Missing 'to'.");
        }
        if (_replyTo == null) {
            throw new NullPointerException(
                    "Both 'from', 'to' and 'replyTo' must be set when request(..): Missing 'replyTo'.");
        }
        FactoryConfig factoryConfig = _parentFactory.getFactoryConfig();

        MatsTrace matsTrace = MatsTrace.createNew(_traceId);

        matsTrace = matsTrace.addRequestCall(_from, _to, _matsStringSerializer.serializeObject(requestDto),
                Collections.singletonList(_replyTo),
                _matsStringSerializer.serializeObject(replySto),
                _matsStringSerializer.serializeObject(requestSto));

        sendMessage(log, _jmsSession, factoryConfig, _matsStringSerializer, matsTrace, _to, "new REQUEST");
        try {
            _jmsSession.commit();
        }
        catch (JMSException e) {
            throw new MatsBackendException("Problems committing when sending new REQUEST message to [" + _to
                    + "] via JMS API", e);
        }
    }

    @Override
    public void send(Object requestDto) {
        send(requestDto, null);
    }

    @Override
    public void send(Object requestDto, Object requestSto) {
        if (_from == null) {
            throw new NullPointerException("Both 'from' and 'to' must be set when invoke(..): Missing 'from'.");
        }
        if (_to == null) {
            throw new NullPointerException("Both 'from' and 'to' must be set when invoke(..): Missing 'to'.");
        }
        FactoryConfig factoryConfig = _parentFactory.getFactoryConfig();

        MatsTrace matsTrace = MatsTrace.createNew(_traceId);

        matsTrace = matsTrace.addInvokeCall(_from, _to,
                _matsStringSerializer.serializeObject(requestDto),
                Collections.emptyList(),
                _matsStringSerializer.serializeObject(requestSto));

        sendMessage(log, _jmsSession, factoryConfig, _matsStringSerializer, matsTrace, _to, "new INVOKE");
        try {
            _jmsSession.commit();
        }
        catch (JMSException e) {
            throw new MatsBackendException("Problems committing when sending new INVOKE message to [" + _to
                    + "] via JMS API", e);
        }
    }

    @Override
    public void publish(Object requestDto) {
        // TODO Auto-generated method stub

    }

    @Override
    public void publish(Object requestDto, Object requestStateDto) {
        // TODO Auto-generated method stub

    }

}