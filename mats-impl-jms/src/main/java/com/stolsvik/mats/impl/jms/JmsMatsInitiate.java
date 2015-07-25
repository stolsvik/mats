package com.stolsvik.mats.impl.jms;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Queue;
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
    private final MatsStringSerializer _matsJsonSerializer;

    JmsMatsInitiate(JmsMatsFactory parentFactory, Session jmsSession, MatsStringSerializer matsJsonSerializer) {
        _parentFactory = parentFactory;
        _jmsSession = jmsSession;
        _matsJsonSerializer = matsJsonSerializer;
    }

    private String _traceId;
    private String _from;
    private String _to;
    private String _reply;
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
    public MatsInitiate reply(String endpointId) {
        _reply = endpointId;
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
    public void request(Object requestDto, Object replyStateDto) {
        // TODO Auto-generated method stub

    }

    @Override
    public void request(Object requestStateDto, Object requestDto, Object replyStateDto) {
        // TODO Auto-generated method stub

    }

    @Override
    public void invoke(Object requestDto) {
        invoke(null, requestDto);
    }

    @Override
    public void invoke(Object requestStateDto, Object requestDto) {
        try {
            FactoryConfig factoryConfig = _parentFactory.getFactoryConfig();

            MatsTrace trace = MatsTrace.createNew(_traceId);
            String requestStateString = requestStateDto != null
                    ? _matsJsonSerializer.serializeObject(requestStateDto)
                    : null;

            trace.addInvokeCall(_from, _to,
                    _matsJsonSerializer.serializeObject(requestDto),
                    Collections.emptyList(),
                    requestStateString);

            MapMessage mm = _jmsSession.createMapMessage();
            mm.setString(factoryConfig.getMatsTraceKey(), _matsJsonSerializer.serializeMatsTrace(trace));

            Queue destination = _jmsSession.createQueue(factoryConfig.getMatsDestinationPrefix() + _to);
            log.info("Created destionation to send to: [" + destination + "].");
            MessageProducer producer = _jmsSession.createProducer(destination);

            log.info(LOG_PREFIX + "SENDING new INVOKE message to [" + _to + "].");

            producer.send(mm);
            _jmsSession.commit();
        }
        catch (JMSException e) {
            throw new MatsBackendException("Problems talking with the JMS API", e);
        }
    }

    @Override
    public void publish(Object requestDto) {
        // TODO Auto-generated method stub

    }

    @Override
    public void publish(Object requestStateDto, Object requestDto) {
        // TODO Auto-generated method stub

    }

}