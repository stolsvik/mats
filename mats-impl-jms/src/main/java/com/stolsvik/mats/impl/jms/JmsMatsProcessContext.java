package com.stolsvik.mats.impl.jms;

import java.util.LinkedHashMap;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsStage;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.impl.jms.JmsMatsInitiator.JmsMatsInitiate;
import com.stolsvik.mats.serial.MatsSerializer;

/**
 * The JMS MATS implementation of {@link ProcessContext}. Instantiated for each incoming JMS message that is processed,
 * given to the {@link MatsStage}'s process lambda.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class JmsMatsProcessContext<S, R, Z> implements ProcessContext<R>, JmsMatsStatics {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsProcessContext.class);

    private final JmsMatsStage<?, ?, R, Z> _matsStage;
    private final Session _jmsSession;
    private final MapMessage _mapMessage;
    private final MatsTrace<Z> _matsTrace;
    private final S _sto;

    JmsMatsProcessContext(JmsMatsStage<?, ?, R, Z> matsStage, Session jmsSession, MapMessage mapMessage,
            MatsTrace<Z> matsTrace, S sto) {
        _matsStage = matsStage;
        _jmsSession = jmsSession;
        _mapMessage = mapMessage;
        _matsTrace = matsTrace;
        _sto = sto;
    }

    private final LinkedHashMap<String, Object> _props = new LinkedHashMap<>();
    private final LinkedHashMap<String, byte[]> _binaries = new LinkedHashMap<>();
    private final LinkedHashMap<String, String> _strings = new LinkedHashMap<>();

    @Override
    public String getStageId() {
        return _matsStage.getStageId();
    }

    @Override
    public String getFromStageId() {
        return _matsTrace.getCurrentCall().getFrom();
    }

    @Override
    public String toString() {
        return _matsTrace.toString();
    }

    @Override
    public String getTraceId() {
        return _matsTrace.getTraceId();
    }

    @Override
    public String getEndpointId() {
        return _matsStage.getParentEndpoint().getEndpointId();
    }

    @Override
    public byte[] getBytes(String key) {
        try {
            return _mapMessage.getBytes(key);
        }
        catch (JMSException e) {
            throw new MatsBackendException("Got JMS problems when trying to context.getBinary(\"" + key + "\").", e);
        }
    }

    @Override
    public String getString(String key) {
        try {
            return _mapMessage.getString(key);
        }
        catch (JMSException e) {
            throw new MatsBackendException("Got JMS problems when trying to context.getString(\"" + key + "\").", e);
        }
    }

    @Override
    public void addBytes(String key, byte[] payload) {
        _binaries.put(key, payload);
    }

    @Override
    public void addString(String key, String payload) {
        _strings.put(key, payload);
    }

    @Override
    public void setTraceProperty(String propertyName, Object propertyValue) {
        _props.put(propertyName, propertyValue);
    }

    @Override
    public <T> T getTraceProperty(String propertyName, Class<T> clazz) {
        MatsSerializer<Z> matsSerializer = _matsStage
                .getParentEndpoint().getParentFactory().getMatsSerializer();
        Z value = _matsTrace.getTraceProperty(propertyName);
        if (value == null) {
            throw new IllegalArgumentException("No value for property named [" + propertyName + "].");
        }
        return matsSerializer.deserializeObject(value, clazz);
    }

    @Override
    public void request(String endpointId, Object requestDto) {
        // :: Add next stage as replyTo endpoint Id
        List<String> stack = _matsTrace.getCurrentCall().getStack();
        stack.add(0, _matsStage.getNextStageId());

        // :: Create next MatsTrace
        MatsSerializer<Z> matsSerializer = _matsStage
                .getParentEndpoint().getParentFactory().getMatsSerializer();
        MatsTrace<Z> requestMatsTrace = _matsTrace.addRequestCall(_matsStage.getStageId(), endpointId, matsSerializer
                .serializeObject(requestDto), stack, matsSerializer.serializeObject(_sto), null);

        // Pack it off
        sendMatsMessage(log, _jmsSession, _matsStage.getParentEndpoint().getParentFactory(), true, requestMatsTrace,
                _props, _binaries, _strings, endpointId, "REQUEST");
    }

    @Override
    public void reply(Object replyDto) {
        // :: Pop the replyTo endpointId from the stack
        List<String> stack = _matsTrace.getCurrentCall().getStack();
        if (stack.size() == 0) {
            // This is OK, it is just like a normal java call where you do not use return value, e.g. map.put(k, v).
            log.info("Stage [" + _matsStage.getStageId() + " invoked context.reply(..), but there are no elements"
                    + " on the stack, hence no one to reply to. Dropping message.");
            return;
        }
        String replyToStageId = stack.remove(0);

        // :: Create next MatsTrace
        MatsSerializer<Z> matsSerializer = _matsStage
                .getParentEndpoint().getParentFactory().getMatsSerializer();
        MatsTrace<Z> replyMatsTrace = _matsTrace.addReplyCall(_matsStage.getStageId(), replyToStageId,
                matsSerializer.serializeObject(replyDto), stack);

        // Pack it off
        sendMatsMessage(log, _jmsSession, _matsStage.getParentEndpoint().getParentFactory(), true, replyMatsTrace,
                _props, _binaries, _strings, replyToStageId, "REPLY");
    }

    @Override
    public void next(Object incomingDto) {
        // :: Assert that we have a next-stage
        if (_matsStage.getNextStageId() == null) {
            throw new IllegalStateException("Stage [" + _matsStage.getStageId()
                    + "] invoked context.next(..), but there is no next stage.");
        }

        // :: Use same stack, as this is a "sideways call", thus the replyStack is same as for previous stage
        List<String> stack = _matsTrace.getCurrentCall().getStack();

        // :: Create next (heh!) MatsTrace
        MatsSerializer<Z> matsSerializer = _matsStage
                .getParentEndpoint().getParentFactory().getMatsSerializer();
        MatsTrace<Z> nextMatsTrace = _matsTrace.addNextCall(_matsStage.getStageId(), _matsStage.getNextStageId(),
                matsSerializer.serializeObject(incomingDto), stack, matsSerializer.serializeObject(_sto));

        // Pack it off
        sendMatsMessage(log, _jmsSession, _matsStage.getParentEndpoint().getParentFactory(), true, nextMatsTrace,
                _props, _binaries, _strings, _matsStage.getNextStageId(), "NEXT");
    }

    @Override
    public void initiate(InitiateLambda lambda) {
        lambda.initiate(new JmsMatsInitiate<>(_matsStage.getParentEndpoint().getParentFactory(), _jmsSession,
                _matsTrace.getTraceId(), _matsStage.getStageId()));
    }
}
