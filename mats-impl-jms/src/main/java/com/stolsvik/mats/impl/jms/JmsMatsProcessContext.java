package com.stolsvik.mats.impl.jms;

import java.util.List;

import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsTrace;
import com.stolsvik.mats.util.MatsStringSerializer;

public class JmsMatsProcessContext<S, R> implements ProcessContext<R>, JmsMatsStatics {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsProcessContext.class);

    private final JmsMatsStage<?, ?, R> _matsStage;
    private final Session _jmsSession;
    private final MatsTrace _matsTrace;
    private final S _sto;

    public JmsMatsProcessContext(JmsMatsStage<?, ?, R> matsStage, Session jmsSession, MatsTrace matsTrace, S sto) {
        _matsStage = matsStage;
        _jmsSession = jmsSession;
        _matsTrace = matsTrace;
        _sto = sto;
    }

    @Override
    public MatsTrace getTrace() {
        return _matsTrace;
    }

    @Override
    public byte[] getBinary(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getString(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addBinary(String key, byte[] payload) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addString(String key, String payload) {
        // TODO Auto-generated method stub

    }

    @Override
    public void request(String endpointId, Object requestDto) {
        // :: Get infrastructure.
        JmsMatsFactory parentFactory = _matsStage.getParentEndpoint().getParentFactory();
        FactoryConfig factoryConfig = parentFactory.getFactoryConfig();
        MatsStringSerializer matsStringSerializer = parentFactory.getMatsStringSerializer();

        // :: Add next stage as replyTo endpoint Id
        List<String> stack = _matsTrace.getCurrentCall().getStack();
        stack.add(0, _matsStage.getNextStageId());

        // :: Create next MatsTrace
        MatsTrace requestMatsTrace = _matsTrace.addRequestCall(_matsStage.getStageId(), endpointId, matsStringSerializer
                .serializeObject(requestDto), stack, matsStringSerializer.serializeObject(_sto), null);

        sendMessage(log, _jmsSession, factoryConfig, matsStringSerializer, true, requestMatsTrace,
                endpointId, "REQUEST");
    }

    @Override
    public void reply(Object replyDto) {
        // :: Get infrastructure.
        JmsMatsFactory parentFactory = _matsStage.getParentEndpoint().getParentFactory();
        FactoryConfig factoryConfig = parentFactory.getFactoryConfig();
        MatsStringSerializer matsStringSerializer = parentFactory.getMatsStringSerializer();

        // :: Pop the replyTo endpointId from the stack
        List<String> stack = _matsTrace.getCurrentCall().getStack();
        if (stack.size() == 0) {
            // This is OK, it is just like a normal java call where you do not use return value, e.g. map.put(k, v).
            log.info("Stage [" + _matsStage.getStageId() + " invoked context.reply(..), but there are no elements"
                    + " on the stack, hence no one to reply to. Dropping message.");
            return;
        }
        String replyTo = stack.remove(0);

        // :: Create next MatsTrace
        MatsTrace replyMatsTrace = _matsTrace.addReplyCall(_matsStage.getStageId(), replyTo,
                matsStringSerializer.serializeObject(replyDto), stack);

        sendMessage(log, _jmsSession, factoryConfig, matsStringSerializer, true, replyMatsTrace,
                replyTo, "REPLY");
    }

    @Override
    public void next(Object incomingDto) {
        // :: Get infrastructure.
        JmsMatsFactory parentFactory = _matsStage.getParentEndpoint().getParentFactory();
        FactoryConfig factoryConfig = parentFactory.getFactoryConfig();
        MatsStringSerializer matsStringSerializer = parentFactory.getMatsStringSerializer();

        // :: Assert that we have a next-stage
        if (_matsStage.getNextStageId() == null) {
            throw new IllegalStateException("Stage [" + _matsStage.getStageId()
                    + "] invoked context.next(..), but there is no next stage.");
        }

        // :: Use same stack, as this is a "sideways call", thus the replyStack is same as for previous stage
        List<String> stack = _matsTrace.getCurrentCall().getStack();

        // :: Create next (heh!) MatsTrace
        MatsTrace nextMatsTrace = _matsTrace.addNextCall(_matsStage.getStageId(), _matsStage.getNextStageId(),
                matsStringSerializer.serializeObject(incomingDto), stack, matsStringSerializer.serializeObject(_sto));

        sendMessage(log, _jmsSession, factoryConfig, matsStringSerializer, true, nextMatsTrace,
                _matsStage.getNextStageId(), "NEXT");
    }

    @Override
    public void initiate(InitiateLambda lambda) {
        // TODO Auto-generated method stub

    }
}
