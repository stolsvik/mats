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

public class JmsMatsProcessContext<R> implements ProcessContext<R>, JmsMatsStatics {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsProcessContext.class);

    private final JmsMatsStage<?, ?, R> _matsStage;
    private final Session _jmsSession;
    private final MatsTrace _matsTrace;

    public JmsMatsProcessContext(JmsMatsStage matsStage, Session jmsSession, MatsTrace matsTrace) {
        _matsStage = matsStage;
        _jmsSession = jmsSession;
        _matsTrace = matsTrace;
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
        // TODO Auto-generated method stub

    }

    @Override
    public void reply(Object replyDto) {
        List<String> stack = _matsTrace.getCurrentCall().getStack();
        if (stack.size() == 0) {
            log.info("reply(..) was invoked, but there are no elements on the stack, hence none to reply to."
                    + " Dropping message.");
            return;
        }
        String replyTo = stack.remove(0);

        // :: Get infrastructure.
        JmsMatsFactory parentFactory = _matsStage.getParentEndpoint().getParentFactory();
        FactoryConfig factoryConfig = parentFactory.getFactoryConfig();
        MatsStringSerializer matsStringSerializer = parentFactory.getMatsStringSerializer();

        // :: Create next MatsTrace
        MatsTrace replyMatsTrace = _matsTrace.addReplyCall(_matsStage.getStageId(), replyTo,
                matsStringSerializer.serializeObject(replyDto), stack);

        sendMessage(log, _jmsSession, factoryConfig, matsStringSerializer, replyMatsTrace, replyTo, "REPLY");

    }

    @Override
    public void next(Object incomingDto) {
        // TODO Auto-generated method stub

    }

    @Override
    public void initiate(InitiateLambda lambda) {
        // TODO Auto-generated method stub

    }

}
