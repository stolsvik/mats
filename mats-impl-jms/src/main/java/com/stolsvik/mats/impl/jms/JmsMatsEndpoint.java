package com.stolsvik.mats.impl.jms;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint;

/**
 * The JMS implementation of {@link MatsEndpoint}.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class JmsMatsEndpoint<S, R> implements MatsEndpoint<S, R> {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsFactory.class);

    private final JmsMatsFactory _parentFactory;
    private final String _endpointId;
    private final boolean _queue;
    private final Class<S> _stateClass;
    private final Class<R> _replyClass;

    JmsMatsEndpoint(JmsMatsFactory parentFactory, String endpointId, boolean queue, Class<S> stateClass,
            Class<R> replyClass) {
        _parentFactory = parentFactory;
        _endpointId = endpointId;
        _queue = queue;
        _stateClass = stateClass;
        _replyClass = replyClass;
    }

    JmsMatsFactory getParentFactory() {
        return _parentFactory;
    }

    Class<S> getStateClass() {
        return _stateClass;
    }

    private List<JmsMatsStage<?, S, R>> _stages = new ArrayList<>();

    @Override
    public EndpointConfig getEndpointConfig() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <I> void stage(Class<I> incomingClass, ProcessLambda<I, S, R> processor) {
        // TODO: Refuse adding stages if already started, or if lastStage is added.
        // Make stageId, which is the endpointId for the first, then endpointId.stage1, stage2 etc.
        String stageId = _stages.size() == 0 ? _endpointId : _endpointId + ".stage" + (_stages.size() + 1);
        JmsMatsStage<I, S, R> stage = new JmsMatsStage<>(this, stageId, _queue,
                incomingClass, _stateClass, processor);
        // :: Set this next stage's Id on the previous stage, unless we're first, in which case there is no previous.
        if (_stages.size() > 0) {
            _stages.get(_stages.size() - 1).setNextStageId(stageId);
        }
        _stages.add(stage);
    }

    @Override
    public <I> void lastStage(Class<I> incomingClass, ProcessReturnLambda<I, S, R> processor) {
        // TODO: Refuse adding stages if already started, or if lastStage is added.
        // :: Wrap a standard ProcessLambda around the ProcessReturnLambda, performing the sole convenience it provides.
        stage(incomingClass, (processContext, incomingDto, state) -> {
            // Invoke the ProcessReturnLambda, holding on to the returned value from it.
            R replyDto = processor.process(processContext, incomingDto, state);
            // Replying with the returned value.
            processContext.reply(replyDto);
        });
        // Since this is the last stage, we'll start it now.
        start();
    }

    @Override
    public void start() {
        log.info(JmsMatsStatics.LOG_PREFIX + "   | Starting all stages for [" + _endpointId + "].");
        _stages.forEach(s -> s.start());
    }

    @Override
    public void close() {
        _stages.forEach(s -> s.close());
    }
}
