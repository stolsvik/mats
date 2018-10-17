package com.stolsvik.mats.impl.jms;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsConfig;
import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsStage;
import com.stolsvik.mats.MatsStage.StageConfig;

/**
 * The JMS implementation of {@link MatsEndpoint}.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class JmsMatsEndpoint<R, S, Z> implements MatsEndpoint<R, S> {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsEndpoint.class);

    private final JmsMatsFactory<Z> _parentFactory;
    private final String _endpointId;
    private final boolean _queue;
    private final Class<S> _stateClass;
    private final Class<R> _replyClass;

    JmsMatsEndpoint(JmsMatsFactory<Z> parentFactory, String endpointId, boolean queue, Class<S> stateClass,
            Class<R> replyClass) {
        _parentFactory = parentFactory;
        _endpointId = endpointId;
        _queue = queue;
        _stateClass = stateClass;
        _replyClass = replyClass;
    }

    private final JmsEndpointConfig _endpointConfig = new JmsEndpointConfig();

    JmsMatsFactory<Z> getParentFactory() {
        return _parentFactory;
    }

    String getEndpointId() {
        return _endpointId;
    }

    Class<S> getStateClass() {
        return _stateClass;
    }

    private List<JmsMatsStage<R, S, ?, Z>> _stages = new CopyOnWriteArrayList<>();

    @Override
    public EndpointConfig<R, S> getEndpointConfig() {
        return _endpointConfig;
    }

    @Override
    public <I> MatsStage<R, S, I> stage(Class<I> incomingClass, ProcessLambda<R, S, I> processor) {
        return stage(incomingClass, MatsFactory.NO_CONFIG, processor);
    }

    @Override
    public <I> MatsStage<R, S, I> stage(Class<I> incomingClass,
            Consumer<? super StageConfig<R, S, I>> stageConfigLambda,
            ProcessLambda<R, S, I> processor) {
        // TODO: Refuse adding stages if already started, or if lastStage is added.
        // Make stageId, which is the endpointId for the first, then endpointId.stage1, stage2 etc.
        String stageId = _stages.size() == 0 ? _endpointId : _endpointId + ".stage" + (_stages.size());
        JmsMatsStage<R, S, I, Z> stage = new JmsMatsStage<>(this, stageId, _queue,
                incomingClass, _stateClass, processor);
        // :: Set this next stage's Id on the previous stage, unless we're first, in which case there is no previous.
        if (_stages.size() > 0) {
            _stages.get(_stages.size() - 1).setNextStageId(stageId);
        }
        _stages.add(stage);
        stageConfigLambda.accept(stage.getStageConfig());
        @SuppressWarnings("unchecked")
        MatsStage<R, S, I> matsStage = stage;
        return matsStage;
    }

    @Override
    public <I> MatsStage<R, S, I> lastStage(Class<I> incomingClass, ProcessReturnLambda<R, S, I> processor) {
        return lastStage(incomingClass, MatsFactory.NO_CONFIG, processor);
    }

    @Override
    public <I> MatsStage<R, S, I> lastStage(Class<I> incomingClass,
            Consumer<? super StageConfig<R, S, I>> stageConfigLambda,
            com.stolsvik.mats.MatsEndpoint.ProcessReturnLambda<R, S, I> processor) {
        // TODO: Refuse adding stages if already started, or if lastStage is added.
        // :: Wrap a standard ProcessLambda around the ProcessReturnLambda, performing the return-reply convenience.
        MatsStage<R, S, I> stage = stage(incomingClass, stageConfigLambda,
                (processContext, state, incomingDto) -> {
                    // Invoke the ProcessReturnLambda, holding on to the returned value from it.
                    R replyDto = processor.process(processContext, state, incomingDto);
                    // Replying with the returned value.
                    processContext.reply(replyDto);
                });
        // Since this is the last stage, we'll finish the setup.
        finishSetup();
        return stage;
    }

    @Override
    public void finishSetup() {
        if (! _parentFactory.isHoldEndpointsUntilFactoryIsStarted()) {
            start();
        }
    }

    @Override
    public void start() {
        log.info(JmsMatsStatics.LOG_PREFIX + "   | Starting all stages for [" + _endpointId + "].");
        _stages.forEach(JmsMatsStage::start);
    }

    @Override
    public void waitForStarted() {
        _stages.forEach(JmsMatsStage::waitForStarted);
    }

    @Override
    public void stop() {
        _stages.forEach(JmsMatsStage::stop);
    }

    private class JmsEndpointConfig implements EndpointConfig<R, S> {
        private int _concurrency;

        @Override
        public MatsConfig setConcurrency(int numberOfThreads) {
            _concurrency = numberOfThreads;
            return this;
        }

        @Override
        public boolean isConcurrencyDefault() {
            return _concurrency == 0;
        }

        @Override
        public int getConcurrency() {
            if (_concurrency == 0) {
                return _parentFactory.getFactoryConfig().getConcurrency();
            }
            return _concurrency;
        }

        @Override
        public boolean isRunning() {
            for (JmsMatsStage<R, S, ?, Z> stage : _stages) {
                if (stage.getStageConfig().isRunning()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Class<?> getIncomingMessageClass() {
            return _stages.get(0).getStageConfig().getIncomingMessageClass();
        }

        @Override
        public Class<R> getReplyClass() {
            return _replyClass;
        }

        @Override
        public Class<S> getStateClass() {
            return _stateClass;
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<MatsStage<R, S, ?>> getStages() {
            // Hack to have the compiler shut up.
            return (List<MatsStage<R, S, ?>>) (List<?>) _stages;
        }
    }
}
