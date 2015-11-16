package com.stolsvik.mats.impl.jms;

import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsConfig.ConfigLambda;
import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.EndpointConfig;
import com.stolsvik.mats.MatsEndpoint.ProcessSingleLambda;
import com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.util.MatsStringSerializer;

public class JmsMatsFactory implements MatsFactory, JmsMatsStatics {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsFactory.class);

    /**
     * TODO: Must be removed, as it only uses a non-db transaction manager.
     */
    public static JmsMatsFactory createMatsFactory(ConnectionFactory jmsConnectionFactory,
            MatsStringSerializer matsStringSerializer) {
        return createMatsFactory(JmsMatsTransactionManager_Standalone.create(jmsConnectionFactory),
                matsStringSerializer);
    }

    public static JmsMatsFactory createMatsFactory(JmsMatsTransactionManager jmsMatsTransactionManager,
            MatsStringSerializer matsStringSerializer) {
        return new JmsMatsFactory(jmsMatsTransactionManager, matsStringSerializer);
    }

    private final JmsMatsTransactionManager _jmsMatsTransactionManager;
    private final MatsStringSerializer _matsStringSerializer;
    private final JmsMatsFactoryConfig _factoryConfig = new JmsMatsFactoryConfig();

    private JmsMatsFactory(JmsMatsTransactionManager jmsMatsTransactionManager,
            MatsStringSerializer matsStringSerializer) {
        _jmsMatsTransactionManager = jmsMatsTransactionManager;
        _matsStringSerializer = matsStringSerializer;
        log.info(LOG_PREFIX + "Created [" + id(this) + "].");
    }

    public JmsMatsTransactionManager getJmsMatsTransactionManager() {
        return _jmsMatsTransactionManager;
    }

    MatsStringSerializer getMatsStringSerializer() {
        return _matsStringSerializer;
    }

    private CopyOnWriteArrayList<MatsEndpoint<?, ?>> _createdEndpoints = new CopyOnWriteArrayList<>();
    private CopyOnWriteArrayList<MatsInitiator> _createdInitiators = new CopyOnWriteArrayList<>();

    @Override
    public FactoryConfig getFactoryConfig() {
        return _factoryConfig;
    }

    @Override
    public <S, R> MatsEndpoint<S, R> staged(String endpointId, Class<S> stateClass, Class<R> replyClass) {
        JmsMatsEndpoint<S, R> endpoint = new JmsMatsEndpoint<>(this, endpointId, true, stateClass, replyClass);
        _createdEndpoints.add(endpoint);
        return endpoint;
    }

    @Override
    public <S, R> MatsEndpoint<S, R> staged(String endpointId, Class<S> stateClass, Class<R> replyClass,
            ConfigLambda<EndpointConfig> configLambda) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <I, R> MatsEndpoint<Void, R> single(String endpointId, Class<I> incomingClass, Class<R> replyClass,
            ProcessSingleLambda<I, R> processor) {
        return single(endpointId, incomingClass, replyClass, (endpointConfig) -> {
            /* no-config */ } , processor);
    }

    @Override
    public <I, R> MatsEndpoint<Void, R> single(String endpointId, Class<I> incomingClass, Class<R> replyClass,
            ConfigLambda<EndpointConfig> configLambda, ProcessSingleLambda<I, R> processor) {
        JmsMatsEndpoint<Void, R> endpoint = new JmsMatsEndpoint<>(this, endpointId, true, Void.class, replyClass);
        // :: Wrap a standard ProcessLambda around the ProcessTerminatorLambda
        endpoint.stage(incomingClass, (processContext, incomingDto, state) -> {
            // Invoke the endpoint, storing the returned value... (Note: no state)
            R reply = processor.process(processContext, incomingDto);
            // ... and invoke reply on the context with that.
            // (The only difference between a normal multi-stage and a single endpoint is this one convenience)
            processContext.reply(reply);
        });
        _createdEndpoints.add(endpoint);
        configLambda.config(endpoint.getEndpointConfig());
        endpoint.start();
        return endpoint;
    }

    @Override
    public <I, S> MatsEndpoint<S, Void> terminator(String endpointId, Class<I> incomingClass, Class<S> stateClass,
            ProcessTerminatorLambda<I, S> processor) {
        return terminator(true, endpointId, incomingClass, stateClass, processor);
    }

    @Override
    public <I, S> MatsEndpoint<S, Void> terminator(String endpointId, Class<I> incomingClass, Class<S> stateClass,
            ConfigLambda<EndpointConfig> configLambda, ProcessTerminatorLambda<I, S> processor) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <I, S> MatsEndpoint<S, Void> subscriptionTerminator(String endpointId, Class<I> incomingClass,
            Class<S> stateClass, ProcessTerminatorLambda<I, S> processor) {
        return terminator(false, endpointId, incomingClass, stateClass, processor);
    }

    @Override
    public <I, S> MatsEndpoint<S, Void> subscriptionTerminator(String endpointId, Class<I> incomingClass,
            Class<S> stateClass, ConfigLambda<EndpointConfig> configLambda, ProcessTerminatorLambda<I, S> processor) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * INTERNAL method, since terminator(...) and subscriptionTerminator(...) is near identical.
     */
    private <S, I> MatsEndpoint<S, Void> terminator(boolean queue, String endpointId, Class<I> incomingClass,
            Class<S> stateClass, ProcessTerminatorLambda<I, S> processor) {
        JmsMatsEndpoint<S, Void> endpoint = new JmsMatsEndpoint<>(this, endpointId, queue, stateClass, Void.class);
        // :: Wrap a standard ProcessLambda around the ProcessTerminatorLambda
        endpoint.stage(incomingClass, (processContext, incomingDto, state) -> {
            // This is just a direct forward - there is no difference from a ProcessLambda, except Void reply type.
            processor.process(processContext, incomingDto, state);
        });
        _createdEndpoints.add(endpoint);
        endpoint.start();
        return endpoint;
    }

    @Override
    public MatsInitiator getInitiator(String initiatorId) {
        JmsMatsInitiator initiator = new JmsMatsInitiator(this,
                _jmsMatsTransactionManager.getTransactionContext(null), _matsStringSerializer);
        _createdInitiators.add(initiator);
        return initiator;
    }

    @Override
    public void start() {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() {
        log.info(LOG_PREFIX + "Closing [" + id(this) + "], thus closing all created endpoints and initiators.");
        for (MatsEndpoint<?, ?> endpoint : _createdEndpoints) {
            try {
                endpoint.close();
            }
            catch (Throwable t) {
                log.warn("Got some throwable when closing endpoint [" + endpoint + "].", t);
            }
        }
        for (MatsInitiator initiator : _createdInitiators) {
            try {
                initiator.close();
            }
            catch (Throwable t) {
                log.warn("Got some throwable when closing initiator [" + initiator + "].", t);
            }
        }
    }

    private class JmsMatsFactoryConfig implements FactoryConfig {

        private int _concurrency;

        @Override
        public FactoryConfig setConcurrency(int numberOfThreads) {
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
                return Runtime.getRuntime().availableProcessors();
            }
            return _concurrency;
        }

        @Override
        public boolean isRunning() {
            for (MatsEndpoint<?, ?> endpoint : _createdEndpoints) {
                if (endpoint.getEndpointConfig().isRunning()) {
                    return true;
                }
            }
            return false;
        }
    }
}
