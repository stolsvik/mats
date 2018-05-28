package com.stolsvik.mats.impl.jms;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.EndpointConfig;
import com.stolsvik.mats.MatsEndpoint.ProcessSingleLambda;
import com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsStage.StageConfig;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager_JmsAndJdbc.JdbcConnectionSupplier;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager_JmsOnly.JmsConnectionSupplier;
import com.stolsvik.mats.serial.MatsSerializer;

public class JmsMatsFactory<Z> implements MatsFactory, JmsMatsStatics {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsFactory.class);

    public static <Z> JmsMatsFactory<Z> createMatsFactory_JmsOnlyTransactions(String appName, String appVersion,
            JmsConnectionSupplier jmsConnectionSupplier,
            MatsSerializer<Z> matsSerializer) {
        return createMatsFactory(appName, appVersion, JmsMatsTransactionManager_JmsOnly.create(jmsConnectionSupplier),
                matsSerializer);
    }

    public static <Z> JmsMatsFactory<Z> createMatsFactory_JmsAndJdbcTransactions(String appName, String appVersion,
            JmsConnectionSupplier jmsConnectionSupplier,
            JdbcConnectionSupplier jdbcConnectionSupplier,
            MatsSerializer<Z> matsSerializer) {
        return createMatsFactory(appName, appVersion, JmsMatsTransactionManager_JmsAndJdbc.create(jmsConnectionSupplier,
                jdbcConnectionSupplier), matsSerializer);
    }

    public static <Z> JmsMatsFactory<Z> createMatsFactory(String appName, String appVersion,
            JmsMatsTransactionManager jmsMatsTransactionManager,
            MatsSerializer<Z> matsSerializer) {
        return new JmsMatsFactory<>(appName, appVersion, jmsMatsTransactionManager, matsSerializer);
    }

    private final String _appName;
    private final String _appVersion;
    private final JmsMatsTransactionManager _jmsMatsTransactionManager;
    private final MatsSerializer<Z> _matsSerializer;
    private final JmsMatsFactoryConfig _factoryConfig;

    private JmsMatsFactory(String appName, String appVersion,
            JmsMatsTransactionManager jmsMatsTransactionManager,
            MatsSerializer<Z> matsSerializer) {
        _appName = appName;
        _appVersion = appVersion;
        _jmsMatsTransactionManager = jmsMatsTransactionManager;
        _matsSerializer = matsSerializer;
        _factoryConfig = new JmsMatsFactoryConfig();
        log.info(LOG_PREFIX + "Created [" + id(this) + "].");
    }

    /**
     * Sets the JMS destination prefix, which will be employed by all queues and topics, (obviously) both for sending
     * and listeing.
     *
     * @param destinationPrefix the JMS queue and topic destination prefix - defaults to "mats:".
     */
    public void setMatsDestinationPrefix(String destinationPrefix) {
        _factoryConfig._matsDestinationPrefix = destinationPrefix;
    }

    public JmsMatsTransactionManager getJmsMatsTransactionManager() {
        return _jmsMatsTransactionManager;
    }

    MatsSerializer<Z> getMatsSerializer() {
        return _matsSerializer;
    }

    private CopyOnWriteArrayList<MatsEndpoint<?, ?>> _createdEndpoints = new CopyOnWriteArrayList<>();
    private CopyOnWriteArrayList<MatsInitiator> _createdInitiators = new CopyOnWriteArrayList<>();

    @Override
    public FactoryConfig getFactoryConfig() {
        return _factoryConfig;
    }

    @Override
    public <S, R> JmsMatsEndpoint<S, R, Z> staged(String endpointId, Class<S> stateClass, Class<R> replyClass) {
        return staged(endpointId, stateClass, replyClass, NO_CONFIG);
    }

    @Override
    public <S, R> JmsMatsEndpoint<S, R, Z> staged(String endpointId, Class<S> stateClass, Class<R> replyClass,
            Consumer<? super EndpointConfig<S, R>> endpointConfigLambda) {
        JmsMatsEndpoint<S, R, Z> endpoint = new JmsMatsEndpoint<>(this, endpointId, true, stateClass, replyClass);
        _createdEndpoints.add(endpoint);
        endpointConfigLambda.accept(endpoint.getEndpointConfig());
        return endpoint;
    }

    @Override
    public <I, R> MatsEndpoint<Void, R> single(String endpointId,
            Class<I> incomingClass,
            Class<R> replyClass,
            ProcessSingleLambda<I, R> processor) {
        return single(endpointId, incomingClass, replyClass, NO_CONFIG, NO_CONFIG, processor);
    }

    @Override
    public <I, R> JmsMatsEndpoint<Void, R, Z> single(String endpointId,
            Class<I> incomingClass,
            Class<R> replyClass,
            Consumer<? super EndpointConfig<Void, R>> endpointConfigLambda,
            Consumer<? super StageConfig<I, Void, R>> stageConfigLambda,
            ProcessSingleLambda<I, R> processor) {
        // Get a normal Staged Endpoint
        JmsMatsEndpoint<Void, R, Z> endpoint = staged(endpointId, Void.class, replyClass, endpointConfigLambda);
        // :: Wrap the ProcessSingleLambda in a single lastStage-ProcessReturnLambda
        endpoint.lastStage(incomingClass, stageConfigLambda,
                (processContext, incomingDto, state) -> {
                    // This is just a direct forward - albeit a single stage has no state.
                    return processor.process(processContext, incomingDto);
                });
        return endpoint;
    }

    @Override
    public <I, S> JmsMatsEndpoint<S, Void, Z> terminator(String endpointId,
            Class<I> incomingClass,
            Class<S> stateClass,
            ProcessTerminatorLambda<I, S> processor) {
        return terminator(true, endpointId, incomingClass, stateClass, NO_CONFIG, NO_CONFIG, processor);
    }

    @Override
    public <I, S> JmsMatsEndpoint<S, Void, Z> terminator(String endpointId,
            Class<I> incomingClass,
            Class<S> stateClass,
            Consumer<? super EndpointConfig<S, Void>> endpointConfigLambda,
            Consumer<? super StageConfig<I, S, Void>> stageConfigLambda,
            ProcessTerminatorLambda<I, S> processor) {
        return terminator(true, endpointId, incomingClass, stateClass, endpointConfigLambda, stageConfigLambda,
                processor);
    }

    @Override
    public <I, S> JmsMatsEndpoint<S, Void, Z> subscriptionTerminator(String endpointId, Class<I> incomingClass,
            Class<S> stateClass,
            ProcessTerminatorLambda<I, S> processor) {
        return terminator(false, endpointId, incomingClass, stateClass, NO_CONFIG, NO_CONFIG, processor);
    }

    @Override
    public <I, S> JmsMatsEndpoint<S, Void, Z> subscriptionTerminator(String endpointId, Class<I> incomingClass,
            Class<S> stateClass,
            Consumer<? super EndpointConfig<S, Void>> endpointConfigLambda,
            Consumer<? super StageConfig<I, S, Void>> stageConfigLambda,
            ProcessTerminatorLambda<I, S> processor) {
        return terminator(false, endpointId, incomingClass, stateClass, endpointConfigLambda, stageConfigLambda,
                processor);
    }

    /**
     * INTERNAL method, since terminator(...) and subscriptionTerminator(...) are near identical.
     */
    private <S, I> JmsMatsEndpoint<S, Void, Z> terminator(boolean queue, String endpointId,
            Class<I> incomingClass,
            Class<S> stateClass,
            Consumer<? super EndpointConfig<S, Void>> endpointConfigLambda,
            Consumer<? super StageConfig<I, S, Void>> stageConfigLambda,
            ProcessTerminatorLambda<I, S> processor) {
        // Need to create the JmsMatsEndpoint ourselves, since we need to set the queue-parameter.
        JmsMatsEndpoint<S, Void, Z> endpoint = new JmsMatsEndpoint<>(this, endpointId, queue, stateClass,
                Void.class);
        _createdEndpoints.add(endpoint);
        endpointConfigLambda.accept(endpoint.getEndpointConfig());
        // :: Wrap the ProcessTerminatorLambda in a single lastStage-ProcessReturnLambda
        endpoint.lastStage(incomingClass, stageConfigLambda,
                (processContext, incomingDto, state) -> {
                    // This is just a direct forward - there is no difference from a ProcessReturnLambda ...
                    processor.process(processContext, incomingDto, state);
                    // ... except we have no reply (Void reply type).
                    return null;
                });
        return endpoint;
    }

    @Override
    public MatsInitiator createInitiator() {
        JmsMatsInitiator<Z> initiator = new JmsMatsInitiator<>(this,
                _jmsMatsTransactionManager.getTransactionContext(null));
        _createdInitiators.add(initiator);
        return initiator;
    }

    @Override
    public void start() {
        // TODO Make the "delayed start" functionality.
    }

    @Override
    public void waitForStarted() {
        _createdEndpoints.forEach(MatsEndpoint::waitForStarted);
    }

    @Override
    public void stop() {
        log.info(LOG_PREFIX + "Closing [" + id(this) + "], thus closing all created endpoints and initiators.");
        for (MatsEndpoint<?, ?> endpoint : _createdEndpoints) {
            try {
                endpoint.stop();
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

        private String _matsDestinationPrefix = "mats:";

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

        @Override
        public String getAppName() {
            return _appName;
        }

        @Override
        public String getAppVersion() {
            return _appVersion;
        }

        @Override
        public String getMatsDestinationPrefix() {
            return _matsDestinationPrefix;
        }
    }
}
