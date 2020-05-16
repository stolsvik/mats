package com.stolsvik.mats.impl.jms;

import java.io.BufferedInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.EndpointConfig;
import com.stolsvik.mats.MatsEndpoint.ProcessSingleLambda;
import com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsStage.StageConfig;
import com.stolsvik.mats.serial.MatsSerializer;

public class JmsMatsFactory<Z> implements MatsFactory, JmsMatsStatics, JmsMatsStartStoppable {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsFactory.class);

    public static <Z> JmsMatsFactory<Z> createMatsFactory_JmsOnlyTransactions(String appName, String appVersion,
            JmsMatsJmsSessionHandler jmsMatsJmsSessionHandler,
            MatsSerializer<Z> matsSerializer) {
        return createMatsFactory(appName, appVersion, jmsMatsJmsSessionHandler,
                JmsMatsTransactionManager_JmsOnly.create(), matsSerializer);
    }

    public static <Z> JmsMatsFactory<Z> createMatsFactory_JmsAndJdbcTransactions(String appName, String appVersion,
            JmsMatsJmsSessionHandler jmsMatsJmsSessionHandler, DataSource dataSource,
            MatsSerializer<Z> matsSerializer) {
        return createMatsFactory(appName, appVersion, jmsMatsJmsSessionHandler,
                JmsMatsTransactionManager_JmsAndJdbc.create(dataSource), matsSerializer);
    }

    public static <Z> JmsMatsFactory<Z> createMatsFactory(String appName, String appVersion,
            JmsMatsJmsSessionHandler jmsMatsJmsSessionHandler,
            JmsMatsTransactionManager jmsMatsTransactionManager,
            MatsSerializer<Z> matsSerializer) {
        return new JmsMatsFactory<>(appName, appVersion, jmsMatsJmsSessionHandler, jmsMatsTransactionManager,
                matsSerializer);
    }

    static {
        // Initialize any Specifics for the MessageBrokers that are on the classpath (i.e. if ActiveMQ is there..)
        JmsMatsMessageBrokerSpecifics.init();
    }

    private final String _appName;
    private final String _appVersion;
    private final JmsMatsJmsSessionHandler _jmsMatsJmsSessionHandler;
    private final JmsMatsTransactionManager _jmsMatsTransactionManager;
    private final MatsSerializer<Z> _matsSerializer;
    private final JmsMatsFactoryConfig _factoryConfig;

    private JmsMatsFactory(String appName, String appVersion,
            JmsMatsJmsSessionHandler jmsMatsJmsSessionHandler,
            JmsMatsTransactionManager jmsMatsTransactionManager,
            MatsSerializer<Z> matsSerializer) {
        _appName = appName;
        _appVersion = appVersion;
        _jmsMatsJmsSessionHandler = jmsMatsJmsSessionHandler;
        _jmsMatsTransactionManager = jmsMatsTransactionManager;
        _matsSerializer = matsSerializer;
        _factoryConfig = new JmsMatsFactoryConfig();

        log.info(LOG_PREFIX + "Created [" + idThis() + "].");
    }

    private final List<JmsMatsEndpoint<?, ?, Z>> _createdEndpoints = new ArrayList<>();
    private final List<JmsMatsInitiator<Z>> _createdInitiators = new ArrayList<>();

    private volatile String _nodename = getHostname_internal();
    private volatile boolean _holdEndpointsUntilFactoryIsStarted;

    private static String getHostname_internal() {
        try (BufferedInputStream in = new BufferedInputStream(Runtime.getRuntime().exec("hostname").getInputStream())) {
            byte[] b = new byte[256];
            int readBytes = in.read(b, 0, b.length);
            // Using platform default charset, which probably is exactly what we want in this one specific case.
            return new String(b, 0, readBytes).trim();
        }
        catch (Throwable t) {
            try {
                return InetAddress.getLocalHost().getHostName();
            }
            catch (UnknownHostException e) {
                return "_cannot_find_hostname_";
            }
        }
    }

    public JmsMatsJmsSessionHandler getJmsMatsJmsSessionHandler() {
        return _jmsMatsJmsSessionHandler;
    }

    public JmsMatsTransactionManager getJmsMatsTransactionManager() {
        return _jmsMatsTransactionManager;
    }

    public MatsSerializer<Z> getMatsSerializer() {
        return _matsSerializer;
    }

    @Override
    public FactoryConfig getFactoryConfig() {
        return _factoryConfig;
    }

    @Override
    public <R, S> JmsMatsEndpoint<R, S, Z> staged(String endpointId, Class<R> replyClass, Class<S> stateClass) {
        return staged(endpointId, replyClass, stateClass, NO_CONFIG);
    }

    @Override
    public <R, S> JmsMatsEndpoint<R, S, Z> staged(String endpointId, Class<R> replyClass, Class<S> stateClass,
            Consumer<? super EndpointConfig<R, S>> endpointConfigLambda) {
        JmsMatsEndpoint<R, S, Z> endpoint = new JmsMatsEndpoint<>(this, endpointId, true, stateClass, replyClass);
        addCreatedEndpoint(endpoint);
        endpointConfigLambda.accept(endpoint.getEndpointConfig());
        return endpoint;
    }

    @Override
    public <R, I> MatsEndpoint<R, Void> single(String endpointId,
            Class<R> replyClass, Class<I> incomingClass,
            ProcessSingleLambda<R, I> processor) {
        return single(endpointId, replyClass, incomingClass, NO_CONFIG, NO_CONFIG, processor);
    }

    @Override
    public <R, I> JmsMatsEndpoint<R, Void, Z> single(String endpointId,
            Class<R> replyClass, Class<I> incomingClass,
            Consumer<? super EndpointConfig<R, Void>> endpointConfigLambda,
            Consumer<? super StageConfig<R, Void, I>> stageConfigLambda,
            ProcessSingleLambda<R, I> processor) {
        // Get a normal Staged Endpoint
        JmsMatsEndpoint<R, Void, Z> endpoint = staged(endpointId, replyClass, Void.TYPE, endpointConfigLambda);
        // :: Wrap the ProcessSingleLambda in a single lastStage-ProcessReturnLambda
        endpoint.lastStage(incomingClass, stageConfigLambda,
                (processContext, state, incomingDto) -> {
                    // This is just a direct forward - albeit a single stage has no state.
                    return processor.process(processContext, incomingDto);
                });
        return endpoint;
    }

    @Override
    public <S, I> JmsMatsEndpoint<Void, S, Z> terminator(String endpointId,
            Class<S> stateClass, Class<I> incomingClass,
            ProcessTerminatorLambda<S, I> processor) {
        return terminator(true, endpointId, incomingClass, stateClass, NO_CONFIG, NO_CONFIG, processor);
    }

    @Override
    public <S, I> JmsMatsEndpoint<Void, S, Z> terminator(String endpointId,
            Class<S> stateClass, Class<I> incomingClass,
            Consumer<? super EndpointConfig<Void, S>> endpointConfigLambda,
            Consumer<? super StageConfig<Void, S, I>> stageConfigLambda,
            ProcessTerminatorLambda<S, I> processor) {
        return terminator(true, endpointId, incomingClass, stateClass, endpointConfigLambda, stageConfigLambda,
                processor);
    }

    @Override
    public <S, I> JmsMatsEndpoint<Void, S, Z> subscriptionTerminator(String endpointId, Class<S> stateClass,
            Class<I> incomingClass,
            ProcessTerminatorLambda<S, I> processor) {
        return terminator(false, endpointId, incomingClass, stateClass, NO_CONFIG, NO_CONFIG, processor);
    }

    @Override
    public <S, I> JmsMatsEndpoint<Void, S, Z> subscriptionTerminator(String endpointId, Class<S> stateClass,
            Class<I> incomingClass,
            Consumer<? super EndpointConfig<Void, S>> endpointConfigLambda,
            Consumer<? super StageConfig<Void, S, I>> stageConfigLambda,
            ProcessTerminatorLambda<S, I> processor) {
        return terminator(false, endpointId, incomingClass, stateClass, endpointConfigLambda, stageConfigLambda,
                processor);
    }

    /**
     * INTERNAL method, since terminator(...) and subscriptionTerminator(...) are near identical.
     */
    private <S, I> JmsMatsEndpoint<Void, S, Z> terminator(boolean queue, String endpointId,
            Class<I> incomingClass,
            Class<S> stateClass,
            Consumer<? super EndpointConfig<Void, S>> endpointConfigLambda,
            Consumer<? super StageConfig<Void, S, I>> stageConfigLambda,
            ProcessTerminatorLambda<S, I> processor) {
        // Need to create the JmsMatsEndpoint ourselves, since we need to set the queue-parameter.
        JmsMatsEndpoint<Void, S, Z> endpoint = new JmsMatsEndpoint<>(this, endpointId, queue, stateClass,
                Void.TYPE);
        addCreatedEndpoint(endpoint);
        endpointConfigLambda.accept(endpoint.getEndpointConfig());
        // :: Wrap the ProcessTerminatorLambda in a single stage that does not return.
        // This is just a direct forward, w/o any return value.
        endpoint.stage(incomingClass, stageConfigLambda, processor::process);
        endpoint.finishSetup();
        return endpoint;
    }

    @Override
    public MatsInitiator getDefaultInitiator() {
        final MatsInitiator initiator = getOrCreateInitiator("default");

        return new MatsInitiator() {
            @Override
            public String getName() {
                return "default";
            }

            @Override
            public void initiate(InitiateLambda lambda) throws MatsMessageSendException, MatsBackendException {
                Supplier<MatsInitiate> initiateSupplier = __stageDemarcatedMatsInitiate.get();
                // ?: Are we within a MatsStage?
                if (initiateSupplier != null) {
                    // -> Evidently within a MatsStage, so use the ThreadLocal MatsInitiate.
                    lambda.initiate(initiateSupplier.get());
                }
                else {
                    // -> No, not within a MatsStage, so use the proper MatsInitiate.
                    initiator.initiate(lambda);
                }
            }

            @Override
            public void initiateUnchecked(InitiateLambda lambda) throws MatsBackendRuntimeException,
                    MatsMessageSendRuntimeException {
                Supplier<MatsInitiate> initiateSupplier = __stageDemarcatedMatsInitiate.get();
                // ?: Are we within a MatsStage?
                if (initiateSupplier != null) {
                    // -> Evidently within a MatsStage, so use the ThreadLocal MatsInitiate.
                    lambda.initiate(initiateSupplier.get());
                }
                else {
                    // -> No, not within a MatsStage, so use the proper MatsInitiate.
                    initiator.initiateUnchecked(lambda);
                }
            }

            @Override
            public void close() {
                Supplier<MatsInitiate> initiateSupplier = __stageDemarcatedMatsInitiate.get();
                // ?: Are we within a MatsStage?
                if (initiateSupplier != null) {
                    // -> Evidently within a MatsStage, so point this out pretty harshly.
                    throw new IllegalStateException("This is the MatsFactory.getDefaultInitiator(), but it was gotten"
                            + " within a Mats Stage demarcation, and is thus a ThreadLocal Stage-specific wrapper"
                            + " around ProcessContext.initiate(..). It as such makes absolutely NO SENSE that you"
                            + " would want to close it: You've gotten the default MatsInitiator, you are within a"
                            + " Mats Stage context, and then you invoke .close() on it?!");
                }
                else {
                    // -> No, not within a MatsStage, so forward close call.
                    initiator.close();
                }
            }

            @Override
            public String toString() {
                return "[Mats-stage transactional demarcation wrapper of MatsFactory.getDefaultInitiator()@" + Integer
                        .toHexString(System.identityHashCode(this)) + ", wrapped MatsInitiator for use when not"
                        + " within MatsStage: " + initiator.toString() + "]";
            }
        };
    }

    @Override
    public MatsInitiator getOrCreateInitiator(String name) {
        synchronized (_createdInitiators) {
            for (MatsInitiator initiator : _createdInitiators) {
                if (initiator.getName().equals(name)) {
                    return initiator;
                }
            }
            JmsMatsInitiator<Z> initiator = new JmsMatsInitiator<>(name, this,
                    _jmsMatsJmsSessionHandler, _jmsMatsTransactionManager);
            addCreatedInitiator(initiator);
            return initiator;
        }
    }

    @Override
    public List<MatsEndpoint<?, ?>> getEndpoints() {
        synchronized (_createdEndpoints) {
            return new ArrayList<>(_createdEndpoints);
        }
    }

    private void addCreatedEndpoint(JmsMatsEndpoint<?, ?, Z> newEndpoint) {
        // :: Assert that it is possible to instantiate the State and Reply classes.
        assertOkToInstantiateClass(newEndpoint.getEndpointConfig().getStateClass(), "State 'STO' Class",
                "Endpoint " + newEndpoint.getEndpointId());
        assertOkToInstantiateClass(newEndpoint.getEndpointConfig().getReplyClass(), "Reply DTO Class",
                "Endpoint " + newEndpoint.getEndpointId());

        // :: Check that we do not have the endpoint already, and if not, register it.
        synchronized (_createdEndpoints) {
            Optional<MatsEndpoint<?, ?>> existingEndpoint = getEndpoint(newEndpoint.getEndpointConfig()
                    .getEndpointId());
            if (existingEndpoint.isPresent()) {
                throw new IllegalStateException("An Endpoint with endpointId='"
                        + newEndpoint.getEndpointConfig().getEndpointId()
                        + "' was already present. Existing: [" + existingEndpoint.get()
                        + "], attempted registered:[" + newEndpoint + "].");
            }
            _createdEndpoints.add(newEndpoint);
        }
    }

    void assertOkToInstantiateClass(Class<?> clazz, String what, String whatInstance) {
        // ?: Void is allowed to "instantiate" - as all places where this is attempted, 'null' will be used instead.
        if (clazz == Void.TYPE) {
            return;
        }
        if (clazz == Void.class) {
            return;
        }
        try {
            _matsSerializer.newInstance(clazz);
        }
        catch (Throwable t) {
            throw new CannotInstantiateClassException("Got problem when using current MatsSerializer to test"
                    + " instantiate [" + what + "] class [" + clazz + "] of [" + whatInstance + "]. MatsSerializer: ["
                    + _matsSerializer + "].", t);
        }
    }

    public static class CannotInstantiateClassException extends RuntimeException {
        public CannotInstantiateClassException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    @Override
    public Optional<MatsEndpoint<?, ?>> getEndpoint(String endpointId) {
        synchronized (_createdEndpoints) {
            for (MatsEndpoint<?, ?> endpoint : _createdEndpoints) {
                if (endpoint.getEndpointConfig().getEndpointId().equals(endpointId)) {
                    return Optional.of(endpoint);
                }
            }
            return Optional.empty();
        }
    }

    @Override
    public List<MatsInitiator> getInitiators() {
        synchronized (_createdInitiators) {
            return new ArrayList<>(_createdInitiators);
        }
    }

    private void addCreatedInitiator(JmsMatsInitiator<Z> initiator) {
        synchronized (_createdInitiators) {
            _createdInitiators.add(initiator);
        }
    }

    @Override
    public void start() {
        log.info(LOG_PREFIX + "Starting [" + idThis() + "], thus starting all created endpoints.");
        // First setting the "hold" to false, so if any subsequent endpoints are added, they will auto-start.
        _holdEndpointsUntilFactoryIsStarted = false;
        // :: Now start all the already configured endpoints
        for (MatsEndpoint<?, ?> endpoint : getEndpoints()) {
            try {
                endpoint.start();
            }
            catch (Throwable t) {
                log.warn("Got some throwable when starting endpoint [" + endpoint + "].", t);
            }
        }
    }

    @Override
    public void holdEndpointsUntilFactoryIsStarted() {
        log.info(LOG_PREFIX + getClass().getSimpleName() + ".holdEndpointsUntilFactoryIsStarted() invoked - will not"
                + " start any configured endpoints until .start() is explicitly invoked!");
        _holdEndpointsUntilFactoryIsStarted = true;
    }

    @Override
    public List<JmsMatsStartStoppable> getChildrenStartStoppable() {
        synchronized (_createdEndpoints) {
            return new ArrayList<>(_createdEndpoints);
        }
    }

    @Override
    public boolean waitForStarted(int timeoutMillis) {
        return JmsMatsStartStoppable.super.waitForStarted(timeoutMillis);
    }

    public boolean isHoldEndpointsUntilFactoryIsStarted() {
        return _holdEndpointsUntilFactoryIsStarted;
    }

    /**
     * Method for Spring's default lifecycle - directly invokes {@link #stop(int) stop(30_000)}.
     */
    public void close() {
        log.info(LOG_PREFIX + getClass().getSimpleName() + ".close() invoked"
                + " (probably via Spring's default lifecycle), forwarding to stop().");
        stop(30_000);
    }

    @Override
    public boolean stop(int gracefulShutdownMillis) {
        log.info(LOG_PREFIX + "Stopping [" + idThis()
                + "], thus stopping/closing all created endpoints and initiators.");
        boolean stopped = JmsMatsStartStoppable.super.stop(gracefulShutdownMillis);

        if (stopped) {
            log.info(LOG_PREFIX + "Everything of [" + idThis() + "} stopped nicely. Now cleaning JMS Session pool.");
        }
        else {
            log.warn(LOG_PREFIX + "Evidently some components of [" + idThis() + "} DIT NOT stop in ["
                    + gracefulShutdownMillis + "] millis, giving up. Now cleaning JMS Session pool.");
        }

        for (MatsInitiator initiator : getInitiators()) {
            initiator.close();
        }

        // :: "Closing the JMS pool"; closing all available SessionHolder, which should lead to the Connections closing.
        _jmsMatsJmsSessionHandler.closeAllAvailableSessions();
        return stopped;
    }

    @Override
    public String idThis() {
        String name = _factoryConfig.getName();
        return ("".equals(name) ? this.getClass().getSimpleName() : name)
                + '@' + Integer.toHexString(System.identityHashCode(this));
    }

    @Override
    public String toString() {
        return idThis();
    }

    private class JmsMatsFactoryConfig implements FactoryConfig {
        // Set to default, which is 0 (which means default logic; 2x numCpus)
        private int _concurrency = 0;

        // Set to default, which is empty string (not null).
        private String _name = "";

        // Set to default.
        private String _matsDestinationPrefix = "mats.";

        // Set to default.
        private String _matsTraceKey = "mats:trace";

        @Override
        public void setName(String name) {
            if (name == null) {
                throw new NullPointerException("name");
            }
            String idBefore = idThis();
            _name = name;
            log.info(LOG_PREFIX + "Set name to [" + name + "] for [" + idBefore + "], new id: [" + idThis() + "].");
        }

        @Override
        public String getName() {
            return _name;
        }

        @Override
        public int getNumberOfCpus() {
            return Integer.parseInt(System.getProperty("mats.cpus",
                    Integer.toString(Runtime.getRuntime().availableProcessors())));
        }

        @Override
        public FactoryConfig setMatsDestinationPrefix(String prefix) {
            log.info("MatsFactory's Mats Destination Prefix is set to [" + prefix + "] (was: [" + _matsDestinationPrefix
                    + "]).");
            _matsDestinationPrefix = prefix;
            return this;
        }

        @Override
        public String getMatsDestinationPrefix() {
            return _matsDestinationPrefix;
        }

        @Override
        public FactoryConfig setMatsTraceKey(String key) {
            log.info("MatsFactory's Mats Trace Key is set to [" + key + "] (was: [" + _matsTraceKey + "]).");
            _matsTraceKey = key;
            return this;
        }

        @Override
        public String getMatsTraceKey() {
            return _matsTraceKey;
        }

        @Override
        public FactoryConfig setConcurrency(int concurrency) {
            log.info("MatsFactory's Concurrency is set to [" + concurrency + "] (was: [" + _concurrency + "]).");
            _concurrency = concurrency;
            return this;
        }

        @Override
        public boolean isConcurrencyDefault() {
            return _concurrency == 0;
        }

        @Override
        public int getConcurrency() {
            if (_concurrency == 0) {
                return 2 * getNumberOfCpus();
            }
            return _concurrency;
        }

        @Override
        public boolean isRunning() {
            // :: Return true if /any/ endpoint is running.
            for (MatsEndpoint<?, ?> endpoint : getEndpoints()) {
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
        public String getNodename() {
            return _nodename;
        }

        @Override
        public FactoryConfig setNodename(String nodename) {
            _nodename = nodename;
            return this;
        }

        @Override
        public <T> T instantiateNewObject(Class<T> type) {
            try {
                return _matsSerializer.newInstance(type);
            }
            catch (Throwable t) {
                throw new CannotInstantiateClassException("Got problem when using current MatsSerializer to test"
                        + " instantiate class [" + type + "] MatsSerializer: ["
                        + _matsSerializer + "].", t);
            }
        }
    }
}
