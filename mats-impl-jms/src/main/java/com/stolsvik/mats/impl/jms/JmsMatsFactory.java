package com.stolsvik.mats.impl.jms;

import java.io.BufferedInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.EndpointConfig;
import com.stolsvik.mats.MatsEndpoint.ProcessSingleLambda;
import com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;
import com.stolsvik.mats.MatsStage.StageConfig;
import com.stolsvik.mats.api.intercept.MatsInitiateInterceptor;
import com.stolsvik.mats.api.intercept.MatsInitiateInterceptor.InitiateInterceptContext;
import com.stolsvik.mats.api.intercept.MatsInterceptable;
import com.stolsvik.mats.api.intercept.MatsInterceptableMatsFactory;
import com.stolsvik.mats.api.intercept.MatsLoggingInterceptor;
import com.stolsvik.mats.api.intercept.MatsMetricsInterceptor;
import com.stolsvik.mats.api.intercept.MatsStageInterceptor;
import com.stolsvik.mats.api.intercept.MatsStageInterceptor.StageInterceptContext;
import com.stolsvik.mats.impl.jms.JmsMatsInitiator.MatsInitiator_TxRequired;
import com.stolsvik.mats.impl.jms.JmsMatsInitiator.MatsInitiator_TxRequiresNew;
import com.stolsvik.mats.serial.MatsSerializer;

public class JmsMatsFactory<Z> implements MatsInterceptableMatsFactory, JmsMatsStatics, JmsMatsStartStoppable {

    public static final String INTERCEPTOR_CLASS_MATS_LOGGING = "com.stolsvik.mats.intercept.logging.MatsMetricsLoggingInterceptor";
    public static final String INTERCEPTOR_CLASS_MATS_METRICS = "com.stolsvik.mats.intercept.micrometer.MatsMicrometerInterceptor";

    private static final Logger log = LoggerFactory.getLogger(JmsMatsFactory.class);

    public static <Z> JmsMatsFactory<Z> createMatsFactory_JmsOnlyTransactions(String appName, String appVersion,
            JmsMatsJmsSessionHandler jmsMatsJmsSessionHandler,
            MatsSerializer<Z> matsSerializer) {
        return createMatsFactory(appName, appVersion, jmsMatsJmsSessionHandler,
                JmsMatsTransactionManager_Jms.create(), matsSerializer);
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

    private static final Class<?> _matsLoggingInterceptor;
    private static final Class<?> _matsMetricsInterceptor;

    static {
        // Initialize any Specifics for the MessageBrokers that are on the classpath (i.e. if ActiveMQ is there..)
        JmsMatsMessageBrokerSpecifics.init();
        // Load the MatsMetricsLoggingInterceptor class if we have it on classpath
        Class<?> matsLoggingInterceptor = null;
        try {
            matsLoggingInterceptor = Class.forName(INTERCEPTOR_CLASS_MATS_LOGGING);
        }
        catch (ClassNotFoundException e) {
            log.info(LOG_PREFIX + "Did NOT find '" + INTERCEPTOR_CLASS_MATS_LOGGING + "' on classpath, won't install.");
        }
        _matsLoggingInterceptor = matsLoggingInterceptor;
        Class<?> matsMetricsInterceptor = null;
        try {
            matsMetricsInterceptor = Class.forName(INTERCEPTOR_CLASS_MATS_METRICS);
        }
        catch (ClassNotFoundException e) {
            log.info(LOG_PREFIX + "Did NOT find '" + INTERCEPTOR_CLASS_MATS_METRICS + "' on classpath, won't install.");
        }
        _matsMetricsInterceptor = matsMetricsInterceptor;
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
        try {
            Field callback = ContextLocal.class.getDeclaredField("callback");
            callback.setAccessible(true);
            synchronized (ContextLocal.class) {
                Object o = callback.get(null);
                if (o == null) {
                    callback.set(null, new JmsMatsContextLocalCallback());
                }
            }
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new AssertionError("Could not access the MatsFactory.ContextLocal.callback field.", e);
        }

        _appName = appName;
        _appVersion = appVersion;
        _jmsMatsJmsSessionHandler = jmsMatsJmsSessionHandler;
        _jmsMatsTransactionManager = jmsMatsTransactionManager;
        _matsSerializer = matsSerializer;
        _factoryConfig = new JmsMatsFactoryConfig();

        installIfPresent(_matsLoggingInterceptor);
        installIfPresent(_matsMetricsInterceptor);

        log.info(LOG_PREFIX + "Created [" + idThis() + "].");
    }

    private void installIfPresent(Class<?> standardInterceptorClass) {
        if (standardInterceptorClass != null) {
            log.info(LOG_PREFIX + "Found '" + standardInterceptorClass.getSimpleName()
                    + "' on classpath, installing for [" + idThis() + "].");
            try {
                Method install = standardInterceptorClass.getDeclaredMethod("install", MatsInterceptable.class);
                install.invoke(null, this);
            }
            catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                log.warn(LOG_PREFIX + "Got problems installing '" + standardInterceptorClass.getSimpleName() + "'.", e);
            }
        }
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

    // :: Interceptors

    private final CopyOnWriteArrayList<MatsInitiateInterceptorProvider> _initiationInterceptorProviders = new CopyOnWriteArrayList<>();
    private final IdentityHashMap<MatsInitiateInterceptor, MatsInitiateInterceptorProvider> _initiateInterceptorSingletonToProvider = new IdentityHashMap<>();

    private final CopyOnWriteArrayList<MatsStageInterceptorProvider> _stageInterceptorProviders = new CopyOnWriteArrayList<>();
    private final IdentityHashMap<MatsStageInterceptor, MatsStageInterceptorProvider> _stageInterceptorSingletonToProvider = new IdentityHashMap<>();

    @Override
    public void addInitiationInterceptorProvider(MatsInitiateInterceptorProvider initiationInterceptorProvider) {
        log.info(LOG_PREFIX + "Adding Provider " + MatsInitiateInterceptor.class.getSimpleName()
                + ": [" + initiationInterceptorProvider + "].");
        _initiationInterceptorProviders.add(initiationInterceptorProvider);
    }

    @Override
    public void removeInitiationInterceptorProvider(MatsInitiateInterceptorProvider initiationInterceptorProvider) {
        log.info(LOG_PREFIX + "Removing Provider " + MatsInitiateInterceptor.class.getSimpleName()
                + ": [" + initiationInterceptorProvider + "].");
        _initiationInterceptorProviders.remove(initiationInterceptorProvider);
    }

    @Override
    public void addInitiationInterceptorSingleton(MatsInitiateInterceptor initiateInterceptorSingleton) {
        MatsInitiateInterceptorProvider provider = na -> initiateInterceptorSingleton;
        addSingletonInterceptor(MatsInitiateInterceptor.class, initiateInterceptorSingleton, provider,
                _initiationInterceptorProviders, _initiateInterceptorSingletonToProvider);
    }

    @Override
    public <T extends MatsInitiateInterceptor> Optional<T> getInitiationInterceptorSingleton(
            Class<T> interceptorClass) {
        return getInterceptorSingleton(_initiateInterceptorSingletonToProvider.keySet(), interceptorClass);
    }

    @Override
    public void removeInitiationInterceptorSingleton(MatsInitiateInterceptor initiateInterceptorSingleton) {
        log.info(LOG_PREFIX + "Removing Singleton " + MatsInitiateInterceptor.class.getSimpleName()
                + ": [" + initiateInterceptorSingleton + "].");
        MatsInitiateInterceptorProvider provider;
        synchronized (_initiateInterceptorSingletonToProvider) {
            provider = _initiateInterceptorSingletonToProvider.remove(initiateInterceptorSingleton);
            if (provider == null) {
                throw new IllegalStateException("Cannot remove because not added: ["
                        + initiateInterceptorSingleton + "]");
            }
        }
        removeInitiationInterceptorProvider(provider);
    }

    @Override
    public void addStageInterceptorProvider(MatsStageInterceptorProvider stageInterceptorProvider) {
        log.info(LOG_PREFIX + "Adding Provider " + MatsStageInterceptorProvider.class.getSimpleName()
                + ": [" + stageInterceptorProvider + "].");
        _stageInterceptorProviders.add(stageInterceptorProvider);
    }

    @Override
    public void removeStageInterceptorProvider(MatsStageInterceptorProvider stageInterceptorProvider) {
        log.info(LOG_PREFIX + "Removing Provider " + MatsStageInterceptorProvider.class.getSimpleName()
                + ": [" + stageInterceptorProvider + "].");
        _stageInterceptorProviders.remove(stageInterceptorProvider);
    }

    @Override
    public void addStageInterceptorSingleton(MatsStageInterceptor stageInterceptor) {
        MatsStageInterceptorProvider provider = na -> stageInterceptor;
        addSingletonInterceptor(MatsStageInterceptor.class, stageInterceptor, provider,
                _stageInterceptorProviders, _stageInterceptorSingletonToProvider);
    }

    @Override
    public <T extends MatsStageInterceptor> Optional<T> getStageInterceptorSingleton(Class<T> interceptorClass) {
        return getInterceptorSingleton(_stageInterceptorSingletonToProvider.keySet(), interceptorClass);
    }

    private <I, T extends I> Optional<T> getInterceptorSingleton(Set<I> singletons, Class<T> typeToFind) {
        for (I singleton : singletons) {
            if (typeToFind.isInstance(singleton)) {
                @SuppressWarnings("unchecked")
                Optional<T> ret = (Optional<T>) Optional.of(singleton);
                return ret;
            }
        }
        return Optional.empty();
    }

    private <I, IP> void addSingletonInterceptor(Class<I> interceptorType,
            I initiateInterceptorSingleton, IP provider, CopyOnWriteArrayList<IP> providersList,
            IdentityHashMap<I, IP> singletonToProviderMap) {
        log.info(LOG_PREFIX + "Adding Singleton " + interceptorType.getSimpleName()
                + ": [" + initiateInterceptorSingleton + "].");
        synchronized (singletonToProviderMap) {
            // :: Special handling for our special interceptors
            // ?: Is this a MatsLoggingInterceptor?
            if (initiateInterceptorSingleton instanceof MatsLoggingInterceptor) {
                // -> Yes, MatsLoggingInterceptor - so clear out any existing to add this new.
                removeInterceptorSingletonType(providersList, singletonToProviderMap,
                        MatsLoggingInterceptor.class);
            }
            if (initiateInterceptorSingleton instanceof MatsMetricsInterceptor) {
                // -> Yes, MatsMetricsInterceptor - so clear out any existing to add this new.
                removeInterceptorSingletonType(providersList, singletonToProviderMap,
                        MatsMetricsInterceptor.class);
            }

            // ----- Not our special handling interceptors

            // :: Handle double-adding of same instance (not allowed)
            if (!((initiateInterceptorSingleton instanceof MatsLoggingInterceptor))
                    || (initiateInterceptorSingleton instanceof MatsMetricsInterceptor)) {
                // ?: Have we already added this same instance?
                if (singletonToProviderMap.containsKey(initiateInterceptorSingleton)) {
                    // -> Yes, already added, cannot add twice.
                    throw new IllegalStateException(interceptorType.getSimpleName()
                            + ": Interceptor Singleton already added: " + initiateInterceptorSingleton + ".");
                }
            }

            // Add the new
            providersList.add(provider);
            singletonToProviderMap.put(initiateInterceptorSingleton, provider);
        }
    }

    private <I, IP> void removeInterceptorSingletonType(CopyOnWriteArrayList<IP> providersList,
            IdentityHashMap<I, IP> singletonToProviderMap, Class<?> typeToRemove) {
        Iterator<Entry<I, IP>> it = singletonToProviderMap.entrySet().iterator();
        while (it.hasNext()) {
            Entry<I, IP> next = it.next();
            // ?: Is this existing interceptor of a type to remove?
            if (typeToRemove.isInstance(next.getKey())) {
                // -> Yes, so remove it, and from the providers.
                log.info(LOG_PREFIX + ".. removing existing: [" + next.getKey() + "], since adding new "
                        + typeToRemove.getSimpleName());
                providersList.remove(next.getValue());
                it.remove();
            }
        }
    }

    @Override
    public void removeStageInterceptorSingleton(MatsStageInterceptor stageInterceptorSingleton) {
        log.info(LOG_PREFIX + "Removing Singleton " + MatsStageInterceptor.class.getSimpleName()
                + ": [" + stageInterceptorSingleton + "].");
        MatsStageInterceptorProvider provider;
        synchronized (_stageInterceptorSingletonToProvider) {
            provider = _stageInterceptorSingletonToProvider.remove(stageInterceptorSingleton);
            if (provider == null) {
                throw new IllegalStateException("Cannot remove because not added: [" + stageInterceptorSingleton + "]");
            }
        }
        removeStageInterceptorProvider(provider);
    }

    List<MatsInitiateInterceptor> getInterceptorsForInitiation(InitiateInterceptContext context) {
        return _initiationInterceptorProviders.stream()
                .map(provider -> provider.provide(context))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    List<MatsStageInterceptor> getInterceptorsForStage(StageInterceptContext context) {
        return _stageInterceptorProviders.stream()
                .map(provider -> provider.provide(context))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
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
        validateNewEndpoint(endpoint);
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
        validateNewEndpoint(endpoint);
        endpointConfigLambda.accept(endpoint.getEndpointConfig());
        // :: Wrap the ProcessTerminatorLambda in a single stage that does not return.
        // This is just a direct forward, w/o any return value.
        endpoint.stage(incomingClass, stageConfigLambda, processor::process);
        endpoint.finishSetup();
        return endpoint;
    }

    ThreadLocal<Supplier<MatsInitiate>> __nestedMatsInitiate_elg = new ThreadLocal<>();

    /**
     * Note: This ThreadLocal is on the MatsFactory, thus the {@link MatsInitiate} is scoped to the MatsFactory. This
     * should make some sense: If you in a Stage do a new Initiation, even with the
     * {@link MatsFactory#getDefaultInitiator()}, if this is <i>on a different MatsFactory</i>, then the transaction
     * should not be hoisted.
     */
    void setCurrentMatsFactoryThreadLocalMatsDemarcation(Supplier<MatsInitiate> matsInitiateSupplier) {
        __nestedMatsInitiate_elg.set(matsInitiateSupplier);
    }

    Optional<Supplier<MatsInitiate>> getCurrentMatsFactoryThreadLocalMatsDemarcation() {
        return Optional.ofNullable(__nestedMatsInitiate_elg.get());
    }

    void clearCurrentMatsFactoryThreadLocalMatsDemarcation() {
        __nestedMatsInitiate_elg.remove();
    }

    private volatile JmsMatsInitiator<Z> _defaultMatsInitiator;

    @Override
    public MatsInitiator getDefaultInitiator() {
        if (_defaultMatsInitiator == null) {
            synchronized (_createdInitiators) {
                if (_defaultMatsInitiator == null) {
                    _defaultMatsInitiator = getOrCreateInitiator_internal("default");
                }
            }
        }
        return new MatsInitiator_TxRequired<Z>(this, _defaultMatsInitiator);
    }

    @Override
    public MatsInitiator getOrCreateInitiator(String name) {
        return new MatsInitiator_TxRequiresNew<Z>(this, getOrCreateInitiator_internal(name));
    }

    public JmsMatsInitiator<Z> getOrCreateInitiator_internal(String name) {
        synchronized (_createdInitiators) {
            for (JmsMatsInitiator<Z> init : _createdInitiators) {
                if (init.getName().equals(name)) {
                    return init;
                }
            }
            // E-> Not found, make new
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

    private void validateNewEndpoint(JmsMatsEndpoint<?, ?, Z> newEndpoint) {
        // :: Assert that it is possible to instantiate the State and Reply classes.
        assertOkToInstantiateClass(newEndpoint.getEndpointConfig().getStateClass(), "State 'STO' Class",
                "Endpoint " + newEndpoint.getEndpointId());
        assertOkToInstantiateClass(newEndpoint.getEndpointConfig().getReplyClass(), "Reply DTO Class",
                "Endpoint " + newEndpoint.getEndpointId());

        // :: Check that we do not have the endpoint already.
        synchronized (_createdEndpoints) {
            Optional<MatsEndpoint<?, ?>> existingEndpoint = getEndpoint(newEndpoint.getEndpointConfig()
                    .getEndpointId());
            if (existingEndpoint.isPresent()) {
                throw new IllegalStateException("An Endpoint with endpointId='"
                        + newEndpoint.getEndpointConfig().getEndpointId()
                        + "' was already present. Existing: [" + existingEndpoint.get()
                        + "], attempted registered:[" + newEndpoint + "].");
            }
        }
    }

    /**
     * Invoked by the endpoint upon {@link MatsEndpoint#finishSetup()}.
     */
    void addNewEndpointToFactory(JmsMatsEndpoint<?, ?, Z> newEndpoint) {
        // :: Check that we do not have the endpoint already - and if not, add it.
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

    void removeEndpoint(JmsMatsEndpoint<?, ?, Z> endpointToRemove) {
        synchronized (_createdEndpoints) {
            Optional<MatsEndpoint<?, ?>> existingEndpoint = getEndpoint(endpointToRemove.getEndpointConfig()
                    .getEndpointId());
            if (!existingEndpoint.isPresent()) {
                throw new IllegalStateException("When trying to remove the endpoint [" + endpointToRemove + "], it was"
                        + " not present in the MatsFactory! EndpointId:[" + endpointToRemove.getEndpointId() + "]");
            }
            _createdEndpoints.remove(endpointToRemove);
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
    public boolean waitForReceiving(int timeoutMillis) {
        return JmsMatsStartStoppable.super.waitForReceiving(timeoutMillis);
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
            log.info(LOG_PREFIX + "MatsFactory's Concurrency is set to [" + concurrency
                    + "] (was: [" + _concurrency + "]).");
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
