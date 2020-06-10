package com.stolsvik.mats.spring;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import javax.inject.Inject;
import javax.jms.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.EndpointConfig;
import com.stolsvik.mats.MatsEndpoint.ProcessSingleLambda;
import com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsStage.StageConfig;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;
import com.stolsvik.mats.spring.matsfactoryqualifier.AbstractQualificationTest;
import com.stolsvik.mats.util_activemq.MatsLocalVmActiveMq;

/**
 * Base class for {@link VerifyShutdownOrderUsingFactoryBeanTest} - we do not use SpringRunner or other frameworks,
 * but instead do all Spring config ourselves. This so that the testing is as application-like as possible.
 * <p>
 * The the beans created in this configuration have been wrapped in a thin shell which replicates the calls straight
 * down to their respective counter parts. The reason for wrapping, is that we want access and the possibility to track
 * when the different beans are closed/stopped/destroyed.
 * <p>
 * Heavily inspired by {@link AbstractQualificationTest} created by Endre Stølsvik.
 */
public class AbstractFactoryBeanTestBase {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    protected AnnotationConfigApplicationContext _ctx;

    /**
     * "Singleton" registry which "tracks" which service have been stopped.
     */
    protected static StoppedRegistry _stoppedRegistry = new StoppedRegistry();

    protected void startSpring() {
        long nanosStart = System.nanoTime();
        log.info("Starting " + this.getClass().getSimpleName() + "!");
        log.info(" \\- new'ing up AnnotationConfigApplicationContext, giving class [" + this.getClass()
                .getSimpleName() + "] as base.");
        _ctx = new AnnotationConfigApplicationContext(DI_MainContext.class);
        double startTimeMs = (System.nanoTime() - nanosStart) / 1_000_000d;
        log.info(" \\- done, AnnotationConfigApplicationContext: [" + _ctx + "], took: [" + startTimeMs + " ms].");

        // Since this test is NOT run by the SpringRunner, the instance which this code is running in is not
        // managed by Spring. That is: JUnit have instantiated one (this), and Spring has instantiated another.
        // Therefore, manually autowire this which JUnit has instantiated.
        _ctx.getAutowireCapableBeanFactory().autowireBean(this);
    }

    protected void stopSpring() {
        // :: Close Spring.
        long nanosStart = System.nanoTime();
        log.info("Stop - closing Spring ApplicationContext.");
        _ctx.close();
        log.info("done. took: [" + ((System.nanoTime() - nanosStart) / 1_000_000d) + " ms].");
    }

    // =============================================== App configuration ==============================================

    /**
     * Context was extracted into a separate class as the approach utilized inside {@link AbstractQualificationTest}
     * did not detected the usage of {@link Import} as utilized on {@link DI_MainContext}.
     */
    @Configuration
    @Import(MatsFactoryBeanDefRegistrar.class)
    @EnableMats
    public static class DI_MainContext {
        @Bean
        protected MatsLocalVmActiveMqVerifiableStopWrapper activeMq1() {
            return new MatsLocalVmActiveMqVerifiableStopWrapper(MatsLocalVmActiveMq.createInVmActiveMq("activeMq1"),
                    _stoppedRegistry);
        }

        @Bean
        protected ConnectionFactory connectionFactory1(
                @Qualifier("activeMq1") MatsLocalVmActiveMqVerifiableStopWrapper activeMq1) {
            return activeMq1.getConnectionFactory();
        }
    }

    public static class MatsFactoryBeanDefRegistrar implements ImportBeanDefinitionRegistrar {
        @Override
        public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata,
                BeanDefinitionRegistry registry) {
            ConstructorArgumentValues constructorArgumentValues = new ConstructorArgumentValues();
            GenericBeanDefinition matsFactory = new GenericBeanDefinition();
            matsFactory.setPrimary(true);
            matsFactory.setBeanClass(MatsMServiceMatsFactory_FactoryBean.class);
            matsFactory.setConstructorArgumentValues(constructorArgumentValues);
            registry.registerBeanDefinition("matsFactory", matsFactory);
        }
    }

    /**
     *
     */
    public static class MatsMServiceMatsFactory_FactoryBean extends AbstractFactoryBean<MatsFactoryVerifiableStopWrapper> {

        @Inject
        private ConnectionFactory _connectionFactory;

        public MatsMServiceMatsFactory_FactoryBean() {
        }


        @Override
        public Class<?> getObjectType() {
            return MatsFactory.class;
        }

        @Override
        protected MatsFactoryVerifiableStopWrapper createInstance() throws Exception {
            return new MatsFactoryVerifiableStopWrapper(JmsMatsFactory.createMatsFactory_JmsOnlyTransactions("##TEST##",
                    "##VERSION##",
                    JmsMatsJmsSessionHandler_Pooling.create(_connectionFactory),
                    new MatsSerializer_DefaultJson()),
                    _stoppedRegistry);
        }
    }

    // ===============================================================================================================

    /**
     * Simple class which contains a {@link LinkedList}, since a LinkedList maintain the elements by insertion order
     * we can be sure that index 0 happened before index 1. This gives us a reason to be reasonable certain that
     * the elements contained within the list happen in this order.
     */
    public static class StoppedRegistry {

        private Map<String, Boolean> _stoppedServices = new LinkedHashMap<>();

        public StoppedRegistry() {
        }

        public void registerStopped(String clazzName, boolean stopped) {
            _stoppedServices.put(clazzName, stopped);
        }

        public Map<String, Boolean> getStoppedServices() {
            return _stoppedServices;
        }
    }

    // ===============================================================================================================

    /**
     * Wrapper replicating the behavior of {@link JmsMatsFactory} by passing all calls through to the internal
     * {@link JmsMatsFactory} given to the constructor. The key method is
     * {@link MatsFactoryVerifiableStopWrapper#stop(int)} which's stops the underlying factory and utilizes a finally
     * clause to register itself with the {@link StoppedRegistry} that this instance was indeed stopped.
     */
    public static class MatsFactoryVerifiableStopWrapper implements MatsFactory {

        private MatsFactory _matsFactory;
        private StoppedRegistry _stoppedRegistry;

        public MatsFactoryVerifiableStopWrapper(MatsFactory matsFactory,
                StoppedRegistry stoppedRegistry) {
            _matsFactory = matsFactory;
            _stoppedRegistry = stoppedRegistry;
        }

        @Override
        public FactoryConfig getFactoryConfig() {
            return _matsFactory.getFactoryConfig();
        }

        @Override
        public <R, S> MatsEndpoint<R, S> staged(String endpointId, Class<R> replyClass, Class<S> stateClass) {
            return _matsFactory.staged(endpointId, replyClass, stateClass);
        }

        @Override
        public <R, S> MatsEndpoint<R, S> staged(String endpointId, Class<R> replyClass, Class<S> stateClass,
                Consumer<? super EndpointConfig<R, S>> endpointConfigLambda) {
            return _matsFactory.staged(endpointId, replyClass, stateClass, endpointConfigLambda);
        }

        @Override
        public <R, I> MatsEndpoint<R, Void> single(String endpointId, Class<R> replyClass, Class<I> incomingClass,
                ProcessSingleLambda<R, I> processor) {
            return _matsFactory.single(endpointId, replyClass, incomingClass, processor);
        }

        @Override
        public <R, I> MatsEndpoint<R, Void> single(String endpointId, Class<R> replyClass, Class<I> incomingClass,
                Consumer<? super EndpointConfig<R, Void>> endpointConfigLambda,
                Consumer<? super StageConfig<R, Void, I>> stageConfigLambda, ProcessSingleLambda<R, I> processor) {
            return _matsFactory
                    .single(endpointId, replyClass, incomingClass, endpointConfigLambda, stageConfigLambda, processor);
        }

        @Override
        public <S, I> MatsEndpoint<Void, S> terminator(String endpointId, Class<S> stateClass, Class<I> incomingClass,
                ProcessTerminatorLambda<S, I> processor) {
            return _matsFactory.terminator(endpointId, stateClass, incomingClass, processor);
        }

        @Override
        public <S, I> MatsEndpoint<Void, S> terminator(String endpointId, Class<S> stateClass, Class<I> incomingClass,
                Consumer<? super EndpointConfig<Void, S>> endpointConfigLambda,
                Consumer<? super StageConfig<Void, S, I>> stageConfigLambda, ProcessTerminatorLambda<S, I> processor) {
            return _matsFactory
                    .terminator(endpointId, stateClass, incomingClass, endpointConfigLambda, stageConfigLambda,
                            processor);
        }

        @Override
        public <S, I> MatsEndpoint<Void, S> subscriptionTerminator(String endpointId, Class<S> stateClass,
                Class<I> incomingClass, ProcessTerminatorLambda<S, I> processor) {
            return _matsFactory.subscriptionTerminator(endpointId, stateClass, incomingClass, processor);
        }

        @Override
        public <S, I> MatsEndpoint<Void, S> subscriptionTerminator(String endpointId, Class<S> stateClass,
                Class<I> incomingClass, Consumer<? super EndpointConfig<Void, S>> endpointConfigLambda,
                Consumer<? super StageConfig<Void, S, I>> stageConfigLambda, ProcessTerminatorLambda<S, I> processor) {
            return _matsFactory.subscriptionTerminator(endpointId, stateClass, incomingClass, endpointConfigLambda,
                    stageConfigLambda, processor);
        }

        @Override
        public List<MatsEndpoint<?, ?>> getEndpoints() {
            return _matsFactory.getEndpoints();
        }

        @Override
        public Optional<MatsEndpoint<?, ?>> getEndpoint(String endpointId) {
            return _matsFactory.getEndpoint(endpointId);
        }

        @Override
        public MatsInitiator getDefaultInitiator() {
            return _matsFactory.getDefaultInitiator();
        }

        @Override
        public MatsInitiator getOrCreateInitiator(String name) {
            return _matsFactory.getOrCreateInitiator(name);
        }

        @Override
        public List<MatsInitiator> getInitiators() {
            return _matsFactory.getInitiators();
        }

        @Override
        public void start() {
            _matsFactory.start();
        }

        @Override
        public void holdEndpointsUntilFactoryIsStarted() {
            _matsFactory.holdEndpointsUntilFactoryIsStarted();
        }

        @Override
        public boolean waitForStarted(int timeoutMillis) {
            return _matsFactory.waitForStarted(timeoutMillis);
        }

        @Override
        public boolean stop(int gracefulShutdownMillis) {
            boolean returnBoolean = false;
            try {
                returnBoolean = _matsFactory.stop(gracefulShutdownMillis);
            }
            finally {
                _stoppedRegistry.registerStopped(getClass().getSimpleName(), returnBoolean);
            }
            return returnBoolean;
        }
    }

    /**
     * Wrapper containing a {@link MatsLocalVmActiveMq} which contains the {@link ConnectionFactory} utilized by the
     * created {@link MatsFactory}.
     */
    public static class MatsLocalVmActiveMqVerifiableStopWrapper {
        MatsLocalVmActiveMq _matsLocalVmActiveMq;
        private StoppedRegistry _stoppedRegistry;

        public MatsLocalVmActiveMqVerifiableStopWrapper(MatsLocalVmActiveMq matsLocalVmActiveMq,
                StoppedRegistry stoppedRegistry) {
            _matsLocalVmActiveMq = matsLocalVmActiveMq;
            _stoppedRegistry = stoppedRegistry;
        }

        /**
         * Called by spring life cycle.
         */
        public void close() {
            try {
                _matsLocalVmActiveMq.close();
            }
            finally {
                _stoppedRegistry.registerStopped(getClass().getSimpleName(), _matsLocalVmActiveMq.getBrokerService().isStopped());
            }
        }

        public ConnectionFactory getConnectionFactory() {
            return _matsLocalVmActiveMq.getConnectionFactory();
        }
    }

}
