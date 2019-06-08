package com.stolsvik.mats.spring.test;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.jms.ConnectionFactory;

import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.stolsvik.mats.spring.test.MatsSpringTestActiveMq.MatsSpringTestActivceMqConfiguration;
import com.stolsvik.mats.spring.test.MatsSpringTestActiveMq.TestBeanFactoryPostProcessor;
import com.stolsvik.mats.util_activemq.MatsTestActiveMq;

/**
 * Sets up an ActiveMQ {@link BrokerService} instance (unless certain system properites are set, read more at
 * {@link MatsTestActiveMq}), and then sets up a JMS ConnectionFactory connecting to this BrokerService.
 * <p>
 * If you are dead-set on getting access to the BrokerService (it is not made available in the Spring Context by default
 * since it might be null if those certain sysprops are set, and Spring dislikes beans that are null), you might inject
 * the class {@link MatsSpringTestActivceMqConfiguration}, and run
 * {@link MatsSpringTestActivceMqConfiguration#getBrokerService()} on that. Note: You probably don't need it.
 * 
 * @author Endre Stølsvik 2019-05-08 23:32 - http://stolsvik.com/, endre@stolsvik.com
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({ MatsSpringTestActivceMqConfiguration.class, TestBeanFactoryPostProcessor.class })
@Documented
public @interface MatsSpringTestActiveMq {
    /**
     * <code>@Configuration</code>-class which is employed when using the {@link MatsSpringTestActiveMq}-annotation.
     */
    @Configuration
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    class MatsSpringTestActivceMqConfiguration implements SmartLifecycle {
        private static final Logger log = LoggerFactory.getLogger(MatsSpringTestActivceMqConfiguration.class);

        private final MatsTestActiveMq _matsTestActiveMq;

        {
            log.info(LOG_PREFIX + "Creating MatsTestActiveMq instance.");
            _matsTestActiveMq = MatsTestActiveMq.createRandomTestActiveMq();
        }

        /**
         * <b>Notice that this method is NOT annotated with <code>@Bean</code>.</b> The rationale for this is twofold:
         * 1) I've never needed the broker in tests, and 2) if certain system properties are set, the
         * {@link MatsTestActiveMq} will not create a <code>BrokerService</code> instance (because with those system
         * properties, it is assumed that there is a BrokerService being started or is run by other means, typically
         * separate process outside of this JVM) - and Spring dislikes beans that are null.
         * 
         * @return the current {@link BrokerService}, if it was created (it might not have been created if certain
         *         system properties are set, read more at {@link MatsTestActiveMq}).
         */
        public BrokerService getBrokerService() {
            return _matsTestActiveMq.getBrokerService();
        }

        @Bean
        MatsTestActiveMq getMatsTestActiveMq() {
            return _matsTestActiveMq;
        }

        @Bean
        public ConnectionFactory getConnectionFactory() {
            return _matsTestActiveMq.getConnectionFactory();
        }

        private static final String LOG_PREFIX = "#SPRINGMATS_TESTMQ# ";

        private boolean _started;

        @Override
        public int getPhase() {
            log.info(LOG_PREFIX + "SmartLifeCycle.getPhase(), returning 'Integer.MIN_VALUE' to be"
                    + " stopped as late as possible.");
            return Integer.MIN_VALUE;
        }

        @Override
        public void start() {
            // Doing nothing, as the ActiveMQ BrokerService and ConnectionFactory was created at instantiation.
            _started = true;
        }

        @Override
        public boolean isRunning() {
            return _started;
        }

        @Override
        public void stop() {
            _matsTestActiveMq.close();
            _started = false;
        }

        @Override
        public boolean isAutoStartup() {
            return true;
        }

        @Override
        public void stop(Runnable callback) {
            log.info(LOG_PREFIX + "SmartLifeCycle.stop(callback): Stopping MatsTestActiveMq's BrokerService.");
            stop();
            callback.run();
        }
    }

    @Component
    class TestBeanFactoryPostProcessor implements BeanFactoryPostProcessor {
        private static final Logger log = LoggerFactory.getLogger(TestBeanFactoryPostProcessor.class);

        private ConfigurableListableBeanFactory _beanFactory;

        @Override
        public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
            ListBeanDefinitions.listAllBeansDefinitions(beanFactory);
            _beanFactory = beanFactory;
        }

        @EventListener
        public void onContextRefreshedEvent(ContextRefreshedEvent e) {
            log.info("XXXX ContextRefreshedEvent.");

            ApplicationContext applicationContext = e.getApplicationContext();

            log.info("BeanDefinitionCount: " + applicationContext.getBeanDefinitionCount());
            String[] beanDefinitionNames = applicationContext.getBeanDefinitionNames();
            for (String beanDefinitionName : beanDefinitionNames) {
                log.info("BeanDefinitionName: " + beanDefinitionName + ": Bean: [" + applicationContext.getBean(
                        beanDefinitionName) + "]");
            }
        }
    }
}
