package com.stolsvik.mats.spring.jms.factories;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.annotation.PostConstruct;
import javax.jms.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsFactory.MatsFactoryWrapper;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;

/**
 * Wrapper that should be used for a JmsMatsFactory in a Spring context. Takes JMS {@link ConnectionFactory} as that
 * will be used to make a "MatsTestMqInterface" in case the MatsFactory produced can be used in a test scenario (which
 * it might can in a setup employing the {@link ScenarioConnectionFactoryProducer}).
 * 
 * @author Endre Stølsvik 2020-11-28 01:28 - http://stolsvik.com/, endre@stolsvik.com
 */
public class SpringJmsMatsFactoryWrapper extends MatsFactoryWrapper {

    public static final String MATS_TEST_MQ_INTERFACE_CLASSNAME = "com.stolsvik.mats.test.MatsTestMqInterface";
    public static final String LATE_POPULATE_METHOD_NAME = "_latePopulate";

    private static final Logger log = LoggerFactory.getLogger(SpringJmsMatsFactoryWrapper.class);
    private static final String LOG_PREFIX = "#SPRINGJMATS# ";

    private final ConnectionFactory _connectionFactory;
    private final MatsFactory _matsFactory;

    private Class<?> _dlqFetcherClass;

    @Autowired
    private Environment _environment;

    @Autowired
    private ApplicationContext _applicationContext;

    {
        // :: Using reflection here to see if we have the MatsTestMqInterface (from mats-test) on classpath.
        try {
            _dlqFetcherClass = Class.forName(MATS_TEST_MQ_INTERFACE_CLASSNAME);
        }
        catch (ClassNotFoundException e) {
            // Handled in the @PostConstruct codepath.
        }
    }

    /**
     * <b></b>Note: The MatsFactory provided may be a {@link MatsFactoryWrapper}, but it must resolve to a
     * {@link JmsMatsFactory} via the {@link MatsFactory#unwrapFully()}.</b>
     */
    public SpringJmsMatsFactoryWrapper(ConnectionFactory connectionFactory, MatsFactory matsFactory) {
        super(matsFactory);
        _connectionFactory = connectionFactory;
        _matsFactory = matsFactory;
    }

    @PostConstruct
    void postConstruct() {
        boolean matsTestProfileActive = _environment.acceptsProfiles(MatsProfiles.PROFILE_MATS_TEST);
        handleMatsTestMqInterfacePopulation(matsTestProfileActive);
        handleMatsFactoryConcurrencyForTestAndDevelopment(matsTestProfileActive);
    }

    void handleMatsTestMqInterfacePopulation(boolean matsTestProfileActive) {
        AutowireCapableBeanFactory autowireCapableBeanFactory = _applicationContext.getAutowireCapableBeanFactory();
        log.info(LOG_PREFIX + SpringJmsMatsFactoryWrapper.class.getSimpleName() + " got @PostConstructed.");

        if (_dlqFetcherClass == null) {
            if (matsTestProfileActive) {
                log.warn(LOG_PREFIX + " \\- Class '" + MATS_TEST_MQ_INTERFACE_CLASSNAME
                        + "' not found on classpath. If you need this tool, you would want to have it on classpath,"
                        + " and have your testing Spring context to contain an \"empty\" such bean so that I could"
                        + " populate it for you with the JMS ConnectionFactory and necessary properties.");
            }
            else {
                log.info(LOG_PREFIX + " \\- Class '" + MATS_TEST_MQ_INTERFACE_CLASSNAME
                        + "' not found on classpath, probably not in a testing scenario.");
            }
            return;
        }

        Object dlqFetcherFromSpringContext;
        try {
            dlqFetcherFromSpringContext = autowireCapableBeanFactory.getBean(_dlqFetcherClass);
        }
        catch (NoSuchBeanDefinitionException e) {
            String msg = " \\- Testing tool '" + MATS_TEST_MQ_INTERFACE_CLASSNAME
                    + "' found on classpath, but not in Spring context. If you need this tool,"
                    + " you would want your testing Spring context to contain an \"empty\" such"
                    + " bean so that I could populate it for you with the JMS ConnectionFactory and"
                    + " necessary properties. The @MatsTestContext already does this.";
            if (matsTestProfileActive) {
                log.warn(LOG_PREFIX + msg);
            }
            else {
                log.info(LOG_PREFIX + msg);
            }
            return;
        }
        log.info(LOG_PREFIX + "Found '" + MATS_TEST_MQ_INTERFACE_CLASSNAME + "' in Spring Context: "
                + dlqFetcherFromSpringContext);

        // :: Now create the MatsTestMqInterface and put in ContextLocal
        try {
            Method factoryMethod = _dlqFetcherClass.getMethod(LATE_POPULATE_METHOD_NAME, ConnectionFactory.class,
                    MatsFactory.class);
            factoryMethod.invoke(dlqFetcherFromSpringContext, _connectionFactory, _matsFactory);
            if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "Invoked the " + LATE_POPULATE_METHOD_NAME + " on '"
                    + MATS_TEST_MQ_INTERFACE_CLASSNAME + " to make the tool ready.");
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError("Didn't find method '" + LATE_POPULATE_METHOD_NAME + "(..)' on Class '"
                    + MATS_TEST_MQ_INTERFACE_CLASSNAME + "!", e);
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new AssertionError("Couldn't invoke method '" + LATE_POPULATE_METHOD_NAME + "(..)' on Class '"
                    + MATS_TEST_MQ_INTERFACE_CLASSNAME + "!", e);
        }
    }

    private void handleMatsFactoryConcurrencyForTestAndDevelopment(boolean matsTestPofileActive) {

        // ?: Is the Concurrency already set to something specific?

        // ?: Are we in explicit testing profile? (i.e. Spring Profile 'mats-test' is active)
        if (matsTestPofileActive) {
            // -> Yes, mats-test profile active, so set concurrency low.
            // ?: Are we in default concurrency?
            if (_matsFactory.getFactoryConfig().isConcurrencyDefault()) {
                // -> Yes, default concurrency - so set it to 1 since testing.
                log.info(LOG_PREFIX + "We're in Spring Profile '" + MatsProfiles.PROFILE_MATS_TEST
                        + "', so set concurrency to 1.");
                _matsFactory.getFactoryConfig().setConcurrency(1);
            }
            else {
                // -> No, concurrency evidently already set to something non-default, so will not override this.
                log.info(LOG_PREFIX + "We're in Spring Profile '" + MatsProfiles.PROFILE_MATS_TEST
                        + "', but the concurrency of MatsFactory is already set to something non-default ("
                        + _matsFactory.getFactoryConfig().getConcurrency() + "), so will not mess with that"
                        + " (would have set to 1).");
            }
        }
        else {
            // -> No, not testing, but might still be LOCALVM (probably development mode, or non-explicit testing)
            // ?: Is the provided JMS ConnectionFactory a ConnectionFactoryScenarioWrapper?
            if (_connectionFactory instanceof ScenarioConnectionFactoryWrapper) {
                // -> Yes it is, check if we're in LOCALVM mode
                ScenarioConnectionFactoryWrapper scenarioWrapped = (ScenarioConnectionFactoryWrapper) _connectionFactory;
                // ?: Are we in MatsScenario.LOCALVM?
                MatsScenario matsScenario = scenarioWrapped.getMatsScenarioUsedToMakeConnectionFactory();
                if (matsScenario == MatsScenario.LOCALVM) {
                    // -> Yes, so assume development - set concurrency low.
                    // ?: Are we in default concurrency?
                    if (_matsFactory.getFactoryConfig().isConcurrencyDefault()) {
                        // -> Yes, default concurrency - so set it to 1 since testing.
                        log.info(LOG_PREFIX + "The supplied ConnectionFactory was created with MatsScenario.LOCALVM,"
                                + " so we assume this is a development situation (or testing where the user forgot to"
                                + " add the Spring active profile 'mats-test' as with @MatsTestProfile), and set the"
                                + " concurrency to a low 2.");
                        _matsFactory.getFactoryConfig().setConcurrency(2);
                    }
                    else {
                        // -> No, concurrency evidently already set to something non-default, so will not override this.
                        log.info(LOG_PREFIX + "The supplied ConnectionFactory was created with MatsScenario.LOCALVM,"
                                + " so we assume this is a development situation (or testing where the user forgot to"
                                + " add the Spring active profile 'mats-test' as with @MatsTestProfile),"
                                + " HOWEVER, the concurrency is already set to something non-default ("
                                + _matsFactory.getFactoryConfig().getConcurrency() + "), so will not mess with that"
                                + " (would have set it to 2).");
                    }
                }
            }
        }
    }
}
