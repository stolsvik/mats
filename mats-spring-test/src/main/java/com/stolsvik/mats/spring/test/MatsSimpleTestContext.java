package com.stolsvik.mats.spring.test;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotatedBeanDefinitionReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.spring.EnableMats;
import com.stolsvik.mats.spring.test.MatsSimpleTestContext.MatsSimpleTestInfrastructureContextInitializer;
import com.stolsvik.mats.spring.test.MatsSimpleTestContext.MatsTestInfrastructureConfiguration;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.Rule_Mats;

/**
 * One-stop-shop for making <i>simple</i> Spring-based integration/unit tests of MATS endpoints (NOT utilizing SQL
 * Connections), reusing the functionality from {@link Rule_Mats}. The configuration is done in
 * {@link MatsTestInfrastructureConfiguration}. This annotation can be put on a test-class, or on a
 * {@literal @Configuration} class (typically a nested static class within the test-class).
 * <p>
 * <i>Observe: This annotation is most probably too simple for an actual integration testing setup.</i>
 * <p>
 * The annotation sets up the following:
 * <ul>
 * <li>Meta-annotated with {@link DirtiesContext}, since the Spring Test Context caching system is problematic when one
 * is handling static/global resources like what an external Message Queue broker is. Read more below.</li>
 * <li>The Configuration is Meta-annotated with {@link EnableMats @EnableMats}, since you pretty obviously need that,
 * and have better things to do than to write it in each test</li>
 * <li>Starts a {@link Rule_Mats} instance (externally from the JUnit machinery - we just uses the same features)</li>
 * <li>... and sticks the rule's {@link MatsFactory} instance into the Spring Context so that the {@literal @EnableMats}
 * annotation is happy, and also so that it is present for AutoWired injection in the tests.</li>
 * <li>.. and sticks the rule's {@link MatsInitiator} instance in the Spring Context, since it is pretty much certain
 * you'll need to initiate a message in your MATS tests.</li>
 * <li>Adds a default {@link MatsTestLatch} instance in the Spring Context, since when you need it, you need it both in
 * the {@literal @Configuration} class, and in the test-class - nice to already have an instance defined.</li>
 * </ul>
 * <p>
 * For some more background on the Context Caching: <a href=
 * "http://docs.spring.io/spring/docs/current/spring-framework-reference/html/integration-testing.html#testcontext-ctx-management-caching">
 * Read the Spring doc about Context Caching</a> and <a href="https://jira.spring.io/browse/SPR-7377">read a "wont-fix"
 * bug report describing how the contexts aren't destroyed due to caching</a>. Since we're setting up message queue
 * consumers on a (potentially) global Message Queue broker, the different contexts would mess each other up, the old
 * consumers still consuming messages that the new test setup expected its consumers to take. For the in-VM case, a
 * solution would be to set up a new message broker (with a new name) for each context (each instantiation of
 * {@link Rule_Mats}), but this would not solve the situation where we ran the tests against an actual external MQ
 * (which Rule_mats supports, read its JavaDoc!). Had there been some "deactivate"/"reactivate" events that could be
 * picked, we could have stopped the endpoints in one context (put in the cache) when firing up a new context, and the
 * restarted them when the context was reused from the cache. Since such a solution does not seem to be viable, the only
 * solution seems to be dirtying of the context, this stopping it from being cached.
 *
 * @author Endre Stølsvik - 2016-06-23 / 2016-08-07 - http://endre.stolsvik.com
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
// @ContextConfiguration makes it possible to annotate the test class itself
@ContextConfiguration(initializers = MatsSimpleTestInfrastructureContextInitializer.class)
// @Import makes it possible to annotate a @Configuration class
@Import(MatsTestInfrastructureConfiguration.class)
// @DirtiesContext since most tests needs this.
@DirtiesContext
// @Documented is only for JavaDoc: The documentation will show that the class is annotated with this annotation.
@Documented
public @interface MatsSimpleTestContext {
    /**
     * Spring {@link Configuration @Configuration} class that cooks up the simple test infrastructure, reusing the
     * functionality from {@link Rule_Mats}.
     */
    @Configuration
    @EnableMats
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    class MatsTestInfrastructureConfiguration {

        @Bean(destroyMethod = "after")
        protected Rule_Mats rule_Mats() throws Throwable {
            Rule_Mats rule_Mats = new Rule_Mats();
            rule_Mats.before();
            return rule_Mats;
        }

        @Bean
        protected MatsFactory testMatsFactory(Rule_Mats rule_Mats) {
            return rule_Mats.getMatsFactory();
        }

        @Bean
        protected MatsInitiator testMatsInitiator(Rule_Mats rule_Mats) {
            return rule_Mats.getMatsInitiator();
        }

        @Bean
        protected MatsTestLatch testMatsTestLatch() {
            return new MatsTestLatch();
        }
    }

    /**
     * The reason for this obscure way to add the {@link MatsTestInfrastructureConfiguration} (as opposed to just point
     * to it with "classes=..") is as follows: Spring has this feature where any static inner @Configuration class of
     * the test class is automatically loaded. If we specify specify classes= or location=, this default will be
     * thwarted.
     * 
     * @see <a href=
     *      "https://docs.spring.io/spring/docs/current/spring-framework-reference/testing.html#testcontext-ctx-management-initializers">
     *      Test doc about ContextConfiguration</a>.
     */
    class MatsSimpleTestInfrastructureContextInitializer implements
            ApplicationContextInitializer<ConfigurableApplicationContext> {
        // Use clogging, since that's what Spring does.
        private static final Log log = LogFactory.getLog(MatsSimpleTestInfrastructureContextInitializer.class);
        private static final String LOG_PREFIX = "#SPRINGMATS# ";

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            log.debug(LOG_PREFIX + "Registering " + MatsTestInfrastructureConfiguration.class.getSimpleName()
                    + " on: " + applicationContext);

            /*
             * Hopefully all the ConfigurableApplicationContexts presented here will also be a BeanDefinitionRegistry.
             * This at least holds for the default 'GenericApplicationContext'.
             */
            new AnnotatedBeanDefinitionReader((BeanDefinitionRegistry) applicationContext.getBeanFactory())
                    .register(MatsTestInfrastructureConfiguration.class);
        }
    }

}
