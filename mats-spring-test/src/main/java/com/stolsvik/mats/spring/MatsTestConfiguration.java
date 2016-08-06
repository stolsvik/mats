package com.stolsvik.mats.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PreDestroy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.spring.MatsTestConfiguration.MatsTestConfiguration_InfrastructureConfiguration;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.Rule_Mats;

/**
 * One-stop-shop for making simple integration/unit tests of MATS endpoints.
 * <ul>
 * <li>Meta-annotated with {@link EnableMats}, since you pretty obviously need that, and have better things to do than
 * to write it in each test</li>
 * <li>Starts a {@link Rule_Mats} instance (not within JUnit machinery - just uses the same features)</li>
 * <li>... and sticks the rule's {@link MatsFactory} instance into the Spring Context so that the EnableMats annotation
 * is happy, and also so that it is present for AutoWired injection in the tests.</li>
 * <li>.. and sticks the rule's {@link MatsInitiator} instance in the Spring Context, since it is pretty much certain
 * you'll need to initiate a message in your MATS tests.</li>
 * <li>Adds a default {@link MatsTestLatch} instance in the Spring Context, since when you need it, you need it both in
 * the {@literal @Configuration} class, and in the test-class - nice to already have it defined.</li>
 * <ul>
 *
 * @author Endre Stølsvik - 2016-06-23 - http://endre.stolsvik.com
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(MatsTestConfiguration_InfrastructureConfiguration.class)
@EnableMats
@Configuration
@Documented
public @interface MatsTestConfiguration {
    /**
     * Spring {@literal @Configuration} class that cooks up the simple test infrastructure, but handles the Spring Test
     * Context Caching system by only instantiating a single instance of Rule_Mats and caching it for the next
     * invocations. If we don't do this, we'd instantiate multiple instances of the JMS Broker when running in the
     * default mode, which is in-JVM AMQ broker.
     * <p>
     * For some more background on the Context Caching: <a href=
     * "http://docs.spring.io/spring/docs/current/spring-framework-reference/html/integration-testing.html#testcontext-ctx-management-caching">
     * Read the Spring doc about Context Caching</a> and <a href="https://jira.spring.io/browse/SPR-7377">read a
     * "wont-fix" bug report describing what was experienced here</a> (Notice that the {@literal @DirtiesContext}
     * suggested works (and still works!), but it would require that <i>all</i> MATS test classes employing
     * {@link MatsTestConfiguration} has to be annotated as such, which would be annoying - and also hindering the
     * actual caching!).
     */
    @Configuration
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public static class MatsTestConfiguration_InfrastructureConfiguration {

        // Use clogging, since that's what Spring does.
        private static final Log log = LogFactory.getLog(MatsTestConfiguration_InfrastructureConfiguration.class);

        private static Rule_Mats __rule_Mats;
        private static AtomicInteger __rule_Mats_Active_Count = new AtomicInteger();

        @Bean
        protected Rule_Mats rule_Mats() throws Throwable {
            if (__rule_Mats == null) {
                log.debug("Rule_Mats not instantiated and started yet, doing it now and caching.");
                __rule_Mats = new Rule_Mats();
                __rule_Mats.before();
                __rule_Mats_Active_Count.incrementAndGet();
            }
            else {
                log.debug("Rule_Mats already instantiated, returning cached instance.");
                __rule_Mats_Active_Count.incrementAndGet();
            }
            return __rule_Mats;
        }

        @PreDestroy
        protected void takeDownRuleMats() {
            if (__rule_Mats_Active_Count.decrementAndGet() == 0) {
                log.debug("This was the last destroy-invocation for " + this.getClass().getSimpleName()
                        + ", destroying cached instance of Rule_Mats.");
                __rule_Mats.after();
                __rule_Mats = null;
            }
            else {
                log.debug("Destroy-invocation for " + this.getClass().getSimpleName()
                        + ", but the cached instance use count of Rule_Mats is not zero yet, so not destroying it.");
            }
        }

        @Bean
        protected MatsFactory testMatsFactory(Rule_Mats rule_Mats) throws Throwable {
            return rule_Mats.getMatsFactory();
        }

        @Bean
        protected MatsInitiator testMatsInitiator(Rule_Mats rule_Mats) {
            return rule_Mats.getInitiator();
        }

        @Bean
        protected MatsTestLatch testMatsTestLatch() {
            return new MatsTestLatch();
        }
    }
}
