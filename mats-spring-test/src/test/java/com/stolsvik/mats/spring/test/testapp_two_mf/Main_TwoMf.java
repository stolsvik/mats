package com.stolsvik.mats.spring.test.testapp_two_mf;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.jms.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;
import com.stolsvik.mats.spring.EnableMats;
import com.stolsvik.mats.test.MatsTestActiveMq;

/**
 * @author Endre Stølsvik 2019-05-17 21:42 - http://stolsvik.com/, endre@stolsvik.com
 */
@Configuration
@EnableMats
@ComponentScan(basePackages = "com.stolsvik.mats.spring.test.testapp_two_mf")
public class Main_TwoMf {
    private static final Logger log = LoggerFactory.getLogger(Main_TwoMf.class);

    private static MatsTestActiveMq _activeMq1;
    private static MatsTestActiveMq _activeMq2;

    public static void main(String... args) throws InterruptedException {
        new Main_TwoMf().start();
    }

    private void start() {
        long nanosStart = System.nanoTime();
        log.info("Starting " + this.getClass().getSimpleName() + "!");
        log.info(" \\- new'ing up AnnotationConfigApplicationContext, giving class [" + this.getClass()
                .getSimpleName() + "] as base.");
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(this.getClass());
        log.info(" \\- done, AnnotationConfigApplicationContext: [" + ctx + "].");

        // ----- Spring is running.

        log.info("Starting application.");
        try {
            TestApplicationBean testApplicationBean = ctx.getBean(TestApplicationBean.class);
            testApplicationBean.run();
        }
        catch (Throwable t) {
            String msg = "Got some Exception when running app.";
            log.error(msg, t);
            throw new RuntimeException(msg, t);
        }
        finally {
            // :: Close Spring.
            ctx.close();

            log.info("Exiting! took " + ((System.nanoTime() - nanosStart) / 1_000_000d) + " ms.");
        }
    }

    @Bean(destroyMethod = "stopBroker")
    protected MatsTestActiveMq activeMq1() {
        return MatsTestActiveMq.createTestActiveMq("activeMq1");
    }

    @Bean(destroyMethod = "stopBroker")
    protected MatsTestActiveMq activeMq2() {
        return MatsTestActiveMq.createTestActiveMq("activeMq2");
    }

    @Bean
    protected ConnectionFactory connectionFactory1(@Qualifier("activeMq1") MatsTestActiveMq activeMq1) {
        return activeMq1.getConnectionFactory();
    }

    @Bean
    protected ConnectionFactory connectionFactory2(@Qualifier("activeMq2") MatsTestActiveMq activeMq2) {
        return activeMq2.getConnectionFactory();
    }

    @Bean
    @TestQualifier(endre="Elg")
    @Qualifier("matsFactoryX")
    protected MatsFactory matsFactory1(@Qualifier("connectionFactory1") ConnectionFactory connectionFactory) {
        return JmsMatsFactory.createMatsFactory_JmsOnlyTransactions(this.getClass().getSimpleName(), "#testing#",
                new JmsMatsJmsSessionHandler_Pooling((ctx) -> connectionFactory.createConnection()),
                new MatsSerializer_DefaultJson());
    }

    @Bean
    @Qualifier("matsFactoryY")
    protected MatsFactory matsFactory2(@Qualifier("connectionFactory2") ConnectionFactory connectionFactory) {
        return JmsMatsFactory.createMatsFactory_JmsOnlyTransactions(this.getClass().getSimpleName(), "#testing#",
                new JmsMatsJmsSessionHandler_Pooling((ctx) -> connectionFactory.createConnection()),
                new MatsSerializer_DefaultJson());
    }

    @Target({ ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    @Qualifier
    public @interface TestQualifier {
        String endre() default "";
    }
}