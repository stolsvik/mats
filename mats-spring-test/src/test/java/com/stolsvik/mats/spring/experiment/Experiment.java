package com.stolsvik.mats.spring.experiment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.spring.EnableMats;
import com.stolsvik.mats.test.Rule_Mats;

import javax.jms.ConnectionFactory;

@Configuration
@EnableMats
@ComponentScan(basePackages = "com.stolsvik.mats.spring.experiment")
public class Experiment {

    private static final Logger log = LoggerFactory.getLogger(Experiment.class);

    public static void main(String... args) {
        new Experiment().start();
    }

    @Bean(destroyMethod = "after")
    protected Rule_Mats rule_Mats() throws Throwable {
        Rule_Mats rule_Mats = new Rule_Mats();
        rule_Mats.before();
        return rule_Mats;
    }

    @Bean
    protected MatsFactory matsFactory(Rule_Mats rule_Mats) {
        return rule_Mats.getMatsFactory();
    }

    public void start() {
        long nanosStart = System.nanoTime();
        log.info("Starting Experiment! new'ing up AnnotationConfigApplicationContext.");
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(Experiment.class);
        log.info("=========== Done new'ing up AnnotationConfigApplicationContext: [" + ctx + "].");


        TestApplicationBean testApplicationBean = ctx.getBean(TestApplicationBean.class);
        try {
            testApplicationBean.run();
        }
        catch (Throwable t) {
            log.error("Got some Exception when running app.", t);
        }

        ctx.close();

        log.info("Exiting! took " + ((System.nanoTime() - nanosStart) / 1_000_000d) + " ms.");
    }
}
