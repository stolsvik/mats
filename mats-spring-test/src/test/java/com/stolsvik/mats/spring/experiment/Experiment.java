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

@Configuration
@EnableMats
@ComponentScan(basePackages = "com.stolsvik.mats.spring.experiment")
public class Experiment {

    private static final Logger log = LoggerFactory.getLogger(Experiment.class);

    public static void main(String... args) throws InterruptedException {
        new Experiment().start();
    }

    @Bean(destroyMethod = "after")
    protected Rule_Mats rule_Mats() throws Throwable {
        Rule_Mats rule_Mats = new Rule_Mats();
        rule_Mats.before();
        return rule_Mats;
    }

    @Bean
    protected MatsFactory matsFactory(Rule_Mats rule_Mats) throws Throwable {
        return rule_Mats.getMatsFactory();
    }

    public void start() throws InterruptedException {
        log.info("Starting Experiment! new'ing up AnnotationConfigApplicationContext.");
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(Experiment.class);
        TestApp testApp = ctx.getBean(TestApp.class);
        try {
            testApp.run();
        }
        catch (Throwable t) {
            log.error("Got some Exception when running app.", t);
        }

        ctx.destroy();

        log.info("Exiting!");
    }
}
