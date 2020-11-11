package com.stolsvik.mats.junit.spring;

import org.junit.runner.RunWith;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestExecutionListeners.MergeMode;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Simple;
import com.stolsvik.mats.junit.spring.AbstractSpringTest.SpringContext;
import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;
import com.stolsvik.mats.util.MatsFuturizer;
import com.stolsvik.mats.util_activemq.MatsLocalVmActiveMq;

/**
 * Example spring context for testing using {@link RuleMatsAutowireTestExecutionListener} to autowire rules.
 *
 * @author Kevin Mc Tiernan, 2020-11-10, kmctiernan@gmail.com
 */
@RunWith(SpringJUnit4ClassRunner.class)
// This would normally point to the applications @Configuration class.
@ContextConfiguration(classes = SpringContext.class)
@ComponentScan(basePackages = "com.stolsvik.mats.junit.spring")
@DirtiesContext
@TestExecutionListeners(value = RuleMatsAutowireTestExecutionListener.class, mergeMode = MergeMode.MERGE_WITH_DEFAULTS)
public abstract class AbstractSpringTest {
    @Configuration
    public static class SpringContext {
        @Bean
        public MatsFuturizer futurizer(MatsFactory matsFactory) {
            return MatsFuturizer.createMatsFuturizer(matsFactory, "SpringTest");
        }

        @Bean
        public MatsSerializer_DefaultJson matsSerializer() {
            return new MatsSerializer_DefaultJson();
        }

        @Bean
        public MatsLocalVmActiveMq mqBroker() {
            return MatsLocalVmActiveMq.createDefaultInVmActiveMq();
        }

        @Bean
        public MatsFactory matsFactory(MatsLocalVmActiveMq broker, MatsSerializer_DefaultJson serializer) {
            JmsMatsJmsSessionHandler_Simple jmsMatsJmsSessionHandler_simple =
                    JmsMatsJmsSessionHandler_Simple.create(broker.getConnectionFactory());
            return JmsMatsFactory
                    .createMatsFactory_JmsOnlyTransactions("MATS-JUPITER", "test", jmsMatsJmsSessionHandler_simple,
                            serializer);
        }
    }
}
