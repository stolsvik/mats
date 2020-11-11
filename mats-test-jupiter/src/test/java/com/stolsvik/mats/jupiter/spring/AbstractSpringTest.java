package com.stolsvik.mats.jupiter.spring;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestExecutionListeners.MergeMode;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Simple;
import com.stolsvik.mats.jupiter.spring.AbstractSpringTest.SpringContext;
import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;
import com.stolsvik.mats.util.MatsFuturizer;
import com.stolsvik.mats.util_activemq.MatsLocalVmActiveMq;

/**
 * Example spring context for testing using {@link ExtensionMatsAutowireTestExecutionListener} to autowire extensions.
 *
 * @author Kevin Mc Tiernan, 2020-11-10, kmctiernan@gmail.com
 */
@ExtendWith(SpringExtension.class)
// This would normally point to the applications @Configuration class.
@ContextConfiguration(classes = SpringContext.class)
@ComponentScan(basePackages = "com.stolsvik.mats.jupiter.spring")
@DirtiesContext
@TestExecutionListeners(value = ExtensionMatsAutowireTestExecutionListener.class,
        mergeMode = MergeMode.MERGE_WITH_DEFAULTS)
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
