package com.stolsvik.mats.spring.jms.tx.varioussetups;

import javax.jms.ConnectionFactory;

import org.junit.runner.RunWith;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.spring.EnableMats;
import com.stolsvik.mats.spring.jms.tx.JmsMatsTransactionManager_JmsAndSpringManagedSqlTx;

/**
 * Abstract test for Spring DB Transaction management, creating a MatsFactory using a PlatformTransactionManager,
 * subclasses specifies how that is created and which type it is (DataSourceTxMgr or HibernateTxMgr).
 *
 * @author Endre Stølsvik 2020-06-05 00:10 - http://stolsvik.com/, endre@stolsvik.com
 */
@RunWith(SpringRunner.class)
public abstract class Test_SpringManagedTx_H2Based_Abstract_PlatformTransactionManager
        extends Test_SpringManagedTx_H2Based_AbstractBase {
    @Configuration
    @EnableMats
    static abstract class SpringConfiguration_AbstractPlatformTransactionManager
            extends SpringConfiguration_AbstractBase {
        @Bean
        protected MatsFactory createMatsFactory(PlatformTransactionManager platformTransactionManager,
                ConnectionFactory connectionFactory, MatsSerializer<String> matsSerializer) {

            // Create the JMS and Spring DataSourceTransactionManager-backed JMS MatsFactory.
            JmsMatsJmsSessionHandler jmsSessionHandler = JmsMatsJmsSessionHandler_Pooling.create(connectionFactory);
            JmsMatsTransactionManager txMgrSpring = JmsMatsTransactionManager_JmsAndSpringManagedSqlTx.create(
                    platformTransactionManager);

            JmsMatsFactory<String> matsFactory = JmsMatsFactory.createMatsFactory(this.getClass().getSimpleName(),
                    "*testing*", jmsSessionHandler, txMgrSpring, matsSerializer);
            // For the MULTIPLE test scenario, it makes sense to test concurrency, so we go for 5.
            matsFactory.getFactoryConfig().setConcurrency(5);
            return matsFactory;
        }
    }
}
