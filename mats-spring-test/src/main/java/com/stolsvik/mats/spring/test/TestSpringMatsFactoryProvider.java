package com.stolsvik.mats.spring.test;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.ConnectionFactory;
import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsFactory.MatsFactoryWrapper;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager_JmsOnly;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;
import com.stolsvik.mats.spring.jms.factories.JmsSpringConnectionFactoryProducer;
import com.stolsvik.mats.spring.jms.tx.JmsMatsTransactionManager_JmsAndSpringDstm;
import com.stolsvik.mats.util_activemq.MatsLocalVmActiveMq;

/**
 * A testing-oriented {@link MatsFactory}-provider corresponding to the methods in
 * {@link JmsSpringConnectionFactoryProducer} but which also create a separate LocalVM ActiveMQ broker for the produced
 * MatsFactory to connect to - this is for the scenarios where you do <i>NOT</i> have your test load the entire
 * application's Spring configuration, but instead "piece together" the relevant @Components containing test-relevant
 * Mats endpoints and other beans from your application along with test-specific mocked-out endpoints: You will then
 * probably not have the MatsFactory present in the Spring context. You can then use this class to get a "quick and
 * dirty" MatsFactory (with an in-vm MQ Broker backing it) on which your application's endpoints, and mocked-out
 * endpoints in the test, can run.
 *
 * @author Endre Stølsvik 2019-06-17 21:26 - http://stolsvik.com/, endre@stolsvik.com
 */
public class TestSpringMatsFactoryProvider {

    private static final AtomicInteger _sequence = new AtomicInteger(0);

    /**
     * If you need a {@link MatsFactory} employing Spring's DataSourceTransactionManager (which you probably do in a
     * Spring environment utilizing SQL), this is your factory method.
     * <p>
     * Usage: Make a @Bean-annotated method which returns the result of this method.
     *
     * @param concurrency
     *            The concurrency of the created {@link MatsFactory}, set with
     *            {@link FactoryConfig#setConcurrency(int)}.
     * @param sqlDataSource
     *            the SQL DataSource which to stash into a Spring {@link DataSourceTransactionManager}, and from which
     *            SQL {@link Connection}s are fetched from, using {@link DataSource#getConnection()}. It is assumed that
     *            if username and password is needed, you have configured that on the DataSource.
     * @return the produced {@link MatsFactory}
     */
    public static MatsFactory createSpringDataSourceTxTestMatsFactory(int concurrency, DataSource sqlDataSource) {
        // Naming broker as calling class, performing replacement of illegal chars according to ActiveMQ rules.
        String appName = getCallerClassName().replaceAll("[^a-zA-Z0-9\\.\\_\\-\\:]", ".")
                + "_" + _sequence.getAndIncrement();
        MatsLocalVmActiveMq inVmActiveMq = MatsLocalVmActiveMq.createInVmActiveMq(appName);
        ConnectionFactory jmsConnectionFactory = inVmActiveMq.getConnectionFactory();
        // :: Create the JMS and Spring DataSourceTransactionManager-backed JMS MatsFactory.
        // JmsSessionHandler (pooler)
        JmsMatsJmsSessionHandler jmsSessionHandler = JmsMatsJmsSessionHandler_Pooling.create(jmsConnectionFactory);
        // JMS + Spring's DataSourceTransactionManager-based MatsTransactionManager
        JmsMatsTransactionManager springSqlTxMgr = JmsMatsTransactionManager_JmsAndSpringDstm.create(sqlDataSource);

        // Serializer
        MatsSerializer<String> matsSerializer = new MatsSerializer_DefaultJson();

        // The MatsFactory itself, supplying the JmsSessionHandler and MatsTransactionManager.
        JmsMatsFactory<String> matsFactory = JmsMatsFactory
                .createMatsFactory(appName, "#testing#", jmsSessionHandler, springSqlTxMgr, matsSerializer);
        // Set concurrency.
        matsFactory.getFactoryConfig().setConcurrency(concurrency);
        // Now wrap this in a wrapper that will close the LocalVM ActiveMQ broker upon matsFactory.stop().
        return new MatsFactoryStopLocalVmBrokerWrapper(matsFactory, inVmActiveMq);
    }

    /**
     * Convenience variant of {@link #createSpringDataSourceTxTestMatsFactory(int, DataSource)} where concurrency is 1,
     * which should be adequate for most testing - unless you explicitly want to test concurrency.
     *
     * @param sqlDataSource
     *            the SQL DataSource which to stash into a Spring {@link DataSourceTransactionManager}, and from which
     *            SQL {@link Connection}s are fetched from, using {@link DataSource#getConnection()}. It is assumed that
     *            if username and password is needed, you have configured that on the DataSource.
     * @return the produced {@link MatsFactory}
     */
    public static MatsFactory createSpringDataSourceTxTestMatsFactory(DataSource sqlDataSource) {
        return createSpringDataSourceTxTestMatsFactory(1, sqlDataSource);
    }

    /**
     * If you need a {@link MatsFactory} that only handles the JMS transactions, this is your factory method - but if
     * you DO make any database calls within any Mats endpoint lambda, you will now have no or poor transactional
     * demarcation, use {@link #createSpringDataSourceTxTestMatsFactory(int, DataSource)
     * createSpringDataSourceTxTestMatsFactory(..)} instead.
     * <p>
     * Usage: Make a @Bean-annotated method which returns the result of this method.
     *
     * @param concurrency
     *            The concurrency of the created {@link MatsFactory}, set with
     *            {@link FactoryConfig#setConcurrency(int)}.
     * @return the produced {@link MatsFactory}
     */
    public static MatsFactory createJmsTxOnlyTestMatsFactory(int concurrency) {
        // Naming broker as calling class, performing replacement of illegal chars according to ActiveMQ rules.
        String appName = getCallerClassName().replaceAll("[^a-zA-Z0-9\\.\\_\\-\\:]", ".")
                + "_" + _sequence.getAndIncrement();

        MatsLocalVmActiveMq inVmActiveMq = MatsLocalVmActiveMq.createInVmActiveMq(appName);
        ConnectionFactory jmsConnectionFactory = inVmActiveMq.getConnectionFactory();
        // :: Create the JMS and Spring DataSourceTransactionManager-backed JMS MatsFactory.
        // JmsSessionHandler (pooler)
        JmsMatsJmsSessionHandler jmsSessionHandler = JmsMatsJmsSessionHandler_Pooling.create(jmsConnectionFactory);
        // JMS only MatsTransactionManager
        JmsMatsTransactionManager jmsOnlyTxMgr = JmsMatsTransactionManager_JmsOnly.create();

        // Serializer
        MatsSerializer<String> matsSerializer = new MatsSerializer_DefaultJson();

        // The MatsFactory itself, supplying the JmsSessionHandler and MatsTransactionManager.
        JmsMatsFactory<String> matsFactory = JmsMatsFactory.createMatsFactory(appName, "#testing#",
                jmsSessionHandler, jmsOnlyTxMgr, matsSerializer);
        // Set concurrency.
        matsFactory.getFactoryConfig().setConcurrency(concurrency);
        // Now wrap this in a wrapper that will close the LocalVM ActiveMQ broker upon matsFactory.stop().
        return new MatsFactoryStopLocalVmBrokerWrapper(matsFactory, inVmActiveMq);
    }

    /**
     * Convenience variant of {@link #createJmsTxOnlyTestMatsFactory(int)} where concurrency is 1, which should be
     * adequate for most testing - unless you explicitly want to test concurrency.
     *
     * @return the produced {@link MatsFactory}
     */
    public static MatsFactory createJmsTxOnlyTestMatsFactory() {
        return createJmsTxOnlyTestMatsFactory(1);
    }

    private static class MatsFactoryStopLocalVmBrokerWrapper extends MatsFactoryWrapper {
        private final MatsLocalVmActiveMq _matsLocalVmActiveMq;

        public MatsFactoryStopLocalVmBrokerWrapper(MatsFactory targetMatsFactory,
                MatsLocalVmActiveMq matsLocalVmActiveMq) {
            super(targetMatsFactory);
            _matsLocalVmActiveMq = matsLocalVmActiveMq;
        }

        @Override
        public boolean stop(int gracefulShutdownMillis) {
            boolean stopped = super.stop(gracefulShutdownMillis);
            _matsLocalVmActiveMq.close();
            return stopped;
        }
    }

    /**
     * from <a href="https://stackoverflow.com/a/11306854">Stackoverflow - Denys Séguret</a>.
     */
    private static String getCallerClassName() {
        StackTraceElement[] stElements = Thread.currentThread().getStackTrace();
        for (int i = 1; i < stElements.length; i++) {
            StackTraceElement ste = stElements[i];
            if (!ste.getClassName().equals(TestSpringMatsFactoryProvider.class.getName())) {
                return ste.getClassName();
            }
        }
        throw new AssertionError("Could not determine calling class.");
    }

}
