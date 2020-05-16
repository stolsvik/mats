package com.stolsvik.mats.spring.jms.factories;

import java.sql.Connection;

import javax.jms.ConnectionFactory;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager_JmsOnly;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.spring.jms.factories.ConnectionFactoryScenarioWrapper.MatsScenario;
import com.stolsvik.mats.spring.jms.tx.JmsMatsTransactionManager_JmsAndSpringDstm;

/**
 * Provides an easy to get hold of the most probable {@link JmsMatsFactory} transaction manager configuration in the
 * Spring world (using {@link JmsMatsTransactionManager_JmsAndSpringDstm}).
 *
 * @author Endre Stølsvik 2019-06-10 02:45 - http://stolsvik.com/, endre@stolsvik.com
 */
public class JmsSpringMatsFactoryProducer {
    private static final Logger log = LoggerFactory.getLogger(JmsSpringMatsFactoryProducer.class);
    private static final String LOG_PREFIX = "#SPRINGJMATS# ";

    /**
     * If you need a {@link MatsFactory} employing Spring's DataSourceTransactionManager (which you probably do in a
     * Spring environment utilizing SQL), this is your factory method.
     * <p>
     * Usage: Make a @Bean-annotated method which returns the result of this method.
     *
     * @param appName
     *            the containing application's name (for debugging purposes, you'll find it in the trace).
     * @param appVersion
     *            the containing application's version (for debugging purposes, you'll find it in the trace).
     * @param matsSerializer
     *            the {@link JmsMatsFactory} utilizes the {@link MatsSerializer}, so you need to provide one. (It is
     *            probably the one from the 'mats-serial-json' package).
     * @param jmsConnectionFactory
     *            the JMS {@link ConnectionFactory} to fetch JMS Connections from, using
     *            {@link ConnectionFactory#createConnection()}. It is assumed that if username and password is needed,
     *            you have configured that on the ConnectionFactory. Otherwise, you'll have to make the JmsMatsFactory
     *            yourself - check the code of this method, and you'll see where the JMS Connections are created.
     * @param sqlDataSource
     *            the SQL DataSource which to stash into a Spring {@link DataSourceTransactionManager}, and from which
     *            SQL {@link Connection}s are fetched from, using {@link DataSource#getConnection()}. It is assumed that
     *            if username and password is needed, you have configured that on the DataSource.
     * @return the produced {@link MatsFactory}
     */
    public static MatsFactory createSpringDataSourceTxMatsFactory(String appName, String appVersion,
            MatsSerializer<?> matsSerializer, ConnectionFactory jmsConnectionFactory, DataSource sqlDataSource) {
        // :: Create the JMS and Spring DataSourceTransactionManager-backed JMS MatsFactory.
        log.info(LOG_PREFIX + "createSpringDataSourceTxMatsFactory(" + appName + ", " + appVersion + ", "
                + matsSerializer + ", " + jmsConnectionFactory + ", " + sqlDataSource + ")");
        // JmsSessionHandler (pooler)
        JmsMatsJmsSessionHandler jmsSessionHandler = JmsMatsJmsSessionHandler_Pooling.create(jmsConnectionFactory);
        // JMS + Spring's DataSourceTransactionManager-based MatsTransactionManager
        JmsMatsTransactionManager springSqlTxMgr = JmsMatsTransactionManager_JmsAndSpringDstm.create(sqlDataSource);

        // The MatsFactory itself, supplying the JmsSessionHandler and MatsTransactionManager.
        JmsMatsFactory<?> matsFactory = JmsMatsFactory
                .createMatsFactory(appName, appVersion, jmsSessionHandler, springSqlTxMgr, matsSerializer);
        // :: Handle testing situation
        // ?: Is the provided JMS ConnectionFactory a ConnectionFactoryScenarioWrapper?
        if (jmsConnectionFactory instanceof ConnectionFactoryScenarioWrapper) {
            // -> Yes it is, check if we're in LOCALVM mode
            ConnectionFactoryScenarioWrapper scenarioWrapped = (ConnectionFactoryScenarioWrapper) jmsConnectionFactory;
            // ?: Are we in MatsScenario.LOCALVM?
            if (scenarioWrapped.getMatsScenarioUsedToMakeConnectionFactory() == MatsScenario.LOCALVM) {
                // -> Yes, so assume test - set concurrency to 1.
                log.info(LOG_PREFIX + "The supplied ConnectionFactory was created with MatsScenario.LOCALVM, so we"
                        + " assume this is a testing situation, and set the concurrency to 1.");
                matsFactory.getFactoryConfig().setConcurrency(1);
            }
        }
        return matsFactory;
    }

    /**
     * If you need a {@link MatsFactory} that only handles the JMS transactions, this is your factory method - but if
     * you DO make any database calls within any Mats endpoint lambda, you will now have no or poor transactional
     * demarcation, use
     * {@link #createSpringDataSourceTxMatsFactory(String, String, MatsSerializer, ConnectionFactory, DataSource)
     * createSpringDataSourceTxMatsFactory(..)} instead.
     * <p>
     * Usage: Make a @Bean-annotated method which returns the result of this method.
     *
     * @param appName
     *            the containing application's name (for debugging purposes, you'll find it in the trace).
     * @param appVersion
     *            the containing application's version (for debugging purposes, you'll find it in the trace).
     * @param matsSerializer
     *            the {@link JmsMatsFactory} utilizes the {@link MatsSerializer}, so you need to provide one. (It is
     *            probably the one from the 'mats-serial-json' package).
     * @param jmsConnectionFactory
     *            the JMS {@link ConnectionFactory} to fetch JMS Connections from, using
     *            {@link ConnectionFactory#createConnection()}. It is assumed that if username and password is needed,
     *            you have configured that on the ConnectionFactory. Otherwise, you'll have to make the JmsMatsFactory
     *            yourself - check the code of this method, and you'll see where the JMS Connections are created.
     * @return the produced {@link MatsFactory}
     */
    public static MatsFactory createJmsTxOnlyMatsFactory(String appName, String appVersion,
            MatsSerializer<?> matsSerializer, ConnectionFactory jmsConnectionFactory) {
        // :: Create the JMS and Spring DataSourceTransactionManager-backed JMS MatsFactory.
        log.info(LOG_PREFIX + "createJmsTxOnlyMatsFactory(" + appName + ", " + appVersion + ", " + matsSerializer + ", "
                + jmsConnectionFactory + ")");
        // JmsSessionHandler (pooler)
        JmsMatsJmsSessionHandler jmsSessionHandler = JmsMatsJmsSessionHandler_Pooling.create(jmsConnectionFactory);
        // JMS only MatsTransactionManager
        JmsMatsTransactionManager jmsOnlyTxMgr = JmsMatsTransactionManager_JmsOnly.create();

        // The MatsFactory itself, supplying the JmsSessionHandler and MatsTransactionManager.
        JmsMatsFactory<?> matsFactory = JmsMatsFactory
                .createMatsFactory(appName, appVersion, jmsSessionHandler, jmsOnlyTxMgr, matsSerializer);
        // :: Handle testing situation
        // ?: Is the provided JMS ConnectionFactory a ConnectionFactoryScenarioWrapper?
        if (jmsConnectionFactory instanceof ConnectionFactoryScenarioWrapper) {
            // -> Yes it is, check if we're in LOCALVM mode
            ConnectionFactoryScenarioWrapper scenarioWrapped = (ConnectionFactoryScenarioWrapper) jmsConnectionFactory;
            // ?: Are we in MatsScenario.LOCALVM?
            if (scenarioWrapped.getMatsScenarioUsedToMakeConnectionFactory() == MatsScenario.LOCALVM) {
                // -> Yes, so assume test - set concurrency to 1.
                log.info(LOG_PREFIX + "The supplied ConnectionFactory was created with MatsScenario.LOCALVM, so we"
                        + " assume this is a testing situation, and set the concurrency to 1.");
                matsFactory.getFactoryConfig().setConcurrency(1);
            }
        }
        return matsFactory;
    }
}
