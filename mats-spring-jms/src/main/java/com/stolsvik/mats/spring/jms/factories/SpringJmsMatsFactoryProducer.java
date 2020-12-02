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
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager_Jms;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.spring.jms.tx.JmsMatsTransactionManager_JmsAndSpringManagedSqlTx;

/**
 * Provides an easy way to get hold of the most probable {@link JmsMatsFactory} transaction manager configuration in the
 * Spring world (using {@link JmsMatsTransactionManager_JmsAndSpringManagedSqlTx}, or only the
 * {@link JmsMatsTransactionManager_Jms} if no DataSource is needed) - as a convenience, if the supplied
 * {@link ConnectionFactory} is of type {@link ScenarioConnectionFactoryWrapper}, a check is done whether it was created
 * with {@link MatsScenario#LOCALVM}, in which case it is assumed that it is in testing mode, and the factory's
 * concurrency is then set to 1.
 *
 * @author Endre Stølsvik 2019-06-10 02:45 - http://stolsvik.com/, endre@stolsvik.com
 */
public class SpringJmsMatsFactoryProducer {
    private static final Logger log = LoggerFactory.getLogger(SpringJmsMatsFactoryProducer.class);
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
    public static SpringJmsMatsFactoryWrapper createSpringDataSourceTxMatsFactory(String appName, String appVersion,
            MatsSerializer<?> matsSerializer, ConnectionFactory jmsConnectionFactory, DataSource sqlDataSource) {
        // :: Create the JMS and Spring DataSourceTransactionManager-backed JMS MatsFactory.
        log.info(LOG_PREFIX + "createSpringDataSourceTxMatsFactory(" + appName + ", " + appVersion + ", "
                + matsSerializer + ", " + jmsConnectionFactory + ", " + sqlDataSource + ")");
        // JMS + Spring's DataSourceTransactionManager-based MatsTransactionManager
        JmsMatsTransactionManager springSqlTxMgr = JmsMatsTransactionManager_JmsAndSpringManagedSqlTx.create(
                sqlDataSource);

        return createJmsMatsFactory(appName, appVersion, matsSerializer, jmsConnectionFactory,
                springSqlTxMgr);
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
    public static SpringJmsMatsFactoryWrapper createJmsTxOnlyMatsFactory(String appName, String appVersion,
            MatsSerializer<?> matsSerializer, ConnectionFactory jmsConnectionFactory) {
        // :: Create the JMS and Spring DataSourceTransactionManager-backed JMS MatsFactory.
        log.info(LOG_PREFIX + "createJmsTxOnlyMatsFactory(" + appName + ", " + appVersion + ", " + matsSerializer + ", "
                + jmsConnectionFactory + ")");
        // JMS only MatsTransactionManager
        JmsMatsTransactionManager jmsOnlyTxMgr = JmsMatsTransactionManager_Jms.create();

        return createJmsMatsFactory(appName, appVersion, matsSerializer, jmsConnectionFactory,
                jmsOnlyTxMgr);
    }

    private static SpringJmsMatsFactoryWrapper createJmsMatsFactory(String appName, String appVersion,
            MatsSerializer<?> matsSerializer, ConnectionFactory jmsConnectionFactory,
            JmsMatsTransactionManager txMgr) {
        // JmsSessionHandler (pooler)
        JmsMatsJmsSessionHandler jmsSessionHandler = JmsMatsJmsSessionHandler_Pooling.create(jmsConnectionFactory);
        // The MatsFactory itself, supplying the JmsSessionHandler and MatsTransactionManager.
        JmsMatsFactory<?> matsFactory = JmsMatsFactory
                .createMatsFactory(appName, appVersion, jmsSessionHandler, txMgr, matsSerializer);

        return new SpringJmsMatsFactoryWrapper(jmsConnectionFactory, matsFactory);
    }
}
