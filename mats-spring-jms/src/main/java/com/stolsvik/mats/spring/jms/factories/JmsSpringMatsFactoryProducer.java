package com.stolsvik.mats.spring.jms.factories;

import java.sql.Connection;

import javax.jms.ConnectionFactory;
import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager_JmsOnly;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.spring.SpringMatsFactory;
import com.stolsvik.mats.spring.jms.tx.JmsMatsTransactionManager_JmsAndSpringDstm;

/**
 * Provides an easy to get hold of the most probable {@link JmsMatsFactory} transaction manager configuration in the
 * Spring world (using {@link JmsMatsTransactionManager_JmsAndSpringDstm}).
 *
 * @author Endre Stølsvik 2019-06-10 02:45 - http://stolsvik.com/, endre@stolsvik.com
 */
public class JmsSpringMatsFactoryProducer {

    /**
     * If you need a {@link MatsFactory} employing Spring's DataSourceTransactionManager (which you probably do in a
     * Spring environment utilizing SQL), this is your factory method.
     * <p>
     * Make a @Bean-annotated method which returns the result of this method. The created MatsFactory is wrapped in
     * {@link SpringMatsFactory}, as it should be when running Mats in Spring.
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
     * @return the produced {@link SpringMatsFactory}
     */
    public static SpringMatsFactory createSpringDataSourceTxMatsFactory(String appName, String appVersion,
            MatsSerializer<?> matsSerializer, ConnectionFactory jmsConnectionFactory, DataSource sqlDataSource) {
        return SpringMatsFactory.wrapTargetMatsFactory(() -> {
            // :: Create the JMS and Spring DataSourceTransactionManager-backed JMS MatsFactory.
            // JmsSessionHandler (pooler)
            JmsMatsJmsSessionHandler_Pooling jmsSessionHandler = new JmsMatsJmsSessionHandler_Pooling((
                    s) -> jmsConnectionFactory.createConnection());
            // JMS + Spring's DataSourceTransactionManager-based MatsTransactionManager
            JmsMatsTransactionManager_JmsAndSpringDstm transMgr_SpringSql = JmsMatsTransactionManager_JmsAndSpringDstm
                    .create(sqlDataSource);

            // The MatsFactory itself, supplying the JmsSessionHandler and MatsTransactionManager.
            return JmsMatsFactory
                    .createMatsFactory(appName, appVersion, jmsSessionHandler, transMgr_SpringSql, matsSerializer);
        });
    }

    /**
     * If you need a {@link MatsFactory} that only handles the JMS transactions, this is your factory method - but if
     * you DO make any database calls within any Mats endpoint lambda, you will now have no or poor transactional
     * demarcation, use
     * {@link #createSpringDataSourceTxMatsFactory(String, String, MatsSerializer, ConnectionFactory, DataSource)
     * createSpringDataSourceTxMatsFactory(..)} instead.
     * <p>
     * Make a @Bean-annotated method which returns the result of this method. The created MatsFactory is wrapped in
     * {@link SpringMatsFactory}, as it should be when running Mats in Spring.
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
     * @return the produced {@link SpringMatsFactory}
     */
    public static SpringMatsFactory createJmsTxOnlyMatsFactory(String appName, String appVersion,
            MatsSerializer<?> matsSerializer, ConnectionFactory jmsConnectionFactory) {
        return SpringMatsFactory.wrapTargetMatsFactory(() -> {
            // :: Create the JMS and Spring DataSourceTransactionManager-backed JMS MatsFactory.
            // JmsSessionHandler (pooler)
            JmsMatsJmsSessionHandler_Pooling jmsSessionHandler = new JmsMatsJmsSessionHandler_Pooling((
                    s) -> jmsConnectionFactory.createConnection());
            // JMS only MatsTransactionManager
            JmsMatsTransactionManager jmsOnlyTransMgr = JmsMatsTransactionManager_JmsOnly.create();

            // The MatsFactory itself, supplying the JmsSessionHandler and MatsTransactionManager.
            return JmsMatsFactory
                    .createMatsFactory(appName, appVersion, jmsSessionHandler, jmsOnlyTransMgr, matsSerializer);
        });
    }
}
