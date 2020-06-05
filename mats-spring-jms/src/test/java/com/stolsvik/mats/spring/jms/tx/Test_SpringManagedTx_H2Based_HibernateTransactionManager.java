package com.stolsvik.mats.spring.jms.tx;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.sql.DataSource;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.orm.hibernate5.LocalSessionFactoryBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.MethodMode;
import org.springframework.test.context.junit4.SpringRunner;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.spring.EnableMats;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.jms.tx.Test_SpringManagedTx_H2Based_AbstractResourceTransactionaManager.SpringConfiguration_UsingPlatformTransactionManager;
import com.stolsvik.mats.test.MatsTestLatch.Result;
import com.stolsvik.mats.util.RandomString;

/**
 * Testing Spring DB Transaction management, using HibernateTransactionManager - also including tests using all of
 * Hibernate/JPA, Spring JDBC and Plain JDBC in a single Stage, all being tx-managed from the
 * sole HibernateTransactionManager.
 *
 * @author Endre Stølsvik 2020-06-05 00:10 - http://stolsvik.com/, endre@stolsvik.com
 */
@RunWith(SpringRunner.class)
public class Test_SpringManagedTx_H2Based_HibernateTransactionManager extends Test_SpringManagedTx_H2Based_Base {

    private static final Logger log = LoggerFactory.getLogger(
            Test_SpringManagedTx_H2Based_HibernateTransactionManager.class);

    public static final String SERVICE_HIBERNATE = "mats.spring.SpringManagedTx_H2Based_Hibernate";

    @Configuration
    @EnableMats
    static class SpringConfiguration_HibernateTxMgr extends
            SpringConfiguration_UsingPlatformTransactionManager {

        @Bean
        LocalSessionFactoryBean createHibernateSessionFactory(DataSource dataSource) {
            // This is a FactoryBean that creates a Hibernate SessionFactory working with Spring's HibernateTxMgr
            LocalSessionFactoryBean factory = new LocalSessionFactoryBean();
            // Setting the DataSource
            factory.setDataSource(dataSource);
            // Setting the single annotated Entity test class we have
            factory.setAnnotatedClasses(DataTableDbo.class);
            return factory;
        }

        @Bean
        HibernateTransactionManager createHibernateTransactionaManager(SessionFactory sessionFactory) {
            // Note: don't need to .setDataSource() since we use LocalSessionFactoryBean, read JavaDoc of said method.
            return new HibernateTransactionManager(sessionFactory);
        }

        @Inject
        private SessionFactory _sessionFactory;

        @Inject
        private DataSource _dataSource;

        /**
         * Setting up the single-stage endpoint that will store a row in the database using Hibernate, Spring JDBC and
         * Plain JDBC, but which will throw afterwards if the request DTO says so.
         */
        @MatsMapping(endpointId = SERVICE_HIBERNATE)
        public SpringTestDataTO springMatsSingleEndpoint_Hibernate(ProcessContext<SpringTestDataTO> context,
                SpringTestDataTO msg) {
            log.info("Incoming message for '" + SERVICE + "': DTO:[" + msg + "], context:\n" + context);

            // :: Insert row in database using Hibernate/JPA
            String valueHibernate = SERVICE_HIBERNATE + '[' + msg.string + "]-Hibernate";
            DataTableDbo data = new DataTableDbo(valueHibernate);
            // Getting current Hibernate Session (must not close it)
            Session currentSession = _sessionFactory.getCurrentSession();
            currentSession.save(data);

            // :: .. and also insert row using Spring JDBC
            String valueSpringJdbc = SERVICE_HIBERNATE + '[' + msg.string + "]-SpringJdbc";
            _jdbcTemplate.update("INSERT INTO datatable VALUES (?)", valueSpringJdbc);

            // :: .. and finally insert row using pure JDBC
            String valuePlainJdbc = SERVICE_HIBERNATE + '[' + msg.string + "]-PlainJdbc";
            Connection con = DataSourceUtils.getConnection(_dataSource);
            try {
                PreparedStatement stmt = con.prepareStatement("INSERT INTO datatable VALUES (?)");
                stmt.setString(1, valuePlainJdbc);
                stmt.execute();
                stmt.close();
                // NOTE: Must NOT close Connection.
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }

            // Assert that this is the same Connection that we get from the ProcessContext
            Optional<Connection> contextAttributeConnection = context.getAttribute(Connection.class);
            Assert.assertSame(con, contextAttributeConnection.get());

            if (msg.string.startsWith(THROW)) {
                log.info("Asked to throw RuntimeException, and that we do!");
                throw new RuntimeException("This RTE should make the SQL INSERT rollback!");
            }
            return new SpringTestDataTO(msg.number * 2, msg.string);
        }
    }

    @Entity
    @Table(name = "datatable")
    public static class DataTableDbo {
        @Id
        @Column(name = "data")
        private String data;

        public DataTableDbo() {
        }

        public DataTableDbo(String data) {
            this.data = data;
        }

        public void setData(String data) {
            this.data = data;
        }

        public String getData() {
            return data;
        }

        @Override
        public String toString() {
            return "DataTableDbo[data=" + data + "]";
        }
    }

    @Test
    @DirtiesContext(methodMode = MethodMode.AFTER_METHOD)
    public void test_Hibernate_Good() throws SQLException {
        SpringTestDataTO dto = new SpringTestDataTO(27, GOOD);
        String traceId = "testGood_TraceId:" + RandomString.randomCorrelationId();
        sendMessage(SERVICE_HIBERNATE, dto, traceId);

        Result<SpringTestStateTO, SpringTestDataTO> result = _latch.waitForResult();
        Assert.assertEquals(traceId, result.getContext().getTraceId());
        Assert.assertEquals(new SpringTestDataTO(dto.number * 2, dto.string), result.getData());

        // :: Assert against the data from the database - it should be there!
        List<String> expected = new ArrayList<>(4);
        // Add in expected order based on "ORDER BY data"
        expected.add(TERMINATOR + '[' + GOOD + ']');
        expected.add(SERVICE_HIBERNATE + '[' + GOOD + "]-Hibernate");
        expected.add(SERVICE_HIBERNATE + '[' + GOOD + "]-PlainJdbc");
        expected.add(SERVICE_HIBERNATE + '[' + GOOD + "]-SpringJdbc");

        Assert.assertEquals(expected, getDataFromDatabase());
    }

    @Test
    @DirtiesContext(methodMode = MethodMode.AFTER_METHOD)
    public void test_Hibernate_ThrowsShouldRollback() throws SQLException {
        SpringTestDataTO dto = new SpringTestDataTO(13, THROW);
        String traceId = "testBad_TraceId:" + RandomString.randomCorrelationId();
        sendMessage(SERVICE_HIBERNATE, dto, traceId);

        // :: This should result in a DLQ, since the SERVICE throws.
        MatsTrace<String> dlqMessage = _matsLocalVmActiveMq.getDlqMessage(_matsSerializer,
                _matsFactory.getFactoryConfig().getMatsDestinationPrefix(),
                _matsFactory.getFactoryConfig().getMatsTraceKey(),
                SERVICE_HIBERNATE);
        // There should be a DLQ
        Assert.assertNotNull(dlqMessage);
        // The DTO and TraceId of the DLQ'ed message should be the one we sent.
        String data = dlqMessage.getCurrentCall().getData();
        SpringTestDataTO dtoInDlq = _matsSerializer.deserializeObject(data, SpringTestDataTO.class);
        Assert.assertEquals(dto, dtoInDlq);
        Assert.assertEquals(traceId, dlqMessage.getTraceId());

        // There should be zero rows in the database, since the RuntimeException should have rolled back processing
        // of SERVICE, and thus TERMINATOR should not have gotten a message either (and thus not inserted row).
        List<String> dataFromDatabase = getDataFromDatabase();
        Assert.assertEquals(0, dataFromDatabase.size());
    }
}