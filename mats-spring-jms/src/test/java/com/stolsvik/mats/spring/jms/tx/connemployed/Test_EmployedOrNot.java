package com.stolsvik.mats.spring.jms.tx.connemployed;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.InfrastructureProxy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsFactory.ContextLocal;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;
import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.jms.tx.JmsMatsTransactionManager_JmsAndSpringManagedSqlTx;
import com.stolsvik.mats.spring.jms.tx.SpringTestDataTO;
import com.stolsvik.mats.spring.test.MatsTestContext;
import com.stolsvik.mats.test.MatsTestHelp;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.TestH2DataSource;

/**
 * Checks whether the "employed" flag works in simple case.
 *
 * @author Endre Stølsvik 2021-01-18 21:35 - http://stolsvik.com/, endre@stolsvik.com
 */
@RunWith(SpringRunner.class)
@MatsTestContext
public class Test_EmployedOrNot {
    private static final Logger log = MatsTestHelp.getClassLogger();

    public static final String TERMINATOR_NO_DS_ACCESS = "test.Terminator.noDsAccess";

    @Configuration
    @EnableTransactionManagement
    public static class TestConfiguration {
        /**
         * Note: The DataSource that will be put in the Spring context is actually an ordinary DataSource (no magic,
         * except the Mats Test tooling). Specifically, it is NOT proxied with Mats' "magic lazy proxy".
         */
        @Bean
        TestH2DataSource testH2DataSource() {
            TestH2DataSource standard = TestH2DataSource.createStandard();
            standard.createDataTable();
            return standard;
        }

        /**
         * Note: The DataSource given to the {@link DataSourceTransactionManager} is proxied with Mats' "magic lazy
         * proxy".
         */
        @Bean
        PlatformTransactionManager createDataSourceTransactionaManager(DataSource dataSource) {
            // NOTICE! THIS IS IMPORTANT! We wrap the DataSource with the "magic lazy proxy" of Mats.
            DataSource magicLazyProxied = JmsMatsTransactionManager_JmsAndSpringManagedSqlTx
                    .wrapLazyConnectionDatasource(dataSource);
            // .. and use this when we create the DSTM which Mats and Spring tools will get.
            return new DataSourceTransactionManager(magicLazyProxied);
        }

        @Bean
        JdbcTemplate jdbcTemplate(DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }

        @Bean
        SimpleJdbcInsert simpleJdbcInsert(DataSource dataSource) {
            return new SimpleJdbcInsert(dataSource).withTableName("datatable");
        }

        @Inject
        private DataSource _dataSource;

        @Inject
        private MatsFactory _matsFactory;

        @Inject
        private MatsTestLatch _latch;

        @MatsMapping(TERMINATOR_NO_DS_ACCESS)
        void matsTerminator(ProcessContext<Void> context, @Dto SpringTestDataTO msg) {

            // :: ProcessContext and MatsInitiate

            // Assert that we DO have ProcessContext in ContextLocal
            @SuppressWarnings("rawtypes")
            Optional<ProcessContext> processContextOptional = ContextLocal.getAttribute(ProcessContext.class);
            Assert.assertTrue("ProcessContext SHALL be be present in ContextLocal", processContextOptional.isPresent());

            // Assert that we do NOT have MatsInitiate in ContextLocal
            Optional<MatsInitiate> matsInitiateOptional = ContextLocal.getAttribute(MatsInitiate.class);
            Assert.assertFalse("MatsInitiate shall NOT be present in ContextLocal", matsInitiateOptional.isPresent());

            // :: SQL Connections

            // We have not employed the SQL Connection yet
            Supplier<Boolean> connectionEmployed_outside = getConnectionEmployedStateSupplierFromContextLocal();
            Assert.assertFalse(connectionEmployed_outside.get());

            // Get the SQL Connection via ProcessContext
            Optional<Connection> optional = context.getAttribute(Connection.class);
            if (!optional.isPresent()) {
                throw new AssertionError("Missing context.getAttribute(Connection.class).");
            }
            Connection connectionFromContextAttribute_outside = optional.get();
            // .. it should be a proxy
            Assert.assertTrue(connectionIsLazyProxy(connectionFromContextAttribute_outside));
            log.info(TERMINATOR_NO_DS_ACCESS + ": context.getAttribute(Connection.class): "
                    + connectionFromContextAttribute_outside);
            // .. but still not /employed/
            Assert.assertFalse(connectionEmployed_outside.get());

            // Get the SQL Connection through DataSourceUtils
            Connection connectionFromDataSourceUtils_outside = DataSourceUtils.getConnection(_dataSource);
            // .. it should be a proxy
            Assert.assertTrue(connectionIsLazyProxy(connectionFromDataSourceUtils_outside));
            // .. AND it should be the same Connection (instance/object) as from ProcessContext
            Assert.assertSame(connectionFromContextAttribute_outside, connectionFromDataSourceUtils_outside);
            // .. and still not /employed/
            Assert.assertFalse(connectionEmployed_outside.get());

            // ::: Initiates

            // :: Using DefaultInitiator, do an initiate
            // NOTE! NOTHING should have changed when doing this, as it is LITERALLY just the same as doing
            // processContext.initiate(init ->...)
            _matsFactory.getDefaultInitiator().initiateUnchecked(init -> {

                // :: ProcessContext and MatsInitiate

                // Assert that we DO have ProcessContext in ContextLocal
                @SuppressWarnings("rawtypes")
                Optional<ProcessContext> processContextOptional_defaultInit = ContextLocal.getAttribute(
                        ProcessContext.class);
                Assert.assertTrue("ProcessContext SHALL be be present in ContextLocal",
                        processContextOptional_defaultInit.isPresent());

                // Assert that we do NOT have MatsInitiate in ContextLocal (deficiency per now, 2020-01-19)
                // Point is: We're within a Stage, and "riding" on the ProcessContext's MatsInitiate.
                Optional<MatsInitiate> matsInitiateOptional_defaultInit = ContextLocal.getAttribute(MatsInitiate.class);
                Assert.assertFalse("MatsInitiate shall NOT be present in ContextLocal", matsInitiateOptional_defaultInit
                        .isPresent());

                // :: SQL Connections

                // We have not employed the SQL Connection yet
                Supplier<Boolean> connectionEmployed_defaultInit = getConnectionEmployedStateSupplierFromContextLocal();
                Assert.assertFalse(connectionEmployed_defaultInit.get());

                // :: Get the SQL Connection via ProcessContext
                Optional<Connection> optionalX = context.getAttribute(Connection.class);
                if (!optionalX.isPresent()) {
                    throw new AssertionError("Missing context.getAttribute(Connection.class).");
                }
                Connection connectionFromContextAttribute_defaultInit = optionalX.get();
                // .. it should be a proxy
                Assert.assertTrue(connectionIsLazyProxy(connectionFromContextAttribute_defaultInit));
                log.info(TERMINATOR_NO_DS_ACCESS + ": context.getAttribute(Connection.class): "
                        + connectionFromContextAttribute_defaultInit);
                // .. but still not /employed/
                Assert.assertFalse(connectionEmployed_defaultInit.get());

                // :: Get the SQL Connection through DataSourceUtils
                Connection connectionFromDataSourceUtils_defaultInit = DataSourceUtils.getConnection(_dataSource);
                // .. it should be a proxy
                Assert.assertTrue(connectionIsLazyProxy(connectionFromDataSourceUtils_defaultInit));
                // .. AND it should be the same Connection (instance/object) as from ProcessContext
                Assert.assertSame(connectionFromContextAttribute_defaultInit,
                        connectionFromDataSourceUtils_defaultInit);
                // .. and still not /employed/
                Assert.assertFalse(connectionEmployed_defaultInit.get());

                // BIG POINT:
                // Assert that this Connection IS THE SAME as the one on the outside
                Assert.assertSame(connectionFromDataSourceUtils_outside, connectionFromDataSourceUtils_defaultInit);
            });

            // :: Using a non-default initiator, do an initiate
            // NOTE! Pretty much everything should have changed when doing this, as it is very much the same as
            // standing completely on the outside, doing an initiation (the current implementation is actually that
            // it runs in a separate Thread!)
            _matsFactory.getOrCreateInitiator("sub" + Math.random()).initiateUnchecked(init -> {

                // :: ProcessContext and MatsInitiate

                log.info("This should be run on a different thread!");

                // Assert that we DO have ProcessContext in ContextLocal
                // Point is: We're NOT within a Stage now!
                @SuppressWarnings("rawtypes")
                Optional<ProcessContext> processContextOptional_separateInit = ContextLocal.getAttribute(
                        ProcessContext.class);
                Assert.assertFalse("ProcessContext shall NOT be be present in ContextLocal",
                        processContextOptional_separateInit.isPresent());

                // Assert that we DO have MatsInitiate in ContextLocal
                // Point is: We're within a MatsInitiate
                Optional<MatsInitiate> matsInitiateOptional_separateInit = ContextLocal.getAttribute(
                        MatsInitiate.class);
                Assert.assertTrue("MatsInitiate SHALL be present in ContextLocal", matsInitiateOptional_separateInit
                        .isPresent());

                // :: SQL Connections

                // We have not employed the SQL Connection yet
                Supplier<Boolean> connectionEmployed_separateInit = getConnectionEmployedStateSupplierFromContextLocal();
                Assert.assertFalse(connectionEmployed_separateInit.get());

                // Get the SQL Connection via ProcessContext
                Optional<Connection> optionalX = context.getAttribute(Connection.class);
                if (!optionalX.isPresent()) {
                    throw new AssertionError("Missing context.getAttribute(Connection.class).");
                }
                Connection connectionFromContextAttribute_separateInit = optionalX.get();
                // .. it should be a proxy
                Assert.assertTrue(connectionIsLazyProxy(connectionFromContextAttribute_separateInit));
                log.info(TERMINATOR_NO_DS_ACCESS + ": context.getAttribute(Connection.class): "
                        + connectionFromContextAttribute_separateInit);
                // .. but still not /employed/
                Assert.assertFalse(connectionEmployed_separateInit.get());

                // Get the SQL Connection through DataSourceUtils
                Connection connectionFromDataSourceUtils_separateInit = DataSourceUtils.getConnection(_dataSource);
                // .. it should be a proxy
                Assert.assertTrue(connectionIsLazyProxy(connectionFromDataSourceUtils_separateInit));
                // .. AND it should be the same Connection (instance/object) as from ProcessContext
                Assert.assertSame(connectionFromContextAttribute_separateInit,
                        connectionFromDataSourceUtils_separateInit);
                // .. and still not /employed/
                Assert.assertFalse(connectionEmployed_separateInit.get());

                // BIG POINT:
                // Assert that this Connection is *NOT* THE SAME as the one on the outside
                Assert.assertNotSame(connectionFromDataSourceUtils_outside, connectionFromDataSourceUtils_separateInit);
            });

            _latch.resolve(null, msg);
        }

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    static Supplier<Boolean> getConnectionEmployedStateSupplierFromContextLocal() {
        Optional<Supplier> attribute = ContextLocal.getAttribute(Supplier.class,
                JmsMatsTransactionManager_JmsAndSpringManagedSqlTx.CONTEXT_LOCAL_KEY_CONNECTION_EMPLOYED_STATE_SUPPLIER);
        if (!attribute.isPresent()) {
            throw new AssertionError("Missing in ContextLocal: CONTEXT_LOCAL_KEY_CONNECTION_EMPLOYED_STATE_SUPPLIER");
        }
        return attribute.get();
    }

    private static boolean connectionIsLazyProxy(DataSource dataSource) {
        Connection connection = DataSourceUtils.getConnection(dataSource);
        boolean isProxy = connectionIsLazyProxy(connection);
        DataSourceUtils.releaseConnection(connection, dataSource);
        return isProxy;
    }

    private static boolean connectionIsLazyProxy(Connection connection) {
        String connectionToString = connection.toString();
        boolean isProxy = connectionToString.startsWith("Lazy Connection proxy");
        log.info("CONNECTION-CHECK: Connection is " + (isProxy ? "PROXY" : "NOT proxy") + "! conn:"
                + connectionToString);
        return isProxy;
    }

    @Inject
    private MatsTestLatch _latch;

    @Inject
    private TestService _testService;

    @Inject
    private TestH2DataSource _testH2DataSource;

    @Inject
    private MatsFactory _matsFactory;

    @Test
    public void noDataSourceUse() {
        _matsFactory.getDefaultInitiator().initiateUnchecked(init -> init.traceId(MatsTestHelp.traceId())
                .from(MatsTestHelp.from("noDataSourceUse"))
                .to(TERMINATOR_NO_DS_ACCESS)
                .send(new SpringTestDataTO()));
        _latch.waitForResult();
    }

    // ===== Testing basic Spring transactional infrastructure in conjunction with the proxying. =====

    @Configuration
    @Service
    public static class TestService {
        @Inject
        private SimpleJdbcInsert _simpleJdbcInsert;

        @Inject
        private DataSource _dataSource;

        /**
         * This is not {@literal @Transactional}, and will thus NOT use the DSTM in Spring, and thus will get its
         * connection directly from the DataSource in the Spring context, which is not proxied with the "magic lazy
         * proxy".
         */
        public void insertRowNonTransactional() {
            // This is no magic lazy proxy.
            Assert.assertFalse(connectionIsLazyProxy(_dataSource));
            _simpleJdbcInsert.execute(Collections.singletonMap("data", "insertRowNonTransactional"));
        }

        /**
         * This is not {@literal @Transactional}, and will thus NOT use the DSTM in Spring, and thus will get its
         * connection directly from the DataSource in the Spring context, which is not proxied with the "magic lazy
         * proxy".
         */
        public void insertRowNonTransactionalThrow() {
            // This is no magic lazy proxy.
            Assert.assertFalse(connectionIsLazyProxy(_dataSource));
            _simpleJdbcInsert.execute(Collections.singletonMap("data", "insertRowNonTransactionalThrow"));
            throw new RuntimeException();
        }

        /**
         * This method is {@literal @Transactional}, and will therefore be managed by the DSTM in Spring. Even though we
         * here employ the DataSource residing in Spring context - exactly as with {@link #insertRowNonTransactional()}
         * - it will still pick up the DataSource contained in the DSTM, which is proxied with Mats' "magic lazy proxy".
         * (This happens because the proxy implements the {@link InfrastructureProxy}, and the "equals" logic of
         * Spring's DataSource resolution thus realizes that what we <i>really</i> want, is a Connection from the
         * "equal" DataSource in the DSTM, which is the "magic lazy proxy").
         */
        @Transactional
        public void insertRowTransactional() {
            // ::This actually ends up being a "magic lazy proxy", even though we supply the DataSource directly from
            // the Spring context, which is not proxied.
            // - At this point the lazy proxy has not gotten the connection yet.
            Assert.assertTrue(connectionIsLazyProxy(_dataSource));
            _simpleJdbcInsert.execute(Collections.singletonMap("data", "insertRowTransactional"));
            // - While at this point the lazy proxy HAS gotten the connection!
            Assert.assertFalse(connectionIsLazyProxy(_dataSource));
        }

        /**
         * Read JavaDoc at {@link #insertRowTransactional()}.
         *
         * <b>This should rollback, and thus not insert the row anyway</b>
         */
        @Transactional
        public void insertRowTransactionalThrow() {
            // ::This actually ends up being a "magic lazy proxy", even though we supply the DataSource directly from
            // the Spring context, which is not proxied.
            // - At this point the lazy proxy has not gotten the connection yet.
            Assert.assertTrue(connectionIsLazyProxy(_dataSource));
            _simpleJdbcInsert.execute(Collections.singletonMap("data", "insertRowTransactionalThrow"));
            // - While at this point the lazy proxy HAS gotten the connection!
            Assert.assertFalse(connectionIsLazyProxy(_dataSource));
            throw new RuntimeException();
        }
    }

    @Test
    public void testBasicSpringTransactionalLogic() {
        // :: Non-transactional
        // Standard insert
        _testService.insertRowNonTransactional();
        try {
            // Since this isn't transactional, the insert will have happened before the throw.
            _testService.insertRowNonTransactionalThrow();
        }
        catch (RuntimeException e) {
            /* no-op */
        }

        // :: Transactional
        // Standard insert
        _testService.insertRowTransactional();
        try {
            // This should rollback, and thus the insert will "not have happened".
            _testService.insertRowTransactionalThrow();
        }
        catch (RuntimeException e) {
            /* no-op */
        }

        List<String> dataFromDataTable = _testH2DataSource.getDataFromDataTable();

        // NOTICE!! The "insertRowTransactionalThrow" should NOT be here, as it threw, and Spring will then rollback.
        Assert.assertEquals(
                Arrays.asList("insertRowNonTransactional", "insertRowNonTransactionalThrow", "insertRowTransactional"),
                dataFromDataTable);
    }
}
