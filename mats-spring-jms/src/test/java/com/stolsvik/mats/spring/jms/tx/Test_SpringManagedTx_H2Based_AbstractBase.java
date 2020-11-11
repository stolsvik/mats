package com.stolsvik.mats.spring.jms.tx;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.jms.ConnectionFactory;
import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.MethodMode;
import org.springframework.test.context.junit4.SpringRunner;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;
import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.EnableMats;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.Sto;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;
import com.stolsvik.mats.util.RandomString;
import com.stolsvik.mats.util_activemq.MatsLocalVmActiveMq;

/**
 * Abstract test of Spring DB Transaction management, performing INSERTs using Spring JdbcTemplate and Plain JDBC,
 * checking commit and rollback - subclasses specifies how the MatsFactory method is created.
 *
 * @author Endre Stølsvik 2019-05-06 21:35 - http://stolsvik.com/, endre@stolsvik.com
 */
@RunWith(SpringRunner.class)
public abstract class Test_SpringManagedTx_H2Based_AbstractBase {

    private static final Logger log = LoggerFactory.getLogger(Test_SpringManagedTx_H2Based_AbstractBase.class);

    public static final String SERVICE = "mats.spring.SpringManagedTx_H2Based";
    public static final String TERMINATOR = SERVICE + ".TERMINATOR";

    public static final String H2_DATABASE_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";

    private static final int MULTIPLE_COUNT = 75;

    @Configuration
    @EnableMats
    static abstract class SpringConfiguration_AbstractBase {
        @Bean
        MatsLocalVmActiveMq createMatsTestActiveMq() {
            return MatsLocalVmActiveMq.createRandomInVmActiveMq();
        }

        @Bean
        public ConnectionFactory createJmsConnectionFactory(MatsLocalVmActiveMq matsLocalVmActiveMq) {
            return matsLocalVmActiveMq.getConnectionFactory();
        }

        @Bean
        MatsSerializer<String> createMatsSerializer() {
            return new MatsSerializer_DefaultJson();
        }

        /**
         * @return a H2 test database.
         */
        @Bean
        public DataSource createDataSource() {
            log.info("Creating H2 DataSource (url:'" + H2_DATABASE_URL
                    + "'), run 'DROP ALL OBJECTS DELETE FILES' on it,"
                    + " then 'CREATE TABLE datatable (data VARCHAR {UNIQUE})'.");

            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL(H2_DATABASE_URL);

            // DROP EVERYTHING
            String dropSql = "DROP ALL OBJECTS DELETE FILES";
            try (Connection con = dataSource.getConnection();
                    Statement stmt = con.createStatement()) {
                stmt.execute(dropSql);
            }
            catch (SQLException e) {
                throw new RuntimeException("Got problems running '" + dropSql + "'.", e);
            }

            // CREATE TABLE
            try (Connection dbCon = dataSource.getConnection();
                    Statement stmt = dbCon.createStatement();) {
                stmt.execute("CREATE TABLE datatable ( data VARCHAR NOT NULL, CONSTRAINT UC_data UNIQUE (data))");
            }
            catch (SQLException e) {
                throw new RuntimeException("Got problems creating the SQL table 'datatable'.", e);
            }

            return optionallyWrapDataSource(dataSource);
        }

        protected DataSource optionallyWrapDataSource(DataSource dataSource) {
            return dataSource;
        }

        @Inject
        private DataSource _dataSource;

        @PreDestroy
        protected void closeDownDatabase() throws SQLException {
            // Shutting down of the database turned out to be pretty important, otherwise it started complaining about
            // missing table DATATABLE in the middle of a MULTIPLE-run with 500 messages.
            Connection con = _dataSource.getConnection();
            Statement stmt = con.createStatement();
            stmt.execute("SHUTDOWN");
            con.close();
        }

        /**
         * This is just to "emulate" a proper Spring-managed JdbcTemplate; Could have made it directly in the endpoint.
         */
        @Bean
        public JdbcTemplate jdbcTemplate(DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }

        /**
         * This is just to "emulate" a proper Spring-managed SimpleJdbcInsert; Could have made it directly in the
         * endpoint.
         */
        @Bean
        public SimpleJdbcInsert simpleJdbcInsert(DataSource dataSource) {
            return new SimpleJdbcInsert(dataSource).withTableName("datatable");
        }

        @Bean
        public MatsTestLatch createTestLatch() {
            return new MatsTestLatch();
        }

        @Inject
        protected JdbcTemplate _jdbcTemplate;

        @Inject
        protected SimpleJdbcInsert _simpleJdbcInsert;

        @Inject
        protected MatsTestLatch _latch;

        /**
         * Setting up the single-stage endpoint that will store a row in the database, but which will throw if the
         * request DTO says so.
         */
        @MatsMapping(endpointId = SERVICE)
        public SpringTestDataTO springMatsSingleEndpoint(ProcessContext<SpringTestDataTO> context,
                SpringTestDataTO msg) {
            log.info("Incoming message for '" + SERVICE + "': DTO:[" + msg + "], context:\n" + context);

            // :: Perform an INSERT using Spring JDBC:
            String valueSpringJdbc = SERVICE + '[' + msg.string + "]-SpringJdbc";
            log.info("SERVICE: Inserting row in database, data='" + valueSpringJdbc + "'");
            // (using "update" to perform INSERT..)
            _jdbcTemplate.update("INSERT INTO datatable VALUES (?)", valueSpringJdbc);

            // :: Perform an INSERT using pure JDBC
            String valuePlainJdbc = SERVICE + '[' + msg.string + "]-PlainJdbc";
            // Note how we're using DataSourceUtils to get the Spring Managed Transactional Connection.
            // .. and do NOT close it afterwards, but use DataSourceUtils.releaseConnection instead
            // Notice how this is exactly like JdbcTemplate.execute() does it.
            Connection con = DataSourceUtils.getConnection(_dataSource);
            try {
                PreparedStatement stmt = con.prepareStatement("INSERT INTO datatable VALUES (?)");
                stmt.setString(1, valuePlainJdbc);
                stmt.execute();
                stmt.close();
                // NOTE: Must NOT close Connection, but can "release" it back using DataSourceUtils:
                // ("Release" does a close if outside Spring Managed TX, and does NOT close if inside a TX)
                DataSourceUtils.releaseConnection(con, _dataSource);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }

            // Assert that this is the same Connection instance that we would get from the ProcessContext
            Optional<Connection> contextAttributeConnection = context.getAttribute(Connection.class);
            Assert.assertSame(con, contextAttributeConnection.get());

            // ?: Are we instructed to throw now, thereby rolling back the above changes?
            if (msg.string.startsWith(THROW)) {
                // -> Yes, we should throw - and this should rollback all DB, eventually DLQing the message.
                log.info("Asked to throw RuntimeException, and that we do!");
                throw new RuntimeException("This RTE should make the SQL INSERT rollback!");
            }
            return new SpringTestDataTO(msg.number * 2, msg.string);
        }

        private AtomicInteger _counter = new AtomicInteger(MULTIPLE_COUNT);

        /**
         * Terminator, which also inserts a row in the database.
         */
        @MatsMapping(endpointId = TERMINATOR)
        protected void springMatsTerminatorEndpoint(ProcessContext<?> context, @Dto SpringTestDataTO msg,
                @Sto SpringTestStateTO state) {

            String value = TERMINATOR + '[' + msg.string + ']';
            log.info("TERMINATOR: Inserting row in database, data='" + value + "'");
            _simpleJdbcInsert.execute(Collections.singletonMap("data", value));

            // Make sure everything commits before resolving latch, by using doAfterCommit.

            // ?: Was this a "multiple" run?
            if (msg.string.startsWith(MULTIPLE)) {
                // -> Yes, multiple, so countdown multiple-counter until latch.
                context.doAfterCommit(() -> {
                    int thisCount = _counter.decrementAndGet();
                    if (thisCount == 0) {
                        _latch.resolve(context, state, msg);
                    }
                });
            }
            else {
                // -> No, ordinary single-test, so latch away.
                context.doAfterCommit(() -> _latch.resolve(context, state, msg));
            }
        }
    }

    @Inject
    private DataSource _dataSource;

    @Inject
    protected MatsFactory _matsFactory;

    @Inject
    protected MatsTestLatch _latch;

    @Inject
    protected MatsLocalVmActiveMq _matsLocalVmActiveMq;

    @Inject
    protected MatsSerializer<String> _matsSerializer;

    protected static final String GOOD = "Good";
    protected static final String THROW = "Throw";
    protected static final String MULTIPLE = "Multiple";

    @Test
    @DirtiesContext(methodMode = MethodMode.AFTER_METHOD)
    public void test_Good() throws SQLException {
        SpringTestDataTO dto = new SpringTestDataTO(27, GOOD);
        String traceId = "testGood_TraceId:" + RandomString.randomCorrelationId();
        sendMessage(SERVICE, dto, traceId);

        Result<SpringTestStateTO, SpringTestDataTO> result = _latch.waitForResult();
        Assert.assertEquals(traceId, result.getContext().getTraceId());
        Assert.assertEquals(new SpringTestDataTO(dto.number * 2, dto.string), result.getData());

        // :: Assert against the data from the database - it should be there!
        List<String> expected = new ArrayList<>(2);
        // Add in expected order based on "ORDER BY data"
        expected.add(TERMINATOR + '[' + GOOD + ']');
        expected.add(SERVICE + '[' + GOOD + "]-PlainJdbc");
        expected.add(SERVICE + '[' + GOOD + "]-SpringJdbc");

        Assert.assertEquals(expected, getDataFromDatabase());
    }

    @Test
    @DirtiesContext(methodMode = MethodMode.AFTER_METHOD)
    public void test_MultipleGood() throws SQLException {
        for (int i = 0; i < MULTIPLE_COUNT; i++) {
            sendMessage(SERVICE, new SpringTestDataTO(i, MULTIPLE + i), RandomString.randomCorrelationId());
        }

        // Wait for the message that counts it down to zero
        _latch.waitForResult(60_000);

        // :: Assert against the data from the database - it should be there!
        // Make expected follow order based on "ORDER BY data", by using TreeSet.
        SortedSet<String> expected = new TreeSet<>();
        for (int i = 0; i < MULTIPLE_COUNT; i++) {
            expected.add(TERMINATOR + '[' + MULTIPLE + i + "]");
            expected.add(SERVICE + '[' + MULTIPLE + i + "]-PlainJdbc");
            expected.add(SERVICE + '[' + MULTIPLE + i + "]-SpringJdbc");
        }
        Assert.assertEquals(new ArrayList<>(expected), getDataFromDatabase());
    }

    @Test
    @DirtiesContext(methodMode = MethodMode.AFTER_METHOD)
    public void test_ThrowsShouldRollback() throws SQLException {
        SpringTestDataTO dto = new SpringTestDataTO(13, THROW);
        String traceId = "testBad_TraceId:" + RandomString.randomCorrelationId();
        sendMessage(SERVICE, dto, traceId);

        // :: This should result in a DLQ, since the SERVICE throws.
        MatsTrace<String> dlqMessage = _matsLocalVmActiveMq.getDlqMessage(_matsSerializer,
                _matsFactory.getFactoryConfig().getMatsDestinationPrefix(),
                _matsFactory.getFactoryConfig().getMatsTraceKey(),
                SERVICE);
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

    protected void sendMessage(String serviceId, SpringTestDataTO dto, String traceId) {
        log.debug("Sending message: " + dto.string);
        _matsFactory.getDefaultInitiator().initiateUnchecked(init -> {
            init.traceId(traceId)
                    .from(Test_SpringManagedTx_H2Based_AbstractBase.class.getSimpleName())
                    .to(serviceId)
                    .replyTo(TERMINATOR, null)
                    .request(dto);
        });
    }

    protected List<String> getDataFromDatabase() throws SQLException {
        Connection connection = _dataSource.getConnection();
        PreparedStatement pstmt = connection.prepareStatement("SELECT * FROM datatable ORDER BY data");
        ResultSet rs = pstmt.executeQuery();
        List<String> actual = new ArrayList<>(2);
        while (rs.next()) {
            String string = rs.getString(1);
            actual.add(string);
        }
        return actual;
    }
}
