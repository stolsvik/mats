package com.stolsvik.mats.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.h2.jdbcx.JdbcDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A H2 DataBase DataSource which has a couple of extra methods which simplifies testing, in particular the
 * {@link #cleanDatabase()}, and {@link #createDataTable()} method with associated convenience methods for storing and
 * getting simple values.
 */
public class TestH2DataSource extends JdbcDataSource {
    private static final Logger log = LoggerFactory.getLogger(TestH2DataSource.class);

    /**
     * System property ("-D" jvm argument) that if set will change the method {@link #createStandard()} from returning
     * an in-memory H2 DataSource, to instead return a DataSource using the URL from the value, with the special case
     * that if the value is "{@link #SYSPROP_VALUE_FILE_BASED file}", it will be
     * <code>"jdbc:h2:./matsTestH2DB;AUTO_SERVER=TRUE"</code>.
     * <p>
     * Value is {@code "mats.test.activemq"}
     */
    public static final String SYSPROP_MATS_TEST_H2 = "mats.test.h2";

    /**
     * If the value of {@link #SYSPROP_MATS_TEST_H2} is this value, the {@link #createStandard()} will use the URL
     * {@link #FILE_BASED_TEST_H2_DATABASE_URL}, which is <code>"jdbc:h2:./matsTestH2DB;AUTO_SERVER=TRUE"</code>.
     * <p>
     * Value is {@code "file"}
     */
    public static final String SYSPROP_VALUE_FILE_BASED = "file";

    public static final String FILE_BASED_TEST_H2_DATABASE_URL = "jdbc:h2:./matsTestH2DB;AUTO_SERVER=TRUE";
    public static final String IN_MEMORY_TEST_H2_DATABASE_URL = "jdbc:h2:mem:matsTestH2DB;DB_CLOSE_DELAY=-1";

    /**
     * Creates an in-memory {@link TestH2DataSource} as specified by the URL {@link #IN_MEMORY_TEST_H2_DATABASE_URL},
     * which is <code>"jdbc:h2:mem:matsTestH2DB"</code>, <b>unless</b> the System Property {@link #SYSPROP_MATS_TEST_H2}
     * (<code>"mats.test.h2"</code>) is directly set to a different URL to use, with the special case that if it is
     * {@link #SYSPROP_VALUE_FILE_BASED} (<code>"file"</code>), in which case {@link #FILE_BASED_TEST_H2_DATABASE_URL}
     * (<code>"jdbc:h2:./matsTestH2DB;AUTO_SERVER=TRUE</code>) is used as URL.
     * <p />
     * <b>Notice that the normal way to use the connected to database is to invoke {@link #cleanDatabase()} for each
     * test, which implies that the database will be utterly wiped!</b>
     *
     * @return the created {@link TestH2DataSource}.
     */
    public static TestH2DataSource createStandard() {
        String sysprop_matsTestH2 = System.getProperty(SYSPROP_MATS_TEST_H2);
        // ?: Was it set?
        if (sysprop_matsTestH2 == null) {
            // -> No, not set - so return normal in-memory database
            return createInMemory();
        }
        // E-> The System Property was set

        // ?: Was it the special "file" value?
        if (sysprop_matsTestH2.equalsIgnoreCase(SYSPROP_VALUE_FILE_BASED)) {
            // -> Yes, special "file" value
            return createFileBased();
        }

        // E-> No, not special value, so treat it as a URL directly
        return create(sysprop_matsTestH2);
    }

    /**
     * Creates a {@link TestH2DataSource} using the URL {@link #FILE_BASED_TEST_H2_DATABASE_URL}, which is
     * <code>"jdbc:h2:./matsTestH2DB;AUTO_SERVER=TRUE"</code>.
     *
     * @return the created {@link TestH2DataSource}.
     */
    public static TestH2DataSource createFileBased() {
        return create(FILE_BASED_TEST_H2_DATABASE_URL);
    }

    /**
     * Creates a {@link TestH2DataSource} using the URL {@link #IN_MEMORY_TEST_H2_DATABASE_URL}, which is
     * <code>"jdbc:h2:mem:matsTestH2DB"</code>.
     *
     * @return the created {@link TestH2DataSource}.
     */
    public static TestH2DataSource createInMemory() {
        return create(IN_MEMORY_TEST_H2_DATABASE_URL);
    }

    /**
     * Creates a {@link TestH2DataSource} using the supplied URL.
     *
     * @return the created {@link TestH2DataSource}.
     */
    public static TestH2DataSource create(String url) {
        log.info("Creating TestH2DataSource with URL [" + url + "].");
        TestH2DataSource dataSource = new TestH2DataSource();
        dataSource.setURL(url);
        dataSource.cleanDatabase();
        return dataSource;
    }

    /**
     * Cleans the test database: Runs SQL <code>"DROP ALL OBJECTS DELETE FILES"</code>.
     */
    public void cleanDatabase() {
        cleanDatabase(false);
    }

    /**
     * Cleans the test database: Runs SQL <code>"DROP ALL OBJECTS DELETE FILES"</code>, and optionally invokes
     * {@link #createDataTable()}.
     * 
     * @param createDataTable
     *            whether to invoke {@link #createDataTable()} afterwards.
     */
    public void cleanDatabase(boolean createDataTable) {
        String dropSql = "DROP ALL OBJECTS DELETE FILES";
        try (Connection con = this.getConnection();
                Statement stmt = con.createStatement()) {
            stmt.execute(dropSql);
        }
        catch (SQLException e) {
            throw new TestH2DataSourceException("Got problems cleaning database by running '" + dropSql + "'.", e);
        }
        if (createDataTable) {
            createDataTable();
        }
    }

    /**
     * Creates a test "datatable", runs SQL <code>"CREATE TABLE datatable ( data VARCHAR )"</code>.
     */
    public void createDataTable() {
        try {
            try (Connection dbCon = this.getConnection();
                    Statement stmt = dbCon.createStatement();) {
                stmt.execute("CREATE TABLE datatable (data VARCHAR NOT NULL, CONSTRAINT UC_data UNIQUE (data))");
            }
        }
        catch (SQLException e) {
            throw new TestH2DataSourceException("Got problems creating the SQL 'datatable'.", e);
        }
    }

    /**
     * Inserts the provided 'data' into the SQL Table 'datatable', using a SQL Connection from <code>this</code>
     * DataSource.
     *
     * @param data
     *            the data to insert.
     */
    public void insertDataIntoDataTable(String data) {
        try (Connection con = this.getConnection()) {
            insertDataIntoDataTable(con, data);
        }
        catch (SQLException e) {
            throw new TestH2DataSourceException("Got problems getting SQL Connection.", e);
        }
    }

    /**
     * Inserts the provided 'data' into the SQL Table 'datatable', using the provided SQL Connection.
     *
     * @param sqlConnection
     *            the SQL Connection to use to insert data.
     * @param data
     *            the data to insert.
     */
    public static void insertDataIntoDataTable(Connection sqlConnection, String data) {
        // :: Populate the SQL table with a piece of data
        try {
            PreparedStatement pStmt = sqlConnection.prepareStatement("INSERT INTO datatable VALUES (?)");
            pStmt.setString(1, data);
            pStmt.execute();
            pStmt.close();
        }
        catch (SQLException e) {
            throw new TestH2DataSourceException("Got problems fetching column 'data' from SQL Table 'datatable'.", e);
        }
    }

    /**
     * @return all rows in the 'datatable' (probably created by {@link #createDataTable()}, using a SQL Connection from
     *         <code>this</code> DataSource.
     */
    public List<String> getDataFromDataTable() {
        try (Connection con = this.getConnection()) {
            return getDataFromDataTable(con);
        }
        catch (SQLException e) {
            throw new TestH2DataSourceException("Got problems getting SQL Connection.", e);
        }
    }

    /**
     * @param sqlConnection
     *            the SQL Connection to use to fetch data.
     * @return all rows in the 'datatable' (probably created by {@link #createDataTable()}, using the provided SQL
     *         Connection - the SQL is <code>"SELECT data FROM datatable ORDER BY data"</code>.
     */
    public static List<String> getDataFromDataTable(Connection sqlConnection) {
        try {
            Statement stmt = sqlConnection.createStatement();
            List<String> ret = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery("SELECT data FROM datatable ORDER BY data")) {
                while (rs.next()) {
                    ret.add(rs.getString(1));
                }
            }
            stmt.close();
            return ret;
        }
        catch (SQLException e) {
            throw new TestH2DataSourceException("Got problems fetching column 'data' from SQL Table 'datatable'.", e);
        }
    }

    /**
     * A {@link RuntimeException} for use in database access methods and tests.
     */
    public static class TestH2DataSourceException extends RuntimeException {
        public TestH2DataSourceException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Closes the database, <b>note:</b> will be picked up by Spring if available as a Bean.
     */
    public void close() {
        // log.info("Shutting down TestH2DataSource - NOTE: Temporarily not operational (i.e. no change from )!");
        
        // NOTE 2020-01-20, endre: This resulted in some of these exceptions when Spring tried to destroy this bean.
        // I am not sure why. It might seem like the shutdown was done asynchronously, with a different thread.
        // It might be an idea to use different named in-mem H2 instances. In that case, make sure it is NOT shut
        // down if using the file-based.
        /**
         * <pre>
         * java.lang.AssertionError: Problems shutting down H2
         *         at com.stolsvik.mats.test.TestH2DataSource.close(TestH2DataSource.java:246)
         *         at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
         *         at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
         *         at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
         *         at java.lang.reflect.Method.invoke(Method.java:498)
         *         at org.springframework.beans.factory.support.DisposableBeanAdapter.invokeCustomDestroyMethod(DisposableBeanAdapter.java:364)
         *         at org.springframework.beans.factory.support.DisposableBeanAdapter.destroy(DisposableBeanAdapter.java:287)
         *         at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.destroyBean(DefaultSingletonBeanRegistry.java:583)
         *         at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.destroySingleton(DefaultSingletonBeanRegistry.java:555)
         *         at org.springframework.beans.factory.support.DefaultListableBeanFactory.destroySingleton(DefaultListableBeanFactory.java:957)
         *         at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.destroySingletons(DefaultSingletonBeanRegistry.java:516)
         *         at org.springframework.beans.factory.support.DefaultListableBeanFactory.destroySingletons(DefaultListableBeanFactory.java:964)
         *         at org.springframework.context.support.AbstractApplicationContext.destroyBeans(AbstractApplicationContext.java:1034)
         *         at org.springframework.context.support.AbstractApplicationContext.doClose(AbstractApplicationContext.java:1009)
         *         at org.springframework.context.support.AbstractApplicationContext$2.run(AbstractApplicationContext.java:929)
         * Caused by: org.h2.jdbc.JdbcSQLNonTransientConnectionException: The database is open in exclusive mode; can not open additional connections [90135-200]
         *         at org.h2.message.DbException.getJdbcSQLException(DbException.java:622)
         *         at org.h2.message.DbException.getJdbcSQLException(DbException.java:429)
         *         at org.h2.message.DbException.get(DbException.java:205)
         *         at org.h2.message.DbException.get(DbException.java:181)
         *         at org.h2.message.DbException.get(DbException.java:170)
         *         at org.h2.engine.Database.createSession(Database.java:1278)
         *         at org.h2.engine.Engine.openSession(Engine.java:140)
         *         at org.h2.engine.Engine.openSession(Engine.java:192)
         *         at org.h2.engine.Engine.createSessionAndValidate(Engine.java:171)
         *         at org.h2.engine.Engine.createSession(Engine.java:166)
         *         at org.h2.engine.Engine.createSession(Engine.java:29)
         *         at org.h2.engine.SessionRemote.connectEmbeddedOrServer(SessionRemote.java:340)
         *         at org.h2.jdbc.JdbcConnection.<init>(JdbcConnection.java:173)
         *         at org.h2.jdbc.JdbcConnection.<init>(JdbcConnection.java:152)
         *         at org.h2.Driver.connect(Driver.java:69)
         *         at org.h2.jdbcx.JdbcDataSource.getJdbcConnection(JdbcDataSource.java:189)
         *         at org.h2.jdbcx.JdbcDataSource.getConnection(JdbcDataSource.java:160)
         *         at com.stolsvik.mats.test.TestH2DataSource.close(TestH2DataSource.java:239)
         * </pre>
         */

        // try {
        // Connection con = getConnection();
        // Statement stmt = con.createStatement();
        // stmt.execute("SHUTDOWN");
        // stmt.close();
        // con.close();
        // }
        // catch (SQLException e) {
        // throw new AssertionError("Problems shutting down H2", e);
        // }
    }
}
