package com.stolsvik.mats.test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.jms.ConnectionFactory;
import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.util.MatsStringSerializer;

/**
 * Provides a H2 Database DataSource, and a
 * {@link JmsMatsFactory#createMatsFactory_JmsAndJdbcTransactions(com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.JmsConnectionSupplier, com.stolsvik.mats.impl.jms.JmsMatsTransactionManager_JmsAndJdbc.JdbcConnectionSupplier, MatsStringSerializer)
 * JmsAndJdbc MATS Transaction Manager}, in addition to features from {@link Rule_Mats}. This enables testing of
 * combined JMS and JDBC scenarios - in particularly used for testing of the MATS library itself (check that commits and
 * rollbacks work as expected).
 * <p>
 * Notice that the H2 Database is started with the "AUTO_SERVER=TRUE" flag that starts the
 * <a href="http://www.h2database.com/html/features.html#auto_mixed_mode">Auto Mixed Mode</a>, meaning that you can
 * connect to the same URL from a different process. This can be of value if you wonder what is in the database at any
 * point: You may do a "Thread.sleep(60000)" at some point in the code, and then connect to the same URL using e.g.
 * <a href="http://www.squirrelsql.org/">SquirrelSQL</a>, and this second H2 client will magically be able to connect to
 * the database by using a TCP socket to the original.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Rule_MatsWithDb extends Rule_Mats {

    private final JdbcDataSource _dataSource;

    {
        // Set up H2 Database
        _dataSource = new JdbcDataSource();
        _dataSource.setURL("jdbc:h2:./matsTestH2DB;AUTO_SERVER=TRUE");
        try {
            try (Connection con = _dataSource.getConnection();
                    Statement stmt = con.createStatement();) {
                stmt.execute("DROP ALL OBJECTS DELETE FILES");
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Got problems running 'DROP ALL OBJECTS DELETE FILES'.", e);
        }
    }

    @Override
    protected MatsFactory createMatsFactory(MatsStringSerializer stringSerializer,
            ConnectionFactory jmsConnectionFactory) {
        // Create the JMS and JDBC TransactionManager-backed JMS MatsFactory.
        return JmsMatsFactory.createMatsFactory_JmsAndJdbcTransactions((s) -> jmsConnectionFactory.createConnection(),
                (s) -> _dataSource.getConnection(), _matsStringSerializer);
    }

    /**
     * @return the H2 {@link DataSource} instance which transaction manager of the MatsFactory is set up with.
     */
    public DataSource getDataSource() {
        return _dataSource;
    }
}
