package com.stolsvik.mats.impl.jms;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler.JmsSessionHolder;
import com.stolsvik.mats.util.MatsTxSqlConnection;
import com.stolsvik.mats.util.MatsTxSqlConnection.MatsSqlConnectionCreationException;

/**
 * Implementation of {@link JmsMatsTransactionManager} that in addition to the JMS transaction also handles a JDBC SQL
 * {@link Connection} (using only pure java, i.e. no Spring) for which it keeps transaction demarcation along with the
 * JMS transaction, by means of <i>"Best Effort 1 Phase Commit"</i>:
 * <ol>
 * <li><b>JMS transaction is entered</b> (a transactional JMS Connection is always within a transaction)
 * <li>JMS Message is retrieved.
 * <li><b>SQL transaction is entered</b>
 * <li>Code is executed, including SQL statements.
 * <li><b>SQL transaction is committed - <font color="red">Any errors also rollbacks the JMS Transaction, so that none
 * of them have happened.</font></b>
 * <li><b>JMS transaction is committed.</b>
 * </ol>
 * Out of that order, one can see that if SQL transaction becomes committed, and then the JMS transaction fails, this
 * will be a pretty bad situation. However, of those two transactions, the SQL transaction is absolutely most likely to
 * fail, as this is where you can have business logic failures, concurrency problems (e.g. MS SQL's "Deadlock Victim"),
 * integrity constraints failing etc - that is, failures in both logic and timing. On the other hand, the JMS
 * transaction (which effectively boils down to <i>"yes, I received this message"</i>) is much harder to fail, where the
 * only situation where it can fail is due to infrastructure/hardware failures (exploding server / full disk on Message
 * Broker). This is called "Best Effort 1PC", and is nicely explained in <a href=
 * "http://www.javaworld.com/article/2077963/open-source-tools/distributed-transactions-in-spring--with-and-without-xa.html?page=2">
 * this article</a>. If this failure occurs, it will be caught and logged on ERROR level (by
 * {@link JmsMatsTransactionManager_JmsOnly}) - and then the Message Broker will probably try to redeliver the message.
 * Also read the <a href="http://activemq.apache.org/should-i-use-xa.html">Should I use XA Transactions</a> from Apache
 * Active MQ.
 * <p>
 * Wise tip when working with <i>Message Oriented Middleware</i>: Code idempotent! Handle double-deliveries!
 * <p>
 * The transactionally demarcated SQL Connection can be retrieved from the {@link MatsTxSqlConnection} utility class.
 * <p>
 * It requires a {@link JdbcConnectionSupplier} upon construction. The {@link JdbcConnectionSupplier} will be asked for
 * a SQL Connection in any MatsStage's StageProcessor that requires it: The fetching of the SQL Connection is lazy in
 * that it won't be retrieved (nor entered into transaction with), until it is actually requested by the user code by
 * means of {@link MatsTxSqlConnection#getConnection()}.
 * <p>
 * The SQL Connection will be {@link Connection#close() closed} after each stage processing (after each transaction,
 * either committed or rollbacked) - if it was requested during the user code.
 * <p>
 * This implementation will not perform any Connection reuse (caching/pooling). It is up to the supplier to implement
 * any pooling, or make use of a pooled DataSource, if so desired. (Which definitely should be desired, due to the heavy
 * use of <i>"get new - use - commit/rollback - close"</i>.)
 *
 * @author Endre Stølsvik - 2015-12-06 - http://endre.stolsvik.com
 */
public class JmsMatsTransactionManager_JmsAndJdbc extends JmsMatsTransactionManager_JmsOnly {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsTransactionManager_JmsAndJdbc.class);

    /**
     * Abstracts away JDBC Connection generation - useful if you need to provide any Connection parameters, or set some
     * Connection properties, for example set the {@link java.sql.Connection#setTransactionIsolation(int) Transaction
     * Isolation Level}.
     * <p>
     * Otherwise, the lambda can be as simple as <code>(txContextKey) -> dataSource.getConnection()</code>.
     */
    @FunctionalInterface
    public interface JdbcConnectionSupplier {
        Connection createJdbcConnection(JmsMatsTxContextKey txContextKey) throws SQLException;
    }

    private final JdbcConnectionSupplier _jdbcConnectionSupplier;

    public static JmsMatsTransactionManager_JmsAndJdbc create(JdbcConnectionSupplier jdbcConnectionSupplier) {
        return new JmsMatsTransactionManager_JmsAndJdbc(jdbcConnectionSupplier);
    }

    protected JmsMatsTransactionManager_JmsAndJdbc(JdbcConnectionSupplier jdbcConnectionSupplier) {
        super();
        _jdbcConnectionSupplier = jdbcConnectionSupplier;
    }

    @Override
    public TransactionContext getTransactionContext(JmsMatsTxContextKey txContextKey) {
        return new TransactionalContext_JmsAndJdbc(_jdbcConnectionSupplier, txContextKey);
    }

    /**
     * The {@link JmsMatsTransactionManager.TransactionContext} implementation for
     * {@link JmsMatsTransactionManager_JmsOnly}.
     */
    public static class TransactionalContext_JmsAndJdbc extends TransactionalContext_JmsOnly {
        private final JdbcConnectionSupplier _jdbcConnectionSupplier;

        public TransactionalContext_JmsAndJdbc(
                JdbcConnectionSupplier jdbcConnectionSupplier, JmsMatsTxContextKey txContextKey) {
            super(txContextKey);
            _jdbcConnectionSupplier = jdbcConnectionSupplier;
        }

        @Override
        public void doTransaction(JmsSessionHolder jmsSessionHolder, ProcessingLambda lambda)
                throws JmsMatsJmsException {
            // :: First make the potential Connection available
            LazyJdbcConnectionSupplier lazyConnectionSupplier = new LazyJdbcConnectionSupplier();
            MatsTxSqlConnection.setThreadLocalConnectionSupplier(lazyConnectionSupplier);

            // :: We invoke the "outer" transaction, which is the JMS transaction.
            super.doTransaction(jmsSessionHolder, () -> {
                // ----- We're *within* the JMS Transaction demarcation.

                /*
                 * NOTICE: We will not get the SQL Connection and set AutoCommit to false /here/ (i.e. start the
                 * transaction), as that will be done implicitly by the user code IFF it actually fetches the SQL
                 * Connection.
                 *
                 * ----- Therefore, we're now IMPLICITLY *within* the SQL Transaction demarcation.
                 */

                try {
                    log.debug(LOG_PREFIX + "About to run ProcessingLambda for " + stageOrInit(_txContextKey)
                            + ", within JDBC SQL Transactional demarcation.");
                    /*
                     * Invoking the provided ProcessingLambda, which typically will be the actual user code (albeit
                     * wrapped with some minor code from the JmsMatsStage to parse the MapMessage, deserialize the
                     * MatsTrace, and fetch the state etc.), which will now be inside both the inner (implicit) SQL
                     * Transaction demarcation, and the outer JMS Transaction demarcation.
                     */
                    lambda.performWithinTransaction();
                }
                // Catch EVERYTHING that can come out of the try-block:
                catch (MatsRefuseMessageException | RuntimeException | Error e) {
                    // ----- The user code had some error occur, or want to reject this message.
                    // !!NOTE!!: The full Exception will be logged by outside JMS-trans class on JMS rollback handling.
                    log.error(LOG_PREFIX + "ROLLBACK SQL: " + e.getClass().getSimpleName() + " while processing "
                            + stageOrInit(_txContextKey) + " (most probably from user code)."
                            + " Rolling back the SQL Connection.");
                    /*
                     * IFF the SQL Connection was fetched, we will now rollback (and close) it.
                     */
                    commitOrRollbackThenCloseConnection(false, lazyConnectionSupplier.getAnyGottenConnection());

                    // ----- We're *outside* the SQL Transaction demarcation (rolled back).

                    // We will now throw on the Exception, which will rollback the JMS Transaction.
                    throw e;
                }
                catch (Throwable t) {
                    // ----- This must have been a "sneaky throws"; Throwing an undeclared checked exception.
                    // !!NOTE!!: The full Exception will be logged by outside JMS-trans class on JMS rollback handling.
                    log.error(LOG_PREFIX + "ROLLBACK SQL: Got an undeclared checked exception " + t.getClass()
                            .getSimpleName() + " while processing " + stageOrInit(_txContextKey)
                            + " (probably 'sneaky throws' of checked exception). Rolling back the SQL Connection.");
                    /*
                     * IFF the SQL Connection was fetched, we will now rollback (and close) it.
                     */
                    commitOrRollbackThenCloseConnection(false, lazyConnectionSupplier.getAnyGottenConnection());

                    // ----- We're *outside* the SQL Transaction demarcation (rolled back).

                    // We will now re-throw the Throwable, which will rollback the JMS Transaction.
                    throw new JmsMatsUndeclaredCheckedExceptionRaisedException("Got a undeclared checked exception " + t
                            .getClass().getSimpleName() + " while processing " + stageOrInit(_txContextKey) + ".", t);
                }

                // ----- The ProcessingLambda went OK, no Exception was raised.

                // Check whether Session/Connection is ok before committing DB (per contract with JmsSessionHolder).
                jmsSessionHolder.isSessionOk();

                // TODO: Also somehow check runFlag of StageProcessor before committing.

                log.debug(LOG_PREFIX + "COMMIT SQL: ProcessingLambda finished, committing SQL Connection.");
                /*
                 * IFF the SQL Connection was fetched, we will now commit (and close) it.
                 */
                commitOrRollbackThenCloseConnection(true, lazyConnectionSupplier.getAnyGottenConnection());

                // ----- We're now *outside* the SQL Transaction demarcation (committed).

                // Return nicely, as the SQL Connection.commit() and .close() went OK.

                // When exiting, the JMS transaction will be committed.
            });
        }

        private void commitOrRollbackThenCloseConnection(boolean commit, Connection con) {
            // ?: Was connection gotten by code in ProcessingLambda (user code)
            if (con == null) {
                // -> No, Connection was not gotten
                log.debug(LOG_PREFIX
                        + "SQL Connection was not requested by stage processing lambda (user code), nothing"
                        + " to perform " + (commit ? "commit" : "rollback") + " on!");
                return;
            }
            // E-> Yes, Connection was gotten by ProcessingLambda (user code)
            // :: Commit or Rollback
            try {
                if (commit) {
                    con.commit();
                    log.debug(LOG_PREFIX + "Committed SQL Connection [" + con + "].");
                }
                else {
                    con.rollback();
                    log.warn(LOG_PREFIX + "Rolled Back SQL Connection [" + con + "].");
                }
            }
            catch (SQLException e) {
                throw new MatsSqlCommitOrRollbackFailedException("Could not " + (commit ? "commit" : "rollback")
                        + " SQL Connection [" + con + "] - for stage [" + _txContextKey + "].", e);
            }
            // :: Close
            try {
                con.close();
                log.debug(LOG_PREFIX + "Closed SQL Connection [" + con + "].");
            }
            catch (SQLException e) {
                log.warn("After performing " + (commit ? "commit"
                        : "rollback") + " on SQL Connection [" + con
                        + "], we tried to close it but that raised an exception - for stage [" + _txContextKey
                        + "]. Will ignore this, since the operation should have gone through.", e);
            }
        }

        /**
         * Raised if commit or rollback of the SQL Connection failed.
         */
        public static final class MatsSqlCommitOrRollbackFailedException extends RuntimeException {
            public MatsSqlCommitOrRollbackFailedException(String message, Throwable cause) {
                super(message, cause);
            }
        }

        /**
         * Performs Lazy-getting (and setting AutoCommit false) of SQL Connection for the StageProcessor thread, by
         * means of being set as the ThreadLocal supplier using
         * {@link MatsTxSqlConnection#setThreadLocalConnectionSupplier(Supplier)}.
         */
        private class LazyJdbcConnectionSupplier implements Supplier<Connection> {
            private Connection _gottenConnection;

            @Override
            public Connection get() {
                // This shall only be done on one thread: The whole point is its ThreadLocal-ness - No concurrency.
                if (_gottenConnection == null) {
                    try {
                        _gottenConnection = _jdbcConnectionSupplier.createJdbcConnection(_txContextKey);
                    }
                    catch (SQLException e) {
                        throw new MatsSqlConnectionCreationException("Could not get SQL Connection from ["
                                + _jdbcConnectionSupplier + "] for [" + _txContextKey + "].", e);
                    }
                    try {
                        _gottenConnection.setAutoCommit(false);
                    }
                    catch (SQLException e) {
                        try {
                            _gottenConnection.close();
                        }
                        catch (SQLException closeE) {
                            log.warn("When trying to set AutoCommit to false, we got an SQLException. When trying"
                                    + " to close that Connection, we got a new SQLException. Ignoring.", closeE);
                        }
                        throw new MatsSqlConnectionCreationException("Could not set AutoCommit to false on the"
                                + " SQL Connection [" + _gottenConnection + "] for [" + _txContextKey + "].", e);
                    }
                }
                return _gottenConnection;
            }

            private Connection getAnyGottenConnection() {
                return _gottenConnection;
            }
        }
    }
}
