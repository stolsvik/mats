package com.stolsvik.mats.spring.jms.tx;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;

import javax.sql.DataSource;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.InfrastructureProxy;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.impl.jms.JmsMatsJmsException;
import com.stolsvik.mats.impl.jms.JmsMatsMessageContext;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager_JmsOnly;

/**
 * Implementation of {@link JmsMatsTransactionManager} that in addition to the JMS transaction keeps a Spring
 * {@link DataSourceTransactionManager} for a DataSource for which it keeps transaction demarcation along with the JMS
 * transaction, by means of <i>"Best Effort 1 Phase Commit"</i>:
 * <ol>
 * <li><b>JMS transaction is entered</b> (a transactional JMS Connection is always within a transaction)
 * <li>JMS Message is retrieved.
 * <li><b>SQL transaction is entered</b>
 * <li>Code is executed, <i>including SQL statements and production of new "outgoing" JMS Messages.</i>
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
 * The transactionally demarcated SQL Connection can be retrieved from user code using
 * {@link ProcessContext#getAttribute(Class, String...) ProcessContext.getAttribute(Connection.class)}.
 *
 * @author Endre Stølsvik 2019-05-09 20:27 - http://stolsvik.com/, endre@stolsvik.com
 */
public class JmsMatsTransactionManager_JmsAndSpringDstm extends JmsMatsTransactionManager_JmsOnly {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsTransactionManager_JmsAndSpringDstm.class);

    private final DataSourceTransactionManager _dataSourceTransactionManager;
    private final Function<JmsMatsTxContextKey, DefaultTransactionDefinition> _transactionDefinitionFunction;

    // NOTICE: This is NOT the DataSource which the TransactionManager uses!
    private final MonitorConnectionGettingDataSourceWrapper_InfrastructureProxy _monitorConnectionGettingDataSourceWrapper;

    private final static String LOG_PREFIX = "#SPRINGJMATS# ";

    private JmsMatsTransactionManager_JmsAndSpringDstm(
            DataSourceTransactionManager dataSourceTransactionManager,
            Function<JmsMatsTxContextKey, DefaultTransactionDefinition> transactionDefinitionFunction) {
        _dataSourceTransactionManager = dataSourceTransactionManager;
        _transactionDefinitionFunction = transactionDefinitionFunction;
        // NOTICE: If created with a DataSourceTransactionManager, we will not know whether the initiation or stage
        // _actually_ got a SQL Connection during its processing.
        _monitorConnectionGettingDataSourceWrapper = null;
    }

    private JmsMatsTransactionManager_JmsAndSpringDstm(DataSource dataSource,
            Function<JmsMatsTxContextKey, DefaultTransactionDefinition> transactionDefinitionFunction) {
        // Wrap the DataSource in a MonitorConnectionGettingDataSourceWrapper_InfrastructureProxy, to know whether the
        // stage or init _actually_ got a SQL Connection
        _monitorConnectionGettingDataSourceWrapper = new MonitorConnectionGettingDataSourceWrapper_InfrastructureProxy(
                dataSource);
        log.info(LOG_PREFIX + "Wrapped the DataSource in a MonitorConnectionGettingDataSourceWrapper, so that we know"
                + " whether the stage or initialization actually used a SQL Connection.");

        // Wrap this again in a LazyConnectionDataSourceProxy, so that we do not _actually_ get a SQL Connection unless
        // the stage or init actually does any data access.
        // NOTICE: If the DataSource we are provided with already is a LazyConnectionDataSourceProxy, this is not a
        // problem: We will have two levels of lazy-ness, which is an absolutely minuscule cost.
        // NOTICE: We use the variant of NOT providing the DataSource at construction, because if we do provide it, it
        // leads to a Connection being gotten from the DataSource to determine default constants, which is not needed in
        // our case. (We set those in the TransactionDefinition before commencing stage/init processing anyway)
        LazyConnectionDataSourceProxy_InfrastructureProxy lazyConnectionDataSourceProxy = new LazyConnectionDataSourceProxy_InfrastructureProxy();
        lazyConnectionDataSourceProxy.setDefaultAutoCommit(false);
        lazyConnectionDataSourceProxy.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        // ... now setting the DataSource, and this will NOT trigger fetching of a Connection.
        lazyConnectionDataSourceProxy.setTargetDataSource(_monitorConnectionGettingDataSourceWrapper);
        log.info(LOG_PREFIX + ".. then wrapped the DataSource again in a LazyConnectionDataSourceProxy, so that we will"
                + " not actually get a physical SQL Connection from the underlying DataSource unless the stage or"
                + " initialization performs data access with it.");

        // Make the internal DataSourceTransactionManager, using the LazyConnectionDataSourceProxy
        _dataSourceTransactionManager = new DataSourceTransactionManager(lazyConnectionDataSourceProxy);
        log.info(LOG_PREFIX + "Created own DataSourceTransactionManager for the JmsMatsTransactionManager: ["
                + _dataSourceTransactionManager + "]");
        // Use the supplied TransactionDefinition Function - which probably is our own default.
        _transactionDefinitionFunction = transactionDefinitionFunction;
    }

    /**
     * <b>Recommended!</b>
     * <p>
     * Creates an own {@link DataSourceTransactionManager} for this created JmsMatsTransactionManager, and ensures that
     * the supplied {@link DataSource} is wrapped in a {@link LazyConnectionDataSourceProxy} (no problem if it already
     * is wrapped in such, though). Also with this way to construct this instance, Mats will know whether the stage or
     * initiation actually performed any SQL data access.
     * <p>
     * Uses a default {@link TransactionDefinition} Function, which sets the transaction name, sets Isolation Level to
     * {@link TransactionDefinition#ISOLATION_READ_COMMITTED}, and sets Propagation Behavior to
     * {@link TransactionDefinition#PROPAGATION_REQUIRES_NEW}.
     *
     * @param dataSource
     *            the DataSource to make a {@link DataSourceTransactionManager} from - which will be wrapped in a
     *            {@link LazyConnectionDataSourceProxy} if it not already is.
     * @return a new {@link JmsMatsTransactionManager_JmsAndSpringDstm}.
     */
    public static JmsMatsTransactionManager_JmsAndSpringDstm create(DataSource dataSource) {
        log.info(LOG_PREFIX + "TransactionDefinition Function not provided, thus using default which sets the"
                + " transaction name, sets Isolation Level to ISOLATION_READ_COMMITTED, and sets Propagation Behavior"
                + " to PROPAGATION_REQUIRES_NEW.");
        return new JmsMatsTransactionManager_JmsAndSpringDstm(dataSource, __defaultTransactionDefinitionFunction);
    }

    /**
     * Creates an own {@link DataSourceTransactionManager} for this created JmsMatsTransactionManager, and ensures that
     * the supplied {@link DataSource} is wrapped in a {@link LazyConnectionDataSourceProxy} (no problem if it already
     * is wrapped in such, though). Also with this way to construct this instance, Mats will know whether the stage or
     * initiation actually performed any SQL data access.
     * <p>
     * Uses the supplied {@link TransactionDefinition} Function to define the transactions - consider
     * {@link #create(DataSource)} if you are OK with the defaults.
     *
     * @param dataSource
     *            the DataSource to make a {@link DataSourceTransactionManager} from - which will be wrapped in a
     *            {@link LazyConnectionDataSourceProxy} if it not already is.
     * @param transactionDefinitionFunction
     *            a {@link Function} which returns a {@link DefaultTransactionDefinition}, possibly based on the
     *            provided {@link JmsMatsTxContextKey} (e.g. different isolation level for a special endpoint).
     * @return a new {@link JmsMatsTransactionManager_JmsAndSpringDstm}.
     */
    public static JmsMatsTransactionManager_JmsAndSpringDstm create(DataSource dataSource,
            Function<JmsMatsTxContextKey, DefaultTransactionDefinition> transactionDefinitionFunction) {
        return new JmsMatsTransactionManager_JmsAndSpringDstm(dataSource, transactionDefinitionFunction);
    }

    /**
     * Creates a {@link JmsMatsTransactionManager_JmsAndSpringDstm} from a provided {@link DataSourceTransactionManager}
     * - do note that the {@link DataSource} within the <code>{@link DataSourceTransactionManager}</code> definitely
     * should be wrapped in a {@link LazyConnectionDataSourceProxy}, and do also note that Mats with this factory will
     * not be able to know whether the stage or initiation actually performed data access.
     * <p>
     * Uses a default {@link TransactionDefinition} Function, which sets the transaction name, sets Isolation Level to
     * {@link TransactionDefinition#ISOLATION_READ_COMMITTED}, and sets Propagation Behavior to
     * {@link TransactionDefinition#PROPAGATION_REQUIRES_NEW}.
     * <p>
     * <b>NOTICE: If you can, rather use {@link #create(DataSource)} or {@link #create(DataSource, Function)}.</b>
     *
     * @param dataSourceTransactionManager
     *            the {@link DataSourceTransactionManager} to use for transaction management.
     * @return a new {@link JmsMatsTransactionManager_JmsAndSpringDstm}.
     */
    public static JmsMatsTransactionManager_JmsAndSpringDstm create(
            DataSourceTransactionManager dataSourceTransactionManager) {
        log.info(LOG_PREFIX + "TransactionDefinition Function not provided, thus using default which sets the"
                + " transaction name, sets Isolation Level to ISOLATION_READ_COMMITTED, and sets Propagation Behavior"
                + " to PROPAGATION_REQUIRES_NEW.");
        return new JmsMatsTransactionManager_JmsAndSpringDstm(dataSourceTransactionManager,
                __defaultTransactionDefinitionFunction);
    }

    /**
     * Creates a {@link JmsMatsTransactionManager_JmsAndSpringDstm} from a provided {@link DataSourceTransactionManager}
     * - do note that the {@link DataSource} within the <code>{@link DataSourceTransactionManager}</code> definitely
     * should be wrapped in a {@link LazyConnectionDataSourceProxy}, and do also note that Mats with this factory will
     * not be able to know whether the stage or initiation actually performed data access.
     * <p>
     * Uses the supplied {@link TransactionDefinition} Function to define the transactions - consider
     * {@link #create(DataSourceTransactionManager)} if you are OK with the defaults.
     * <p>
     * <b>NOTICE: If you can, rather use {@link #create(DataSource)} or {@link #create(DataSource, Function)}.</b>
     *
     * @param dataSourceTransactionManager
     *            the {@link DataSourceTransactionManager} to use for transaction management.
     * @param transactionDefinitionFunction
     *            a {@link Function} which returns a {@link DefaultTransactionDefinition}, possibly based on the
     *            provided {@link JmsMatsTxContextKey} (e.g. different isolation level for a special endpoint).
     * @return a new {@link JmsMatsTransactionManager_JmsAndSpringDstm}.
     */
    public static JmsMatsTransactionManager_JmsAndSpringDstm create(
            DataSourceTransactionManager dataSourceTransactionManager,
            Function<JmsMatsTxContextKey, DefaultTransactionDefinition> transactionDefinitionFunction) {
        return new JmsMatsTransactionManager_JmsAndSpringDstm(dataSourceTransactionManager,
                transactionDefinitionFunction);
    }

    /**
     * <i>"The magnitude of this hack compares favorably with that of the US-of-A's national debt."</i>
     * <p>
     * We want the Lazy-and-Monitored DataSource which we use inside here to "compare equals" with that of the
     * DataSource which is supplied to us - and which then is used "on the outside" - wrt. how {@link DataSourceUtils}
     * compare them in its ThreadLocal cache-hackery.
     * <p>
     * Do note that wrt. craziness of this hack, we are actually employing a feature that exists in
     * TransactionSynchronizationUtils.unwrapResourceIfNecessary(..), where the check for InfraStructureProxy resides.
     */
    private static class LazyConnectionDataSourceProxy_InfrastructureProxy
            extends LazyConnectionDataSourceProxy implements InfrastructureProxy {
        @Override
        public Object getWrappedObject() {
            DataSource targetDataSource = getTargetDataSource();
            if (targetDataSource instanceof InfrastructureProxy) {
                return ((InfrastructureProxy) targetDataSource).getWrappedObject();
            }
            return targetDataSource;
        }
    }

    /**
     * Wrapper of DataSource that keeps ThreadLocal state of whether a SQL Connection has actually been gotten from the
     * underlying DataSource.
     * <p>
     * Also implements {@link InfrastructureProxy}, read up on
     * {@link LazyConnectionDataSourceProxy_InfrastructureProxy}.
     */
    private static class MonitorConnectionGettingDataSourceWrapper_InfrastructureProxy
            extends DelegatingDataSource implements InfrastructureProxy {
        public MonitorConnectionGettingDataSourceWrapper_InfrastructureProxy(DataSource targetDataSource) {
            super(targetDataSource);
        }

        private ThreadLocal<ConnectionAndStacktraceHolder> _connectionThreadLocal = new ThreadLocal<>();

        Connection getThreadLocalGottenConnection() {
            ConnectionAndStacktraceHolder gottenConnection = _connectionThreadLocal.get();
            if (gottenConnection == null) {
                return null;
            }
            return gottenConnection.connection;
        }

        void clearThreadLocalConnection() {
            _connectionThreadLocal.remove();
        }

        @Override
        public Connection getConnection() throws SQLException {
            if (log.isDebugEnabled()) log.debug(LOG_PREFIX
                    + "NOTICE: DataSource.getConnection(): SQL Connection actually being gotten.");
            return getConnection_Internal(super::getConnection);
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "NOTICE: DataSource.getConnection(\"" + username
                    + "\", {password}): SQL Connection actually being gotten.");
            return getConnection_Internal(() -> super.getConnection(username, password));
        }

        private interface ConnectionGetter {
            Connection getConnection() throws SQLException;
        }

        private Connection getConnection_Internal(ConnectionGetter lambda) throws SQLException {
            if (_connectionThreadLocal.get() != null) {
                throw new IllegalStateException("The SQL Connection is already gotten at this point, why is it that"
                        + " you come again getting a new one? The existing one should reside in the"
                        + " ThreadLocal of the DataSourceTransactionManager. A stacktrace of the original gotten"
                        + " Connection is attached as cause.", _connectionThreadLocal.get().debugStacktrace);
            }
            Connection connection = lambda.getConnection();
            _connectionThreadLocal.set(new ConnectionAndStacktraceHolder(connection,
                    new DebugStacktrace("This is where the connection was initially gotten.")));
            return connection;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + " for target DataSource ["
                    + getTargetDataSource()
                    + "], ThreadLocal Connection:[" + _connectionThreadLocal.get() + "]";
        }

        @Override
        public Object getWrappedObject() {
            DataSource targetDataSource = getTargetDataSource();
            if (targetDataSource instanceof InfrastructureProxy) {
                return ((InfrastructureProxy) targetDataSource).getWrappedObject();
            }
            return targetDataSource;
        }

        private static class ConnectionAndStacktraceHolder {
            private final Connection connection;
            private final DebugStacktrace debugStacktrace;

            public ConnectionAndStacktraceHolder(Connection connection,
                    DebugStacktrace debugStacktrace) {
                this.connection = connection;
                this.debugStacktrace = debugStacktrace;
            }
        }

        private static class DebugStacktrace extends Exception {
            public DebugStacktrace(String message) {
                super(message);
            }
        }
    }

    /**
     * The default TransactionDefinition Function. Sets Isolation Level to READ_COMMITTED, Propagation Behavior to
     * REQUIRES_NEW - and also sets the name of the transaction to {@link JmsMatsTxContextKey}.toString().
     */
    private static Function<JmsMatsTxContextKey, DefaultTransactionDefinition> __defaultTransactionDefinitionFunction = (
            txContextKey) -> {
        DefaultTransactionDefinition transDef = new DefaultTransactionDefinition();
        transDef.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transDef.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transDef.setName(txContextKey.toString());
        return transDef;
    };

    @Override
    public TransactionContext getTransactionContext(JmsMatsTxContextKey txContextKey) {
        // Get the TransactionDefinition for this JmsMatsTxContextKey, which is a constant afterwards.
        DefaultTransactionDefinition defaultTransactionDefinition = _transactionDefinitionFunction.apply(txContextKey);
        return new TransactionalContext_JmsAndSpringDstm(txContextKey, _dataSourceTransactionManager,
                defaultTransactionDefinition, _monitorConnectionGettingDataSourceWrapper);
    }

    /**
     * The {@link TransactionContext}-implementation for {@link JmsMatsTransactionManager_JmsAndSpringDstm}.
     */
    private static class TransactionalContext_JmsAndSpringDstm extends TransactionalContext_JmsOnly {

        private final DataSourceTransactionManager _dataSourceTransactionManager;
        private final DefaultTransactionDefinition _transactionDefinitionForThisContext;
        // NOTICE: This is NOT the DataSource which the DataSourceTransactionManager uses!
        // NOTICE: It can be null, if we were created with explicitly provided DataSourceTransactionManager.
        private final MonitorConnectionGettingDataSourceWrapper_InfrastructureProxy _monitorConnectionGettingDataSourceWrapper;

        public TransactionalContext_JmsAndSpringDstm(
                JmsMatsTxContextKey txContextKey,
                DataSourceTransactionManager dataSourceTransactionManager,
                DefaultTransactionDefinition transactionDefinitionForThisContext,
                MonitorConnectionGettingDataSourceWrapper_InfrastructureProxy monitorConnectionGettingDataSourceWrapper) {
            super(txContextKey);
            _dataSourceTransactionManager = dataSourceTransactionManager;
            _transactionDefinitionForThisContext = transactionDefinitionForThisContext;
            _monitorConnectionGettingDataSourceWrapper = monitorConnectionGettingDataSourceWrapper;
        }

        @Override
        public void doTransaction(JmsMatsMessageContext jmsMatsMessageContext, ProcessingLambda lambda)
                throws JmsMatsJmsException {
            try {
                // :: First make the potential SQL Connection available
                // Notice how we here use the DataSourceUtils class, so that we get the tx ThreadLocal Connection.
                // Read more at both DataSourceUtils and DataSourceTransactionManager.
                jmsMatsMessageContext.setSqlConnectionSupplier(() -> DataSourceUtils
                        .getConnection(_dataSourceTransactionManager.getDataSource()));
                jmsMatsMessageContext.setSqllConnectionEmployed(() -> _monitorConnectionGettingDataSourceWrapper
                        .getThreadLocalGottenConnection() != null);

                // :: We invoke the "outer" transaction, which is the JMS transaction.
                super.doTransaction(jmsMatsMessageContext, () -> {
                    // ----- We're *within* the JMS Transaction demarcation.

                    // :: Now go into the SQL Transaction demarcation
                    TransactionStatus transactionStatus = _dataSourceTransactionManager.getTransaction(
                            _transactionDefinitionForThisContext);

                    try {
                        if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "About to run ProcessingLambda for "
                                + stageOrInit(_txContextKey) + ", within Spring SQL Transactional demarcation.");
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
                        // !!NOTE!!: The full Exception will be logged by outside JMS-trans class on JMS rollback
                        // handling.
                        log.error(LOG_PREFIX + "ROLLBACK SQL: " + e.getClass().getSimpleName() + " while processing "
                                + stageOrInit(_txContextKey) + " (should only be from user code)."
                                + " Rolling back the SQL Connection.");
                        /*
                         * IFF the SQL Connection was fetched, we will now rollback (and close) it.
                         */
                        commitOrRollbackSqlTransaction(false, transactionStatus);

                        // ----- We're *outside* the SQL Transaction demarcation (rolled back).

                        // We will now throw on the Exception, which will rollback the JMS Transaction.
                        throw e;
                    }
                    catch (Throwable t) {
                        // ----- This must have been a "sneaky throws"; Throwing an undeclared checked exception.
                        // !!NOTE!!: The full Exception will be logged by outside JMS-trans class on JMS rollback
                        // handling.
                        log.error(LOG_PREFIX + "ROLLBACK SQL: Got an undeclared checked exception " + t.getClass()
                                .getSimpleName() + " while processing " + stageOrInit(_txContextKey)
                                + " (should only be 'sneaky throws' of checked exception in user code)."
                                + " Rolling back the SQL Connection.");
                        /*
                         * IFF the SQL Connection was fetched, we will now rollback (and close) it.
                         */
                        commitOrRollbackSqlTransaction(false, transactionStatus);

                        // ----- We're *outside* the SQL Transaction demarcation (rolled back).

                        // We will now re-throw the Throwable, which will rollback the JMS Transaction.
                        throw new JmsMatsUndeclaredCheckedExceptionRaisedException("Got a undeclared checked exception "
                                + t.getClass().getSimpleName() + " while processing " + stageOrInit(_txContextKey)
                                + ".", t);
                    }

                    // ----- The ProcessingLambda went OK, no Exception was raised.

                    if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "COMMIT SQL: ProcessingLambda finished,"
                            + " committing SQL Connection.");

                    // Check whether Session/Connection is ok before committing DB (per contract with JmsSessionHolder).
                    jmsMatsMessageContext.getJmsSessionHolder().isSessionOk();

                    // TODO: Also somehow check runFlag of StageProcessor before committing.

                    /*
                     * IFF the SQL Connection was fetched, we will now commit (and close) it.
                     */
                    commitOrRollbackSqlTransaction(true, transactionStatus);

                    // ----- We're now *outside* the SQL Transaction demarcation (committed).

                    // Return nicely, as the SQL Connection.commit() went OK.

                    // When exiting this lambda, the JMS transaction will be committed by the "outer" Jms tx impl.
                });
            }
            finally {
                // ?: Do we have "monitoring" of the getting of Connection from DataSource?
                if (_monitorConnectionGettingDataSourceWrapper != null) {
                    // -> Yes, we have monitoring - so we must now clear the ThreadLocal of any gotten Connection.
                    _monitorConnectionGettingDataSourceWrapper.clearThreadLocalConnection();
                }
            }
        }

        /**
         * Make note: The Spring DataSourceTransactionManager closes the SQL Connection after commit or rollback. Read
         * more e.g. <a href="https://stackoverflow.com/a/18207654/39334">here</a>, or look in
         * {@link DataSourceTransactionManager#doCleanupAfterCompletion(Object)}, which invokes
         * {@link DataSourceUtils#releaseConnection(Connection, DataSource)}, which invokes doCloseConnection(), which
         * eventually calls connection.close().
         */
        private void commitOrRollbackSqlTransaction(boolean commit, TransactionStatus transactionStatus) {
            // ?: Was connection gotten by code in ProcessingLambda (user code)
            // NOTICE: We must commit or rollback the Spring TransactionManager nevertheless, to clean up
            if ((_monitorConnectionGettingDataSourceWrapper != null)
                    && (_monitorConnectionGettingDataSourceWrapper.getThreadLocalGottenConnection() == null)) {
                // -> No, Connection was not gotten
                if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "NOTICE: SQL Connection was not requested by stage"
                        + " or initiation (user code), the following commit is no-op.");
                // NOTICE: We must commit or rollback the Spring TransactionManager nevertheless, to clean up.
                // NOTICE: NOT returning! The log line is just for informational purposes.
            }
            // :: Commit or Rollback
            try {
                // ?: Commit or rollback?
                if (commit) {
                    // -> Commit.
                    // ?: Check if we have gotten into "RollbackOnly" state, implying that the user has messed up.
                    if (transactionStatus.isRollbackOnly()) {
                        // -> Yes, we're in "RollbackOnly" - so rollback and throw out.
                        String msg = "When about to commit the SQL Transaction ["
                                + transactionStatus + "], we found that it was in a 'RollbackOnly' state. This implies"
                                + " that you have performed your own Spring transaction management within the Mats"
                                + " Stage, which is not supported. Will now rollback the SQL, and throw out.";
                        log.error(msg);
                        // If the rollback throws, it was a rollback (read the Exception-throwing at final catch).
                        commit = false;
                        // Do rollback.
                        _dataSourceTransactionManager.rollback(transactionStatus);
                        // Throw out.
                        throw new MatsSqlCommitOrRollbackFailedException(msg);
                    }
                    // E-> No, we were not in "RollbackOnly" - so commit this stuff, and get out.
                    _dataSourceTransactionManager.commit(transactionStatus);
                    if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "Committed SQL Transaction ["
                            + transactionStatus + "].");
                }
                else {
                    // -> Rollback.
                    _dataSourceTransactionManager.rollback(transactionStatus);
                    if (log.isDebugEnabled()) log.warn(LOG_PREFIX + "Rolled Back SQL Transaction ["
                            + transactionStatus + "].");
                }
            }
            catch (TransactionException e) {
                throw new MatsSqlCommitOrRollbackFailedException("Could not " + (commit ? "commit" : "rollback")
                        + " SQL Transaction [" + transactionStatus + "] - for stage [" + _txContextKey + "].", e);
            }
        }

        /**
         * Raised if commit or rollback of the SQL Connection failed.
         */
        static final class MatsSqlCommitOrRollbackFailedException extends RuntimeException {
            MatsSqlCommitOrRollbackFailedException(String message, Throwable cause) {
                super(message, cause);
            }

            MatsSqlCommitOrRollbackFailedException(String message) {
                super(message);
            }
        }
    }
}
