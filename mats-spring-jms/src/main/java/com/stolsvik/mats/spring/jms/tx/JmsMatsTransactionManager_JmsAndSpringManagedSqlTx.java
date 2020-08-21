package com.stolsvik.mats.spring.jms.tx;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.InfrastructureProxy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory.ContextLocal;
import com.stolsvik.mats.impl.jms.JmsMatsJmsException;
import com.stolsvik.mats.impl.jms.JmsMatsMessageContext;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager_Jms;

/**
 * Implementation of {@link JmsMatsTransactionManager} that in addition to the JMS transaction keeps a Spring
 * {@link PlatformTransactionManager} employing a JDBC DataSource for which it keeps transaction demarcation along with
 * the JMS transaction, by means of <i>"Best Effort 1 Phase Commit"</i>. Note that you can choose between providing a
 * DataSource, in which case this JmsMatsTransactionManager internally creates a {@link DataSourceTransactionManager},
 * or you can provide a <code>PlatformTransactionManager</code> employing a <code>DataSource</code> that you've created
 * and employ externally (i.e. <code>DataSourceTransactionManager</code>, <code>JpaTransactionManager</code> or
 * <code>HibernateTransactionManager</code>). In the case where you provide a <code>PlatformTransactionManager</code>,
 * you should <i>definitely</i> wrap the DataSource employed when creating it by the method
 * {@link #wrapLazyConnectionDatasource(DataSource)}, so that you get lazy Connection fetching and so that Mats can know
 * whether the SQL Connection was actually employed within a Mats Stage - this also elides the committing of an empty DB
 * transaction if a Mats Stage does not actually employ a SQL Connection. Note that whether you use the wrapped
 * DataSource or the non-wrapped DataSource when creating e.g. {@link JdbcTemplate}s does not matter, as Spring's
 * {@link DataSourceUtils} and {@link TransactionSynchronizationManager} has an unwrapping strategy when retrieving the
 * transactionally demarcated Connection.
 * <p />
 * Explanation of <i>Best Effort 1 Phase Commit</i>:
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
 * transaction (which effectively boils down to <i>"yes, I received this message, and sent these"</i>) is much harder to
 * fail, where the only situation where it can fail is due to infrastructure/hardware failures (exploding server / full
 * disk on Message Broker). This is called "Best Effort 1PC", and is nicely explained in <a href=
 * "http://www.javaworld.com/article/2077963/open-source-tools/distributed-transactions-in-spring--with-and-without-xa.html?page=2">
 * this article</a>. If this failure occurs, it will be caught and logged on ERROR level (by
 * {@link JmsMatsTransactionManager_Jms}) - and then the Message Broker will probably try to redeliver the message.
 * Also read the <a href="http://activemq.apache.org/should-i-use-xa.html">Should I use XA Transactions</a> from Apache
 * Active MQ.
 * <p />
 * Wise tip when working with <i>Message Oriented Middleware</i>: Code idempotent! Handle double-deliveries!
 * <p />
 * The transactionally demarcated SQL Connection can be retrieved from within Mats Stage lambda code user code using
 * {@link ProcessContext#getAttribute(Class, String...) ProcessContext.getAttribute(Connection.class)} - which also is
 * available using {@link ContextLocal#getAttribute(Class, String...) ContextLocal.getAttribute(Connection.class)}.
 * <b>Notice:</b> In a Spring context, you can also get the transactionally demarcated thread-bound Connection via
 * {@link DataSourceUtils#getConnection(DataSource) DataSourceUtils.getConnection(dataSource)} - this is indeed what
 * Spring's JDBC Template and friends are doing. If you go directly to the DataSource, you will get a new Connection not
 * participating in the transaction. This "feature" might sometimes be of interest if you want something to be performed
 * regardless of whether the stage processing fails or not. <i>(However, if you do such a thing, you must remember the
 * built-in retry mechanism JMS Message Brokers has: If something fails, whatever database changes you performed
 * successfully with such a non-tx-managed Connection will not participate in the rollback, and will already have been
 * performed when the message is retried).</i>
 *
 * @author Endre Stølsvik 2019-05-09 20:27 - http://stolsvik.com/, endre@stolsvik.com
 */
public class JmsMatsTransactionManager_JmsAndSpringManagedSqlTx extends JmsMatsTransactionManager_Jms {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsTransactionManager_JmsAndSpringManagedSqlTx.class);

    private final PlatformTransactionManager _platformTransactionManager;
    private final DataSource _dataSource;
    private final Function<JmsMatsTxContextKey, DefaultTransactionDefinition> _transactionDefinitionFunction;

    private final static String LOG_PREFIX = "#SPRINGJMATS# ";

    private JmsMatsTransactionManager_JmsAndSpringManagedSqlTx(
            PlatformTransactionManager platformTransactionManager, DataSource dataSource,
            Function<JmsMatsTxContextKey, DefaultTransactionDefinition> transactionDefinitionFunction) {
        // Store the PlatformTransactionManager we got
        _platformTransactionManager = platformTransactionManager;
        // Store the DataSource
        _dataSource = dataSource;
        // Use the supplied TransactionDefinition Function - which probably is our own default.
        _transactionDefinitionFunction = transactionDefinitionFunction;
    }

    private JmsMatsTransactionManager_JmsAndSpringManagedSqlTx(DataSource dataSource,
            Function<JmsMatsTxContextKey, DefaultTransactionDefinition> transactionDefinitionFunction) {

        // ?: Just sanity check that it is not already wrapped with our magic, because that makes no sense
        if (dataSource instanceof LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy) {
            throw new IllegalArgumentException("When employing the DataSource-taking factory methods, you should not"
                    + " have pre-wrapped the DataSource with the wrapDataSource(..) method. The reason why you did"
                    + " wrap the DataSource was probably that you wanted to employ the"
                    + " PlatformTransactionManager-taking factory methods, not this variant that internally creates"
                    + " a DataSourceTransactionManager.");
        }

        // Wrap the DataSource up in the insanity-inducing stack of wrappers.
        _dataSource = wrapLazyConnectionDatasource(dataSource);

        // Make the internal DataSourceTransactionManager, using the wrapped DataSource.
        _platformTransactionManager = new DataSourceTransactionManager(_dataSource);
        log.info(LOG_PREFIX + "Created own DataSourceTransactionManager for the JmsMatsTransactionManager: ["
                + _platformTransactionManager + "] with magic wrapped DataSource [" + _dataSource + "].");
        // Use the supplied TransactionDefinition Function - which probably is our own default.
        _transactionDefinitionFunction = transactionDefinitionFunction;
    }

    /**
     * <b>Simplest, recommended if appropriate for your setup!</b>
     * <p />
     * Creates an internal {@link DataSourceTransactionManager} for this created JmsMatsTransactionManager, and ensures
     * that the supplied {@link DataSource} is wrapped using the {@link #wrapLazyConnectionDatasource(DataSource)}
     * method. Also with this way to construct the instance, Mats will know whether the stage or initiation actually
     * performed any SQL data access.
     * <p />
     * Uses a default {@link TransactionDefinition} Function, which sets the transaction name, sets Isolation Level to
     * {@link TransactionDefinition#ISOLATION_READ_COMMITTED}, and sets Propagation Behavior to
     * {@link TransactionDefinition#PROPAGATION_REQUIRES_NEW}.
     *
     * @param dataSource
     *            the DataSource to make a {@link DataSourceTransactionManager} from - which will be wrapped using
     *            {@link #wrapLazyConnectionDatasource(DataSource)} if it not already is.
     * @return a new {@link JmsMatsTransactionManager_JmsAndSpringManagedSqlTx}.
     */
    public static JmsMatsTransactionManager_JmsAndSpringManagedSqlTx create(DataSource dataSource) {
        log.info(LOG_PREFIX + "TransactionDefinition Function not provided, thus using default which sets the"
                + " transaction name, sets Isolation Level to ISOLATION_READ_COMMITTED, and sets Propagation Behavior"
                + " to PROPAGATION_REQUIRES_NEW.");
        return new JmsMatsTransactionManager_JmsAndSpringManagedSqlTx(dataSource,
                getStandardTransactionDefinitionFunctionFor(DataSourceTransactionManager.class));
    }

    /**
     * Creates an internal {@link DataSourceTransactionManager} for this created JmsMatsTransactionManager, and ensures
     * that the supplied {@link DataSource} is wrapped using the {@link #wrapLazyConnectionDatasource(DataSource)}
     * method. Also with this way to construct the instance, Mats will know whether the stage or initiation actually
     * performed any SQL data access.
     * <p />
     * Uses the supplied {@link TransactionDefinition} Function to define the transactions - consider
     * {@link #create(DataSource)} if you are OK with the standard.
     *
     * @param dataSource
     *            the DataSource to make a {@link DataSourceTransactionManager} from - which will be wrapped using
     *            {@link #wrapLazyConnectionDatasource(DataSource)} if it not already is.
     * @param transactionDefinitionFunction
     *            a {@link Function} which returns a {@link DefaultTransactionDefinition}, possibly based on the
     *            provided {@link JmsMatsTxContextKey} (e.g. different isolation level for a special endpoint).
     * @return a new {@link JmsMatsTransactionManager_JmsAndSpringManagedSqlTx}.
     */
    public static JmsMatsTransactionManager_JmsAndSpringManagedSqlTx create(DataSource dataSource,
            Function<JmsMatsTxContextKey, DefaultTransactionDefinition> transactionDefinitionFunction) {
        return new JmsMatsTransactionManager_JmsAndSpringManagedSqlTx(dataSource, transactionDefinitionFunction);
    }

    /**
     * Creates a {@link JmsMatsTransactionManager_JmsAndSpringManagedSqlTx} from a provided
     * {@link PlatformTransactionManager} (of a type which manages a DataSource), where the supplied instance is
     * introspected to find a method <code>getDataSource()</code> from where to get the underlying DataSource. Do note
     * that you should preferably have the {@link DataSource} within the <code>{@link PlatformTransactionManager}</code>
     * wrapped using the {@link #wrapLazyConnectionDatasource(DataSource)} method. If not wrapped as such, Mats will not
     * be able to know whether the stage or initiation actually performed data access.
     * <p />
     * Uses the standard {@link TransactionDefinition} Function, which sets the transaction name, sets Isolation Level
     * to {@link TransactionDefinition#ISOLATION_READ_COMMITTED} (unless HibernateTxMgr), and sets Propagation Behavior
     * to {@link TransactionDefinition#PROPAGATION_REQUIRES_NEW} - see
     * {@link #getStandardTransactionDefinitionFunctionFor(Class)}.
     *
     * @param platformTransactionManager
     *            the {@link DataSourceTransactionManager} to use for transaction management.
     * @return a new {@link JmsMatsTransactionManager_JmsAndSpringManagedSqlTx}.
     */
    public static JmsMatsTransactionManager_JmsAndSpringManagedSqlTx create(
            PlatformTransactionManager platformTransactionManager) {
        log.info(LOG_PREFIX + "TransactionDefinition Function not provided, thus using default which sets the"
                + " transaction name, sets Isolation Level to ISOLATION_READ_COMMITTED, and sets Propagation Behavior"
                + " to PROPAGATION_REQUIRES_NEW (unless HibernateTransactionManager, where setting Isolation Level"
                + " evidently is not supported).");
        log.info(LOG_PREFIX + "DataSource not provided, introspecting the supplied PlatformTransactionManager to find"
                + " a method .getDataSource() from where to get it. [" + platformTransactionManager + "]");

        DataSource dataSource;
        try {
            Method getDataSource = platformTransactionManager.getClass().getMethod("getDataSource");
            dataSource = (DataSource) getDataSource.invoke(platformTransactionManager);
            if (dataSource == null) {
                throw new IllegalArgumentException("When invoking .getDataSource() on the PlatformTransactionManager,"
                        + " we got 'null' return [" + platformTransactionManager + "].");
            }
            log.info(LOG_PREFIX + ".. found DataSource [" + dataSource + "].");
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("The supplied PlatformTransactionManager does not have a"
                    + " .getDataSource() method, or got problems invoking it.", e);
        }

        return new JmsMatsTransactionManager_JmsAndSpringManagedSqlTx(platformTransactionManager, dataSource,
                getStandardTransactionDefinitionFunctionFor(platformTransactionManager.getClass()));
    }

    /**
     * Creates a {@link JmsMatsTransactionManager_JmsAndSpringManagedSqlTx} from a provided
     * {@link PlatformTransactionManager} (of a type which manages a DataSource) -Do note that you should preferably
     * have the {@link DataSource} within the <code>{@link PlatformTransactionManager}</code> wrapped using the
     * {@link #wrapLazyConnectionDatasource(DataSource)} method. If not wrapped as such, Mats will not be able to know
     * whether the stage or initiation actually performed data access.
     * <p />
     * Uses the standard {@link TransactionDefinition} Function, which sets the transaction name, sets Isolation Level
     * to {@link TransactionDefinition#ISOLATION_READ_COMMITTED} (unless HibernateTxMgr), and sets Propagation Behavior
     * to {@link TransactionDefinition#PROPAGATION_REQUIRES_NEW} - see
     * {@link #getStandardTransactionDefinitionFunctionFor(Class)}.
     *
     * @param platformTransactionManager
     *            the {@link DataSourceTransactionManager} to use for transaction management.
     * @param dataSource
     *            the {@link DataSource} which the supplied {@link PlatformTransactionManager} handles.
     * @return a new {@link JmsMatsTransactionManager_JmsAndSpringManagedSqlTx}.
     */
    public static JmsMatsTransactionManager_JmsAndSpringManagedSqlTx create(
            PlatformTransactionManager platformTransactionManager, DataSource dataSource) {
        log.info(LOG_PREFIX + "TransactionDefinition Function not provided, thus using default which sets the"
                + " transaction name, sets Isolation Level to ISOLATION_READ_COMMITTED, and sets Propagation Behavior"
                + " to PROPAGATION_REQUIRES_NEW.");
        return new JmsMatsTransactionManager_JmsAndSpringManagedSqlTx(platformTransactionManager, dataSource,
                getStandardTransactionDefinitionFunctionFor(platformTransactionManager.getClass()));
    }

    /**
     * Creates a {@link JmsMatsTransactionManager_JmsAndSpringManagedSqlTx} from a provided
     * {@link PlatformTransactionManager} (managing a DataSource) - do note that it is very good if the
     * {@link DataSource} within the <code>{@link PlatformTransactionManager}</code> is wrapped using the
     * {@link #wrapLazyConnectionDatasource(DataSource)} method. If not wrapped as such, Mats will not be able to know
     * whether the stage or initiation actually performed data access.
     * <p />
     * Uses the supplied {@link TransactionDefinition} Function to define the transactions - consider
     * {@link #create(PlatformTransactionManager, DataSource)} if you are OK with the standard.
     *
     * @param platformTransactionManager
     *            the {@link PlatformTransactionManager} to use for transaction management (must be one employing a
     *            DataSource).
     * @param dataSource
     *            the {@link DataSource} which the supplied {@link PlatformTransactionManager} handles.
     * @param transactionDefinitionFunction
     *            a {@link Function} which returns a {@link DefaultTransactionDefinition}, possibly based on the
     *            provided {@link JmsMatsTxContextKey} (e.g. different isolation level for a special endpoint).
     * @return a new {@link JmsMatsTransactionManager_JmsAndSpringManagedSqlTx}.
     */
    public static JmsMatsTransactionManager_JmsAndSpringManagedSqlTx create(
            PlatformTransactionManager platformTransactionManager, DataSource dataSource,
            Function<JmsMatsTxContextKey, DefaultTransactionDefinition> transactionDefinitionFunction) {
        return new JmsMatsTransactionManager_JmsAndSpringManagedSqlTx(platformTransactionManager, dataSource,
                transactionDefinitionFunction);
    }

    /**
     * Returns the standard TransactionDefinition Function for the supplied PlatformTransactionManager. Sets Isolation
     * Level to {@link TransactionDefinition#ISOLATION_READ_COMMITTED ISOLATION_READ_COMMITTED} (unless
     * HibernateTransactionManager, which does not support setting Isolation Level) and Propagation Behavior to
     * {@link TransactionDefinition#PROPAGATION_REQUIRES_NEW PROPAGATION_REQUIRES_NEW} - and also sets the name of the
     * transaction to {@link JmsMatsTxContextKey}.toString().
     */
    public static Function<JmsMatsTxContextKey, DefaultTransactionDefinition> getStandardTransactionDefinitionFunctionFor(
            Class<? extends PlatformTransactionManager> platformTransactionManager) {
        // ?: Is it HibernateTransactionmanager?
        if (platformTransactionManager.getSimpleName().equals("HibernateTransactionManager")) {
            // -> Yes, Hibernate, which does not allow to set Isolation Level
            return (txContextKey) -> {
                DefaultTransactionDefinition transDef = new DefaultTransactionDefinition();
                transDef.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
                transDef.setName(txContextKey.toString());
                return transDef;
            };
        }
        // E-> normal mode
        return (txContextKey) -> {
            DefaultTransactionDefinition transDef = new DefaultTransactionDefinition();
            transDef.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
            transDef.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
            transDef.setName(txContextKey.toString());
            return transDef;
        };
    }

    /**
     * Creates a proxy/wrapper that has lazy connection getting, and monitoring of whether the connection was actually
     * retrieved. This again enables SpringJmsMats implementation to see if the SQL Connection was actually
     * <i>employed</i> (i.e. a Statement was created) - not only whether we went into transactional demarcation, which a
     * Mats stage <i>always</i> does. This method is internally employed with the {@link #create(DataSource)
     * DataSource-taking} factories of this class which makes an internal {@link DataSourceTransactionManager}, but
     * should also be employed if you externally create another type of {@link PlatformTransactionManager}, e.g. the
     * HibernateTransactionManager, and provide that to the factories of this class taking a PlatformTransactionManager.
     * <p />
     * It creates a stack of proxies as such:
     * <ul>
     * <li>LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy (returned by this method, and is an extension
     * of Spring's {@link LazyConnectionDataSourceProxy}): Hinders a connection from actually being fetched from the
     * underlying DataSource until it is explicitly employed by the application (i.e. starting a transaction will by
     * itself <i>not</i> fetch a Connection). And have a reference to the proxy underneath, so that we can check whether
     * the Connection <i>actually</i> was employed.</li>
     * <li>MonitorConnectionGettingDataSourceWrapper_InfrastructureProxy: Keeps a tab of whether a Connection was gotten
     * from the underlying DataSource. Which, since the layer above is the lazy wrapper, will <i>not</i> happen merely
     * by starting a transaction.</li>
     * <li>The actual DataSource: This is the actual connection to the SQL DataBase, the one you provide as argument to
     * this method.</li>
     * </ul>
     * <p />
     * We want this Lazy-and-Monitored DataSource which is returned here to "compare equals" with that of the DataSource
     * which is supplied to us - and which might be employed by other components "on the outside" of Mats - wrt. how
     * Spring's {@link DataSourceUtils} compare them in its ThreadLocal cache-hackery. Therefore, all those proxies in
     * the list above implement {@link InfrastructureProxy}, which means that Spring can trawl its way down to the
     * actual DataSource when necessary. This is a feature that exists in
     * TransactionSynchronizationUtils.unwrapResourceIfNecessary(..), where the check for InfraStructureProxy resides
     * <p />
     * <i>"The magnitude of this hack compares favorably with that of the US-of-A's national debt."</i>
     * <p />
     * Note: It is not a problem if the DataSource supplied is already wrapped in a
     * {@link LazyConnectionDataSourceProxy}, but it is completely unnecessary.
     * <p />
     * Tip: If you would want to check whether the LazyConnection stuff work, you may within a Mats stage do a
     * DataSourceUtil.getConnection(dataSource) - if the returned Connection's toString() starts with "Lazy Connection
     * proxy for target DataSource [..{wrapped datasource } ..]" then the actual Connection is still not gotten. When it
     * is gotten, the toString() will forward directly to the wrapped actual Connection.
     */
    public static DataSource wrapLazyConnectionDatasource(DataSource targetDataSource) {
        // Wrap the DataSource in a MonitorConnectionGettingDataSourceWrapper_InfrastructureProxy, to know whether the
        // stage or init _actually_ got a SQL Connection
        MonitorConnectionGettingDataSourceProxy_InfrastructureProxy monitorConnectionGettingDataSourceProxy = new MonitorConnectionGettingDataSourceProxy_InfrastructureProxy(
                targetDataSource);
        log.info(LOG_PREFIX + "Wrapped the DataSource in a MonitorConnectionGettingDataSourceProxy, so that we know"
                + " whether the stage or initialization actually used a SQL Connection : ["
                + monitorConnectionGettingDataSourceProxy + "].");

        // Wrap this again in a LazyConnectionDataSourceProxy, so that we do not _actually_ get a SQL Connection unless
        // the stage or init actually does any data access.
        // NOTICE: If the DataSource we are provided with already is a LazyConnectionDataSourceProxy, this is not a
        // problem: We will have two levels of lazy-ness, which is an absolutely minuscule cost.
        // NOTICE: We use the variant of NOT providing the DataSource at construction, because if we do provide it, it
        // leads to a Connection being gotten from the DataSource to determine default constants, which is not needed in
        // our case. (We set those in the TransactionDefinition before commencing stage/init processing anyway)
        LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy lazyAndMonitoredConnectionDataSourceProxy = new LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy(
                monitorConnectionGettingDataSourceProxy);
        lazyAndMonitoredConnectionDataSourceProxy.setDefaultAutoCommit(false);
        lazyAndMonitoredConnectionDataSourceProxy.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        // ... now setting the DataSource, and this will NOT trigger fetching of a Connection.
        lazyAndMonitoredConnectionDataSourceProxy.setTargetDataSource(monitorConnectionGettingDataSourceProxy);
        log.info(LOG_PREFIX + ".. then wrapped the DataSource again in a LazyAndMonitoredConnectionDataSourceProxy,"
                + " so that we will not actually get a physical SQL Connection from the underlying DataSource unless"
                + " the stage or initialization performs data access with it : ["
                + lazyAndMonitoredConnectionDataSourceProxy + "].");

        return lazyAndMonitoredConnectionDataSourceProxy;
    }

    /**
     * Extension of {@link LazyConnectionDataSourceProxy} which implements {@link InfrastructureProxy}, and which
     * exposes the underlying functionality of {@link MonitorConnectionGettingDataSourceProxy_InfrastructureProxy}.
     * <p />
     * Read JavaDoc for {@link #wrapLazyConnectionDatasource(DataSource)}.
     */
    private static class LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy
            extends LazyConnectionDataSourceProxy implements InfrastructureProxy {
        private final MonitorConnectionGettingDataSourceProxy_InfrastructureProxy _monitorDataSource;

        public LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy(
                MonitorConnectionGettingDataSourceProxy_InfrastructureProxy monitorDataSource) {
            _monitorDataSource = monitorDataSource;
        }

        void enableThreadLocals() {
            _monitorDataSource.enableThreadLocals();
        }

        boolean wasConnectionGotten() {
            return _monitorDataSource.wasConnectionGotten();
        }

        void clearThreadLocals() {
            _monitorDataSource.clearThreadLocals();
        }

        @Override
        public Object getWrappedObject() {
            DataSource targetDataSource = getTargetDataSource();
            if (targetDataSource instanceof InfrastructureProxy) {
                return ((InfrastructureProxy) targetDataSource).getWrappedObject();
            }
            return targetDataSource;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + " for target DataSource [" + getTargetDataSource() + "]";
        }
    }

    /**
     * Wrapper of DataSource that keeps ThreadLocal state of whether a SQL Connection has actually been gotten from the
     * underlying DataSource.
     * <p />
     * Read JavaDoc for {@link #wrapLazyConnectionDatasource(DataSource)}.
     */
    private static class MonitorConnectionGettingDataSourceProxy_InfrastructureProxy
            extends DelegatingDataSource implements InfrastructureProxy {
        public MonitorConnectionGettingDataSourceProxy_InfrastructureProxy(DataSource targetDataSource) {
            super(targetDataSource);
        }

        private ThreadLocal<Boolean> _enabledThreadLocals = ThreadLocal.withInitial(() -> Boolean.FALSE);
        private ThreadLocal<Boolean> _connectionThreadLocal = ThreadLocal.withInitial(() -> Boolean.FALSE);

        void enableThreadLocals() {
            _enabledThreadLocals.set(Boolean.TRUE);
        }

        boolean wasConnectionGotten() {
            if (!_enabledThreadLocals.get()) {
                throw new IllegalStateException("The ThreadLocalConnection logic has not been enabled for this code"
                        + " path, so why is this method invoked?");
            }
            return _connectionThreadLocal.get();
        }

        void clearThreadLocals() {
            _enabledThreadLocals.set(Boolean.FALSE);
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
            // Actually fetch the Connection
            Connection connection = lambda.getConnection();
            // If enabled, store it in the ThreadLocal
            if (_enabledThreadLocals.get()) {
                _connectionThreadLocal.set(true);
            }
            return connection;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + " for target DataSource ["
                    + getTargetDataSource()
                    + "], ThreadLocal gotten Connection:[" + _connectionThreadLocal.get() + "]";
        }

        @Override
        public Object getWrappedObject() {
            DataSource targetDataSource = getTargetDataSource();
            if (targetDataSource instanceof InfrastructureProxy) {
                return ((InfrastructureProxy) targetDataSource).getWrappedObject();
            }
            return targetDataSource;
        }

        private static class DebugStacktrace extends Exception {
            public DebugStacktrace(String message) {
                super(message);
            }
        }
    }

    @Override
    public TransactionContext getTransactionContext(JmsMatsTxContextKey txContextKey) {
        // Get the TransactionDefinition for this JmsMatsTxContextKey, which is a constant afterwards.
        DefaultTransactionDefinition defaultTransactionDefinition = _transactionDefinitionFunction.apply(txContextKey);
        return new TransactionalContext_JmsAndSpringDstm(txContextKey, _platformTransactionManager,
                defaultTransactionDefinition, _dataSource);
    }

    /**
     * The {@link TransactionContext}-implementation for {@link JmsMatsTransactionManager_JmsAndSpringManagedSqlTx}.
     */
    private static class TransactionalContext_JmsAndSpringDstm extends TransactionalContext_Jms {
        private final static String LOG_PREFIX = "#SPRINGJMATS# ";

        private final PlatformTransactionManager _platformTransactionManager;
        private final DefaultTransactionDefinition _transactionDefinitionForThisContext;
        private final DataSource _dataSource;

        public TransactionalContext_JmsAndSpringDstm(
                JmsMatsTxContextKey txContextKey,
                PlatformTransactionManager platformTransactionManager,
                DefaultTransactionDefinition transactionDefinitionForThisContext,
                DataSource dataSource) {
            super(txContextKey);
            _platformTransactionManager = platformTransactionManager;
            _transactionDefinitionForThisContext = transactionDefinitionForThisContext;
            _dataSource = dataSource;
        }

        private class IsSqlConnectionEmployedSupplier implements Supplier<Boolean> {
            private Boolean _fixedValue;

            @Override
            public Boolean get() {
                // ?: Has the value been fixed? (i.e. the Mats stage lambda is finished)
                if (_fixedValue != null) {
                    // -> Yes, so return the fixed value
                    return _fixedValue;
                }
                // E-> No, not fixed (i.e. we're still within the Mats stage lambda)
                return isConnectionEmployed() ? Boolean.TRUE : Boolean.FALSE;
            }

            public boolean isConnectionEmployed() {
                // ?: Is this an instance of our "magic" wrapper?
                if (_dataSource instanceof LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy) {
                    // -> Yes, magic, thus ask whether the Connection was /actually/ employed.
                    log.debug("The Spring TransactionManager is employing our \"magic\" DataSource proxy.");
                    return ((LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy) _dataSource)
                            .wasConnectionGotten();
                }
                // E-> No, not magic - so then check if the TransactionSynchronizationManager has gotten it yet.
                log.debug("The Spring TransactionManager is NOT employing our \"magic\" DataSource proxy.");
                /*
                 * Since the logic in the doTransaction(..) below always goes into SQL Transactional demarcation, we
                 * will always retrieve a Connection, and thus 'return true' is pretty much always correct.
                 *
                 * NOTE: We COULD have checked if the TransactionSynchronizationManager.getResource(_dataSource) had a
                 * ConnectionHolder, which had a ConnectionHandle (which by logic above always is true), and then
                 * checked if the DataSourceUtil.getConnection(dataSource) by any chance was a
                 * LazyConnectionDataSourceProxy, and then introspected that for whether the Connection was gotten. But
                 * why could you not then instead use our "magic" proxy?
                 */
                return true;
            }

            public void fixSqlConnectionEmployedValue() {
                _fixedValue = isConnectionEmployed();
            }
        }

        @Override
        public void doTransaction(JmsMatsMessageContext jmsMatsMessageContext, ProcessingLambda lambda)
                throws JmsMatsJmsException {
            IsSqlConnectionEmployedSupplier sqlConnectionEmployedSupplier = new IsSqlConnectionEmployedSupplier();
            try {
                // :? If we have a "magic" DataSource, then enable the ThreadLocal logic
                if (_dataSource instanceof LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy) {
                    // -> Yes, magic, thus ask whether the Connection was /actually/ employed.
                    ((LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy) _dataSource)
                            .enableThreadLocals();
                }

                // :: Make the potential SQL Connection available
                // Notice how we here use the DataSourceUtils class, so that we get the tx ThreadLocal Connection.
                // Read more at both DataSourceUtils and DataSourceTransactionManager.
                jmsMatsMessageContext.setSqlConnectionSupplier(() -> DataSourceUtils.getConnection(_dataSource));
                jmsMatsMessageContext.setSqlConnectionEmployedSupplier(sqlConnectionEmployedSupplier);

                // :: We invoke the "outer" transaction, which is the JMS transaction.
                super.doTransaction(jmsMatsMessageContext, () -> {
                    // ----- We're *within* the JMS Transaction demarcation.

                    // :: Now go into the SQL Transaction demarcation
                    TransactionStatus transactionStatus = _platformTransactionManager.getTransaction(
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
                        try { // try-finally: Fix value of 'isConnectionEmployed'-supplier.
                            lambda.performWithinTransaction();
                        }
                        finally {
                            // NOTICE: This must NOT be moved to later finally block! Point is to fix the value of
                            // whether the Connection was employed BEFORE doing commit or rollback (which will clear
                            // the value if not using "magic" proxy).
                            sqlConnectionEmployedSupplier.fixSqlConnectionEmployedValue();
                        }
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
                System.out.println(
                        "################## About to exit the Transactional Demarcation - was SQL Connection employed: ["
                                + sqlConnectionEmployedSupplier.get() + "].");
                // ?: Do we have "monitoring" of the getting of Connection from DataSource?
                if (_dataSource instanceof LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy) {
                    // -> Yes, we have monitoring - so we must now clear the ThreadLocal of any gotten Connection.
                    ((LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy) _dataSource)
                            .clearThreadLocals();
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
            // NOTICE: THE FOLLOWING if-STATEMENT IS JUST FOR LOGGING!
            // ?: Was connection gotten by code in ProcessingLambda (user code)
            // NOTICE: We must commit or rollback the Spring TransactionManager nevertheless, to clean up
            if ((_dataSource instanceof LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy)
                    && !((LazyAndMonitoredConnectionDataSourceProxy_InfrastructureProxy) _dataSource)
                            .wasConnectionGotten()) {
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
                        _platformTransactionManager.rollback(transactionStatus);
                        // Throw out.
                        throw new MatsSqlCommitOrRollbackFailedException(msg);
                    }
                    // E-> No, we were not in "RollbackOnly" - so commit this stuff, and get out.
                    _platformTransactionManager.commit(transactionStatus);
                    if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "Committed SQL Transaction ["
                            + transactionStatus + "].");
                }
                else {
                    // -> Rollback.
                    _platformTransactionManager.rollback(transactionStatus);
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
