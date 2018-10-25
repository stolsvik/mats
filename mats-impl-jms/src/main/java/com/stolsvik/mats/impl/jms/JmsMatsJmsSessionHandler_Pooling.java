package com.stolsvik.mats.impl.jms;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.impl.jms.JmsMatsStage.JmsMatsStageProcessor;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.JmsMatsTxContextKey;

public class JmsMatsJmsSessionHandler_Pooling implements JmsMatsJmsSessionHandler {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsJmsSessionHandler_Pooling.class);

    private final JmsConnectionSupplier _jmsConnectionSupplier;

    private Object derivePoolingKey(JmsMatsTxContextKey txContextKey) {
        // ?: Is this an Initiator, or a StageProcessor?
        if (txContextKey instanceof JmsMatsInitiator) {
            // -> Initiator
            // Factory: One Connection is shared for all Initiators
            // The Initiator itself: Each Initiator gets a separate Connection
            // new Object(): Each /initiation/ gets its own Connection (don't do this!)
            return txContextKey.getFactory();
            // return txContextKey;
            // return new Object();
        }
        // E-> StageProcessor
        // Factory: Every StageProcessors in the entire Factory shares a Connection
        // Endpoint: The StageProcessors for all Stages in one Endpoint shares a Connection
        // Stage: The StageProcessors in one Stage shares a Connection
        // StageProcessor (i.e. the key itself): Each StageProcessor gets a separate Connection.
        return txContextKey.getStage().getParentEndpoint().getParentFactory();
        // return txContextKey.getStage().getParentEndpoint();
        // return txContextKey.getStage();
        // return txContextKey;

        // NOTICE! If you choose Factory on both, both initiators and endpoints will share a sole Connection.
    }

    public JmsMatsJmsSessionHandler_Pooling(JmsConnectionSupplier jmsConnectionSupplier) {
        _jmsConnectionSupplier = jmsConnectionSupplier;
    }

    @Override
    public JmsSessionHolder getSessionHolder(JmsMatsInitiator<?> initiator) throws JmsMatsJmsException {
        return getSessionHolder_internal(initiator);
    }

    @Override
    public JmsSessionHolder getSessionHolder(JmsMatsStageProcessor<?, ?, ?, ?> stageProcessor)
            throws JmsMatsJmsException {
        return getSessionHolder_internal(stageProcessor);
    }

    @Override
    public int closeAllAvailableSessions() {
        log.info("Closing all available SessionHolders in all pools, thus hoping to close all JMS Connections.");
        int liveConnectionsBefore;
        int availableSessionsClosed = 0;
        int liveConnectionsAfter;
        int employedSessions = 0;
        synchronized (this) {
            liveConnectionsBefore = _liveConnections.size();
            // Copying over the liveConnections, since it hopefully will be modified.
            ArrayList<ConnectionWithSessionPool> connWithSessionPool = new ArrayList<>(_liveConnections.values());
            for (ConnectionWithSessionPool connectionAndSession : connWithSessionPool) {
                availableSessionsClosed += connectionAndSession._availableSessionHolders.size();
                // Copying over the availableHolders, since it hopefully will be modified.
                ArrayList<JmsSessionHolderImpl> availableHolders = new ArrayList<>(
                        connectionAndSession._availableSessionHolders);
                for (JmsSessionHolderImpl availableHolder : availableHolders) {
                    connectionAndSession.internalClose(availableHolder);
                }
            }
            liveConnectionsAfter = _liveConnections.size();
            for (ConnectionWithSessionPool connectionAndSession : connWithSessionPool) {
                employedSessions += connectionAndSession._employedSessionHolders.size();
            }
        }
        log.info(" \\- Live Connections before closing Sessions:[" + liveConnectionsBefore
                + "], Available Sessions (now closed):[" + availableSessionsClosed
                + "], Live Connections after closing:[" + liveConnectionsAfter + "], Employed Sessions:["
                + employedSessions + "].");

        return liveConnectionsAfter;
    }

    private JmsSessionHolder getSessionHolder_internal(JmsMatsTxContextKey txContextKey) throws JmsMatsJmsException {
        // Get the pooling key.
        Object poolingKey = derivePoolingKey(txContextKey);

        // :: Get-or-create ConnectionAndSession - record if we created it, as we then need to create the Connection
        boolean weCreatedConnectionWithSessionPool = false;
        ConnectionWithSessionPool connectionWithSessionPool;
        synchronized (this) {
            connectionWithSessionPool = _liveConnections.get(poolingKey);
            if (connectionWithSessionPool == null) {
                weCreatedConnectionWithSessionPool = true;
                connectionWithSessionPool = new ConnectionWithSessionPool(poolingKey);
                _liveConnections.put(poolingKey, connectionWithSessionPool);
            }
        }

        // ?: Was we the creator of this ConnectionWithSessionPool?
        if (weCreatedConnectionWithSessionPool) {
            // -> Yes, so we must create the JMS Connection
            connectionWithSessionPool.initializeByCreatingJmsConnection(txContextKey);
        }

        // ----- Either we got an existing JMS Connection (or the not-us fetcher got Exception) - or we just created it.

        // Get-or-create a new SessionHolder.
        // Synchronized internally
        JmsSessionHolderImpl jmsSessionHolder = connectionWithSessionPool.getOrCreateAndEmploySessionHolder(
                txContextKey);
        if (log.isDebugEnabled()) log.debug("getSessionHolder(...) for [" + txContextKey + "], derived pool ["
                + connectionWithSessionPool + "], returning [" + jmsSessionHolder + "].");
        return jmsSessionHolder;
    }

    private IdentityHashMap<Object, ConnectionWithSessionPool> _liveConnections = new IdentityHashMap<>();
    private IdentityHashMap<Object, ConnectionWithSessionPool> _crashedConnections = new IdentityHashMap<>();

    class ConnectionWithSessionPool implements JmsMatsStatics {
        final Object _poolingKey;

        final Deque<JmsSessionHolderImpl> _availableSessionHolders = new ArrayDeque<>();
        final Set<JmsSessionHolderImpl> _employedSessionHolders = new HashSet<>();

        final CountDownLatch _countDownLatch = new CountDownLatch(1);

        ConnectionWithSessionPool(Object poolingKey) {
            _poolingKey = poolingKey;
        }

        Connection _jmsConnection;
        Throwable _exceptionWhenCreatingConnection;

        void initializeByCreatingJmsConnection(JmsMatsTxContextKey txContextKey) throws JmsMatsJmsException {
            try {
                Connection jmsConnection = _jmsConnectionSupplier.createJmsConnection(txContextKey);
                // Starting it right away, as that could potentially also give "connection establishment" JMSExceptions
                jmsConnection.start();
                setConnectionOrException_ReleaseWaiters(jmsConnection, null);
            }
            catch (Throwable t) {
                // Got problems - set the Exception, so that any others that got waiting on connection can throw out.
                // Also, will remove the newly created ConnectionWithSessionPool. No-one can have made a Session, and
                // the next guy in should start anew.
                setConnectionOrException_ReleaseWaiters(null, t);
                synchronized (JmsMatsJmsSessionHandler_Pooling.this) {
                    _liveConnections.remove(_poolingKey);
                }
                throw new JmsMatsJmsException("Got problems when trying to create & start a new JMS Connection.", t);
            }
        }

        void setConnectionOrException_ReleaseWaiters(Connection jmsConnection, Throwable t) {
            _jmsConnection = jmsConnection;
            _exceptionWhenCreatingConnection = t;
            _countDownLatch.countDown();
        }

        private Connection getJmsConnection() throws JmsMatsJmsException {
            try {
                boolean ok = _countDownLatch.await(10, TimeUnit.SECONDS);
                if (!ok) {
                    throw new JmsMatsJmsException(
                            "Waited too long for a Connection to appear in the ConnectionWithSessionPool instance.");
                }
                if (_exceptionWhenCreatingConnection != null) {
                    throw new JmsMatsJmsException("Someone else got Exception when they tried to create Connection.",
                            _exceptionWhenCreatingConnection);
                }
                else {
                    return _jmsConnection;
                }
            }
            catch (InterruptedException e) {
                throw new JmsMatsJmsException("Got interrupted while waiting for a Connection to appear in the"
                        + " ConnectionAndSession instance.", e);
            }
        }

        JmsSessionHolderImpl getOrCreateAndEmploySessionHolder(JmsMatsTxContextKey txContextKey)
                throws JmsMatsJmsException {
            synchronized (this) {
                JmsSessionHolderImpl availableSessionHolder = _availableSessionHolders.pollFirst();
                if (availableSessionHolder != null) {
                    availableSessionHolder.setCurrentContext(txContextKey);
                    _employedSessionHolders.add(availableSessionHolder);
                    return availableSessionHolder;
                }
                // E-> No, there was no SessionHolder available, so we must make a new session
                // NOTE: Might throw if it was attempted created by someone else (concurrently), which threw.
                Connection jmsConnection = getJmsConnection();

                // :: Create a new JMS Session and stick it into a SessionHolder, and employ it.
                try {
                    // Create JMS Session from JMS Connection
                    Session jmsSession = jmsConnection.createSession(true, Session.SESSION_TRANSACTED);
                    // Create the default MessageProducer
                    MessageProducer messageProducer = jmsSession.createProducer(null);
                    // Stick them into a SessionHolder
                    JmsSessionHolderImpl jmsSessionHolder = new JmsSessionHolderImpl(txContextKey, this, jmsSession,
                            messageProducer);
                    // Employ it.
                    _employedSessionHolders.add(jmsSessionHolder);
                    // Return it.
                    return jmsSessionHolder;
                }
                catch (Throwable t) {
                    // Bad stuff - create Exception for throwing, and crashing entire ConnectionWithSessionPool
                    JmsMatsJmsException e = new JmsMatsJmsException("Got problems when trying to create a new JMS"
                            + " Session from JMS Connection [" + jmsConnection + "].", t);
                    // :: Crash this ConnectionWithSessionPool
                    // Need a dummy JmsSessionHolderImpl (The JMS objects are not touched by the crashed() method).
                    crashed(new JmsSessionHolderImpl(txContextKey, this, null, null), e);
                    // Throw it out.
                    throw e;
                }
            }
        }

        private volatile Exception _crashed_StackTrace;

        /**
         * Invoked by SessionHolders when their {@link JmsSessionHolderImpl#release()} is invoked.
         *
         * @param jmsSessionHolder
         *            the session holder to be returned.
         */
        void release(JmsSessionHolderImpl jmsSessionHolder) {
            if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "[" + this + "] release() from [" + jmsSessionHolder + "]"
                    + " - moving from 'employed' to 'available' set.");
            jmsSessionHolder.setCurrentContext("available");
            if (_crashed_StackTrace != null) {
                internalClose(jmsSessionHolder);
            }
            else {
                synchronized (this) {
                    jmsSessionHolder.setCurrentContext("available");
                    _employedSessionHolders.remove(jmsSessionHolder);
                    _availableSessionHolders.addFirst(jmsSessionHolder);
                }
            }
        }

        /**
         * Invoked by SessionHolders when their {@link JmsSessionHolderImpl#close()} is invoked.
         *
         * @param jmsSessionHolder
         *            the session holder to be closed (also physically).
         */
        void close(JmsSessionHolderImpl jmsSessionHolder) {
            if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "[" + this + "] close() from [" + jmsSessionHolder + "]"
                    + " - removing from pool and then physically closing JMS Session.");
            internalClose(jmsSessionHolder);
        }

        private void internalClose(JmsSessionHolderImpl jmsSessionHolder) {
            jmsSessionHolder.setCurrentContext("closed");
            // Remove this SessionHolder from pool, and remove ConnectionWithSessionPool if empty (if so, returns true)
            boolean closeJmsConnection = removeSessionHolderFromPool_AndRemoveConnectionAndSessionsIfEmpty(
                    jmsSessionHolder);
            // ?: Was this the last SessionHolder in use?
            if (closeJmsConnection) {
                // -> Yes, last SessionHolder in this ConnectionWithSessionPool, so close the actual JMS Connection
                // (NOTICE! This will also close any JMS Sessions, specifically the one in the closing SessionHolder)
                closeJmsConnection();
            }
            else {
                // -> No, not last SessionHolder, so just close this SessionHolder's actual JMS Session
                try {
                    jmsSessionHolder._jmsSession.close();
                }
                catch (Throwable t) {
                    // Bad stuff - create Exception for throwing, and crashing entire ConnectionWithSessionPool
                    JmsMatsJmsException e = new JmsMatsJmsException("Got problems when trying to close JMS Session ["
                            + jmsSessionHolder._jmsSession + "] from [" + jmsSessionHolder + "].", t);
                    // Crash this ConnectionWithSessionPool
                    crashed(jmsSessionHolder, e);
                    // Not throwing on, per contract.
                }
            }
        }

        /**
         * Invoked by SessionHolders when their {@link JmsSessionHolderImpl#crashed(Throwable)} is invoked.
         *
         * @param jmsSessionHolder
         *            the session holder that crashed, which will be closed (also physically)
         * @param reasonException
         *            the Exception that was deemed as a JMS crash.
         */
        void crashed(JmsSessionHolderImpl jmsSessionHolder, Throwable reasonException) {
            jmsSessionHolder.setCurrentContext("crashed");
            if (log.isWarnEnabled()) log.warn(LOG_PREFIX + "[" + this + "] crashed() from [" + jmsSessionHolder
                    + "] - crashing: Closing JMS Connection, which closes all its Sessions.");

            // ?: Are we already crashed?
            if (_crashed_StackTrace != null) {
                // -> Yes, so then everything should already have been taken care of.
                if (log.isInfoEnabled()) log.info(LOG_PREFIX + " -> Already crashed and closed.");
                // NOTICE! Since the ConnectionWithSessionPool is already crashed, the JMS Connection is already closed,
                // which again implies that the JMS Session is already closed.
                // Thus, only need to remove the SessionHolder, the JMS Session is already closed.
                removeSessionHolderFromPool_AndRemoveConnectionAndSessionsIfEmpty(jmsSessionHolder);
                return;
            }
            // Lock both the whole Handler, and this ConnectionWithSessionPool instance, to avoid having threads sneak
            // by and getting either an available Session, or the Connection.
            // Lock order: Bigger to smaller objects.
            if (log.isInfoEnabled()) log.info(LOG_PREFIX + "[" + this
                    + "] Marking as crashed, clearing available SessionHolders, moving us from"
                    + " live to dead ConnectionWithSessionPool.");
            assertBigToSmallLockOrder();
            synchronized (JmsMatsJmsSessionHandler_Pooling.this) {
                synchronized (this) {
                    // Crash this
                    _crashed_StackTrace = new Exception("This [" + this + "] was crashed.", reasonException);
                    // Clear available SessionHolders (will close Connection, and thus Sessions, outside of synch).
                    _availableSessionHolders.clear();
                    // Removing this SessionHolder from employed
                    boolean closeJmsConnection = removeSessionHolderFromPool_AndRemoveConnectionAndSessionsIfEmpty(
                            jmsSessionHolder);
                    // ?: Was this the last session?
                    if (!closeJmsConnection) {
                        // -> No, it was not the last session, so move us to the crashed-set
                        // Remove us from the live connections set.
                        _liveConnections.remove(_poolingKey);
                        // Add us to the dead set
                        _crashedConnections.put(_poolingKey, this);
                    }
                    /*
                     * NOTE: Any other employed SessionHolders will invoke isConnectionStillActive(), and find that it
                     * is not, and come back with close(). Otherwise, they will also come get a JMS Exception and come
                     * back with crashed().
                     */
                }
            }
            // :: Now close the JMS Connection, since this was a crash, and we want to get rid of it.
            // Closing JMS Connection will per JMS API close all Sessions, Consumers and Producers.
            closeJmsConnection();
        }

        private void assertBigToSmallLockOrder() {
            // If we at this point only have 'this' locked, and not "mother", then we're screwed.
            // Both none locked, and both locked, is OK.
            if (Thread.holdsLock(this) && (!Thread.holdsLock(JmsMatsJmsSessionHandler_Pooling.this))) {
                throw new AssertionError("When locking both '"
                        + JmsMatsJmsSessionHandler_Pooling.class.getSimpleName()
                        + "' and '" + ConnectionWithSessionPool.class.getSimpleName() + "', one shall not"
                        + " start by having the pool locked, as that is the smaller, and the defined"
                        + " locking order is big to small.");
            }
        }

        private boolean removeSessionHolderFromPool_AndRemoveConnectionAndSessionsIfEmpty(
                JmsSessionHolderImpl jmsSessionHolder) {
            if (log.isInfoEnabled()) log.info(LOG_PREFIX + "[" + this + "] Removing [" + jmsSessionHolder
                    + "] from pool.");

            // Lock both the whole Handler, and this ConnectionWithSessionPool instance, to avoid having threads sneak
            // by and getting either an available Session, or the Connection.
            // Lock order: Bigger to smaller objects.
            assertBigToSmallLockOrder();
            synchronized (JmsMatsJmsSessionHandler_Pooling.this) {
                synchronized (this) {
                    // Remove from employed (this is the normal place a SessionHolder live)
                    _employedSessionHolders.remove(jmsSessionHolder);
                    // Remove from available (this is where a SessionHolder lives if the pool is shutting down)
                    _availableSessionHolders.remove(jmsSessionHolder);
                    // ?: Is the ConnectionWithSessionPool now empty?
                    if (_employedSessionHolders.isEmpty() && _availableSessionHolders.isEmpty()) {
                        // -> Yes, none in either employed nor available set.
                        // Remove us from live map, if this is where this ConnectionWithSessionPool live
                        _liveConnections.remove(_poolingKey);
                        // Remove us fom dead map, if this is where this ConnectionWithSessionPool are
                        _crashedConnections.remove(_poolingKey);
                        // We removed the ConnectionWithSessionPool - so close the actual JMS Connection.
                        return true;
                    }
                    // E-> We did not remove the ConnectionWithSessionPool, so keep the JMS Connection open.
                    return false;
                }
            }
        }

        private void closeJmsConnection() {
            log.info(LOG_PREFIX + "[" + this + "] Closing JMS Connection [" + _jmsConnection + "]");
            try {
                _jmsConnection.close();
            }
            catch (Throwable t) {
                log.info(LOG_PREFIX + "[" + this + "] Got a [" + t.getClass().getSimpleName()
                        + "] when trying to close a crashed JMS Connection. Ignoring.", t);
            }
        }

        /**
         * Will be invoked by all SessionHolders at various times in {@link JmsMatsStage}.
         */
        void isConnectionStillActive() throws JmsMatsJmsException {
            if (_crashed_StackTrace != null) {
                throw new JmsMatsJmsException("Connection is crashed.", _crashed_StackTrace);
            }
            JmsMatsActiveMQSpecifics.isConnectionLive(_jmsConnection);
        }

        @Override
        public String toString() {
            int available, employed;
            synchronized (this) {
                available = _availableSessionHolders.size();
                employed = _employedSessionHolders.size();
            }
            return idThis() + "{" + (_crashed_StackTrace == null ? "live" : "crashed") + "!avail:" + available
                    + ",empl:" + employed + "}";
        }
    }

    public static class JmsSessionHolderImpl implements JmsSessionHolder, JmsMatsStatics {
        private final ConnectionWithSessionPool _connectionWithSessionPool;
        private final Session _jmsSession;
        private final MessageProducer _messageProducer;

        public JmsSessionHolderImpl(JmsMatsTxContextKey txContextKey,
                ConnectionWithSessionPool connectionWithSessionPool,
                Session jmsSession,
                MessageProducer messageProducer) {
            _currentContext = txContextKey;
            _connectionWithSessionPool = connectionWithSessionPool;
            _jmsSession = jmsSession;
            _messageProducer = messageProducer;
        }

        private Object _currentContext;

        private void setCurrentContext(Object currentContext) {
            _currentContext = currentContext;
        }

        @Override
        public void isSessionOk() throws JmsMatsJmsException {
            _connectionWithSessionPool.isConnectionStillActive();
        }

        @Override
        public Session getSession() {
            return _jmsSession;
        }

        @Override
        public MessageProducer getDefaultNoDestinationMessageProducer() {
            return _messageProducer;
        }

        @Override
        public void close() {
            _connectionWithSessionPool.close(this);
        }

        @Override
        public void release() {
            _connectionWithSessionPool.release(this);
        }

        @Override
        public void crashed(Throwable t) {
            _connectionWithSessionPool.crashed(this, t);
        }

        @Override
        public String toString() {
            return idThis() + "{pool:@" + Integer.toHexString(System.identityHashCode(_connectionWithSessionPool))
                    + ",ctx:" + _currentContext + "}";
        }
    }
}
