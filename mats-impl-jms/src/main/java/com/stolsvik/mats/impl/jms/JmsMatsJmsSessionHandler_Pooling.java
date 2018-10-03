package com.stolsvik.mats.impl.jms;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Simple.JmsSessionHolder_Simple;
import com.stolsvik.mats.impl.jms.JmsMatsStage.JmsMatsStageProcessor;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.JmsMatsTxContextKey;

public class JmsMatsJmsSessionHandler_Pooling implements JmsMatsJmsSessionHandler {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsJmsSessionHandler_Simple.class);

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
        JmsSessionHolder jmsSessionHolder = getSessionHolder_internal(initiator);
        if (log.isDebugEnabled()) log.debug("getSessionHolder(...) for Initiator [" + initiator + "], returning ["
                + jmsSessionHolder + "].");
        return jmsSessionHolder;
    }

    @Override
    public JmsSessionHolder getSessionHolder(JmsMatsStageProcessor<?, ?, ?, ?> stageProcessor)
            throws JmsMatsJmsException {
        JmsSessionHolder jmsSessionHolder = getSessionHolder_internal(stageProcessor);
        if (log.isDebugEnabled()) log.debug("getSessionHolder(...) for StageProcessor [" + stageProcessor
                + "], returning [" + jmsSessionHolder + "].");
        return jmsSessionHolder;
    }

    private JmsSessionHolder getSessionHolder_internal(JmsMatsTxContextKey txContextKey) throws JmsMatsJmsException {
        // Get the pooling key.
        Object poolingKey = derivePoolingKey(txContextKey);

        // :: Get-or-create ConnectionAndSession - record if we created it, as we then need to create the Connection
        boolean weCreatedConnectionAndSessions = false;
        ConnectionAndSessions connectionAndSessions;
        synchronized (this) {
            connectionAndSessions = _liveConnections.get(poolingKey);
            if (connectionAndSessions == null) {
                weCreatedConnectionAndSessions = true;
                connectionAndSessions = new ConnectionAndSessions(poolingKey);
                _liveConnections.put(poolingKey, connectionAndSessions);
            }
        }

        // ?: Was we the creator of this ConnectionAndSessions?
        if (weCreatedConnectionAndSessions) {
            // -> Yes, so we must create the JMS Connection
            connectionAndSessions.initializeByCreatingJmsConnection(txContextKey);
        }

        // ----- Either we got an existing JMS Connection (or the not-us fetcher got Exception) - or we just created it.

        // Get-or-create a new SessionHolder.
        // Synchronized internally
        return connectionAndSessions.getOrCreateAndEmploySessionHolder();
    }

    private IdentityHashMap<Object, ConnectionAndSessions> _liveConnections = new IdentityHashMap<>();
    private IdentityHashMap<Object, ConnectionAndSessions> _crashedConnections = new IdentityHashMap<>();

    class ConnectionAndSessions implements JmsMatsStatics {
        final Object _poolingKey;

        final Deque<JmsSessionHolderImpl> _availableSessionHolders = new ArrayDeque<>();
        final Set<JmsSessionHolderImpl> _employedSessionHolders = new HashSet<>();

        final CountDownLatch _countDownLatch = new CountDownLatch(1);

        ConnectionAndSessions(Object poolingKey) {
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
                // Also, will remove the newly created ConnectionAndSessions. No-one can have made a Session, and
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
                            "Waited too long for a Connection to appear in the ConnectionAndSessions instance.");
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

        synchronized JmsSessionHolderImpl getOrCreateAndEmploySessionHolder() throws JmsMatsJmsException {
            JmsSessionHolderImpl ret = _availableSessionHolders.pollFirst();
            if (ret != null) {
                _employedSessionHolders.add(ret);
                return ret;
            }
            // E-> No, there was no SessionHolder available, so we must make a new session
            return createAndEmploySessionHolder();
        }

        private JmsSessionHolderImpl createAndEmploySessionHolder() throws JmsMatsJmsException {
            assert Thread.holdsLock(this);
            // NOTE: Might throw if it wasn't ever created.
            Connection jmsConnection = getJmsConnection();

            // :: Create a new JMS Session and stick it into a SessionHolder, and employ it.
            try {
                // Create JMS Session from JMS Connection
                Session jmsSession = jmsConnection.createSession(true, Session.SESSION_TRANSACTED);
                // Stick it into a SessionHolder
                JmsSessionHolderImpl jmsSessionHolder = new JmsSessionHolderImpl(this, jmsSession);
                // Employ it.
                _employedSessionHolders.add(jmsSessionHolder);
                // Return it.
                return jmsSessionHolder;
            }
            catch (Throwable t) {
                // Bad stuff - create Exception for throwing, and crashing entire ConnectionAndSessions
                JmsMatsJmsException e = new JmsMatsJmsException("Got problems when trying to create a new JMS"
                        + " Session from JMS Connection [" + jmsConnection + "].", t);
                // :: Crash this ConnectionAndSessions
                // Need a dummy JmsSessionHolderImpl (The JMS Session is not touched by the crashed() method).
                crashed(new JmsSessionHolderImpl(this, null), e);
                // Throw it out.
                throw e;
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
            log.warn(LOG_PREFIX + "release() from [" + jmsSessionHolder + "] on [" + this
                    + "] - moving from 'employed' to 'available' set (currently: available:["
                    + _availableSessionHolders.size() + "], employed:[" + _employedSessionHolders.size() + "]).");
            synchronized (this) {
                _employedSessionHolders.remove(jmsSessionHolder);
                _availableSessionHolders.addFirst(jmsSessionHolder);
            }
        }

        /**
         * Invoked by SessionHolders when their {@link JmsSessionHolderImpl#close()} is invoked.
         *
         * @param jmsSessionHolder
         *            the session holder to be closed (also physically).
         */
        void close(JmsSessionHolderImpl jmsSessionHolder) {
            log.warn(LOG_PREFIX + "close() from [" + jmsSessionHolder + "] on [" + this
                    + "] - physically closing Session, and then removing from 'employed' set - not enpooling back to"
                    + " 'available' set. (currently: available:[" + _availableSessionHolders.size()
                    + "], employed:[" + _employedSessionHolders.size() + "]).");
            boolean closeJmsConnection = removeSessionHolderFromEmployed_AndRemoveConnectionAndSessionsIfEmpty(
                    jmsSessionHolder);
            // ?: Was this the last SessionHolder in use?
            if (closeJmsConnection) {
                // -> Yes, last SessionHolder in this ConnectionAndSessions, so close the actual JMS Connection
                // (this will also close any JMS Sessions, specifically the one in the closing SessionHolder)
                closeJmsConnection();
            }
            else {
                // -> No, not last SessionHolder, so just close SessionHolder's actual JMS Session
                try {
                    jmsSessionHolder._jmsSession.close();
                }
                catch (Throwable t) {
                    // Bad stuff - create Exception for throwing, and crashing entire ConnectionAndSessions
                    JmsMatsJmsException e = new JmsMatsJmsException("Got problems when trying to close JMS Session ["
                            + jmsSessionHolder._jmsSession + "] from [" + jmsSessionHolder + "].", t);
                    // Crash this ConnectionAndSessions
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
            log.warn(LOG_PREFIX + "crashed() from [" + jmsSessionHolder + "] on [" + this
                    + "] - crashing: Closing JMS Connection, which closes all its Sessions. (currently available:["
                    + _availableSessionHolders.size() + "], employed:[" + _employedSessionHolders.size() + "]).");
            // ?: Are we already crashed?
            if (_crashed_StackTrace != null) {
                // -> Yes, so then everything should already have been taken care of.
                log.info(LOG_PREFIX + " -> Already crashed and closed [" + this + "].");
                // NOTICE! Since the ConnectionAndSessions is already crashed, the JMS Connection is already closed,
                // which again implies that the JMS Session is already closed.
                // Thus, only need to remove the SessionHolder, the JMS Session is already closed.
                removeSessionHolderFromEmployed_AndRemoveConnectionAndSessionsIfEmpty(jmsSessionHolder);
                return;
            }
            // Lock both the whole pooler, and this ConnectionAndSession instance, to avoid having threads sneak
            // by and getting either an available Session, or the Connection.
            // Lock order: Bigger to more granular.
            log.info(LOG_PREFIX + "[" + this + "] Marking as crashed, clearing available SessionHolders,"
                    + " moving us from live to dead ConnectionAndSessions.");
            synchronized (JmsMatsJmsSessionHandler_Pooling.this) {
                synchronized (this) {
                    // Crash this
                    _crashed_StackTrace = new Exception("This [" + this + "] was crashed.", reasonException);
                    // Clear available SessionHolders (will close Connection, and thus Sessions, outside of synch).
                    _availableSessionHolders.clear();
                    // Removing this SessionHolder from employed
                    boolean closeJmsConnection = removeSessionHolderFromEmployed_AndRemoveConnectionAndSessionsIfEmpty(
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

        private boolean removeSessionHolderFromEmployed_AndRemoveConnectionAndSessionsIfEmpty(
                JmsSessionHolderImpl jmsSessionHolder) {
            // Lock order: Bigger to more granular.
            log.debug(LOG_PREFIX + "Removing [" + jmsSessionHolder + "] from employed-set.");
            synchronized (JmsMatsJmsSessionHandler_Pooling.this) {
                synchronized (this) {
                    // Remove from employed.
                    _employedSessionHolders.remove(jmsSessionHolder);
                    // ?: Is the ConnectionAndSessions now empty?
                    if (_employedSessionHolders.isEmpty() && _availableSessionHolders.isEmpty()) {
                        // -> Yes, none in either employed nor available set.
                        // Remove us from live map, if this is where we live
                        _liveConnections.remove(_poolingKey);
                        // Remove us fom dead map, if this is where we are
                        _crashedConnections.remove(_poolingKey);
                        // We removed the ConnectionAndSessions - so close the actual JMS Connection.
                        return true;
                    }
                    // E-> We did not remove the ConnectionAndSessions, so keep the JMS Connection open.
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
                log.info(LOG_PREFIX + "Got a [" + t.getClass().getSimpleName() + "] when trying to close a crashed"
                        + " JMS Connection. This is nothing to worry about.", t);
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
            return idThis() + (_crashed_StackTrace == null ? "[live]" : "[crashed]");
        }
    }

    public static class JmsSessionHolderImpl implements JmsSessionHolder, JmsMatsStatics {
        private static final Logger log = LoggerFactory.getLogger(
                JmsSessionHolder_Simple.class);

        private final ConnectionAndSessions _connectionAndSessions;
        private final Session _jmsSession;

        public JmsSessionHolderImpl(ConnectionAndSessions connectionAndSessions, Session jmsSession) {
            _connectionAndSessions = connectionAndSessions;
            _jmsSession = jmsSession;
        }

        @Override
        public void isSessionOk() throws JmsMatsJmsException {
            _connectionAndSessions.isConnectionStillActive();
        }

        @Override
        public Session getSession() {
            if (log.isDebugEnabled()) log.debug("getSession() on SessionHolder [" + this
                    + "] - returning it directly.");
            return _jmsSession;
        }

        @Override
        public void close() {
            _connectionAndSessions.close(this);
        }

        @Override
        public void release() {
            _connectionAndSessions.release(this);
        }

        @Override
        public void crashed(Throwable t) {
            _connectionAndSessions.crashed(this, t);
        }

        @Override
        public String toString() {
            return idThis();
        }
    }
}
