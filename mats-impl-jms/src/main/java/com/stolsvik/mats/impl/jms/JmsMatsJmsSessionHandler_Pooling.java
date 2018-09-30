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

import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Simple.JmsSessionHolder_Simple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private IdentityHashMap<Object, ConnectionAndSessions> _deadConnections = new IdentityHashMap<>();

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
                // Crash this ConnectionAndSessions
                crashed(e);
                // Throw it out.
                throw e;
            }
        }

        private volatile Exception _crashedFromStackTrace;

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
            synchronized (this) {
                _employedSessionHolders.remove(jmsSessionHolder);
            }
            try {
                jmsSessionHolder._jmsSession.close();
            }
            catch (Throwable t) {
                // Bad stuff - create Exception for throwing, and crashing entire ConnectionAndSessions
                JmsMatsJmsException e = new JmsMatsJmsException("Got problems when trying to close JMS Session ["+jmsSessionHolder._jmsSession+"] from ["+jmsSessionHolder+"].", t);
                // Crash this ConnectionAndSessions
                crashed(e);
                // Not throwing on, per contract.
            }
        }

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
         * Invoked by SessionHolders when their {@link JmsSessionHolderImpl#crashed(Throwable)} is invoked.
         *
         * @param reasonException
         *            the Exception that was deemed as a JMS crash.
         */
        void crashed(Throwable reasonException) {
            log.warn(LOG_PREFIX + "[" + this + "] is crashing - closing down any SessionHolders (available:["
                    + _availableSessionHolders.size() + "], employed:[" + _employedSessionHolders.size() + "]).");
            // ?: Are we already crashed?
            if (_crashedFromStackTrace != null) {
                // -> Yes, so then everything should already have been taken care of.
                log.info(LOG_PREFIX + " -> Already crashed and closed [" + this + "].");
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
                    _crashedFromStackTrace = new Exception("This [" + this + "] was crashed.", reasonException);
                    // Clear available sessions (will close Connection, and thus Sessions, outside of synch).
                    _availableSessionHolders.clear();
                    // NOTE: All the employed sessions will invoke isConnectionStillActive(), and find that it is not.
                }
                // Remove us from the live connections set.
                _liveConnections.remove(_poolingKey);
                // Add us to the dead set
                _deadConnections.put(_poolingKey, this);
            }
            // :: Now close the JMS Connection, which (per API) will close all Sessions, and Consumers and Producers.
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
         * Will be invoked by all SessionHolders at various times.
         *
         * TODO: Probably OK to throw a MatsBackendExcption here, as the cleaning already performed in such
         * circumstances will be the correct procedure.
         * 
         * @return {@code true} if JMS Connection is still active.
         */
        boolean isConnectionStillActive() throws JmsMatsJmsException {
            if (_crashedFromStackTrace != null) {
                return false;
            }
            return JmsMatsActiveMQSpecifics.isConnectionLive(_jmsConnection);
        }

        @Override
        public String toString() {
            return idThis();
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
        public boolean isSessionStillActive() throws JmsMatsJmsException {
            return _connectionAndSessions.isConnectionStillActive();
        }

        @Override
        public Session getSession() {
            if (log.isDebugEnabled()) log.debug("getSession() on SessionHolder [" + this
                    + "] - returning it directly.");
            return _jmsSession;
        }

        @Override
        public void close() {
            if (log.isDebugEnabled()) log.debug("close() on SessionHolder [" + this + "] - invoking mother.");
            _connectionAndSessions.close(this);
        }

        @Override
        public void release() {
            if (log.isDebugEnabled()) log.debug("release() on SessionHolder [" + this + "] - invoking mother.");
            _connectionAndSessions.release(this);
        }

        @Override
        public void crashed(Throwable t) {
            if (log.isDebugEnabled()) log.debug("crashed() on SessionHolder [" + this + "] - invoking mother", t);
            _connectionAndSessions.crashed(t);
        }

        @Override
        public String toString() {
            return idThis();
        }
    }
}
