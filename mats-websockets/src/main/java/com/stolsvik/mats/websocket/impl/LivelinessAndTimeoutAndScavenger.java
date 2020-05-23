package com.stolsvik.mats.websocket.impl;

import java.time.Instant;
import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.websocket.ClusterStoreAndForward;
import com.stolsvik.mats.websocket.MatsSocketServer.SessionRemovedEvent.SessionRemovedEventType;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer.SessionRemovedEventImpl;

/**
 * Liveliness Updater and Timeouter and Session Remnants Scavenger: Notifies the CSAF every X milliseconds (which should
 * measure in 1 or single-digit minutes) about which MatsSocketSessionIds that are active, and every Y milliseconds
 * (which should measure in minutes) times out session that are older than T milliseconds (which should measure in hours
 * or days), and every Z milliseconds (which should measure in deca-minutes) scavenges for session remnants.
 *
 * @author Endre Stølsvik 2020-05-17 21:29 - http://stolsvik.com/, endre@stolsvik.com
 */
class LivelinessAndTimeoutAndScavenger implements MatsSocketStatics {
    private static final Logger log = LoggerFactory.getLogger(
            LivelinessAndTimeoutAndScavenger.class);

    // :: From Constructor params
    private final DefaultMatsSocketServer _matsSocketServer;
    private final ClusterStoreAndForward _csaf;
    private final long _millisBetweenLivelinessUpdate;
    private final long _millisBetweenTimeoutRun;
    private final long _millisBetweenScavengeSessionRemnantsRun;
    private final Supplier<Long> _timeoutSessionsOlderThanTimestampSupplier;

    // :: Constructor inited
    private final Thread _sessionLivelinessUpdaterThread;
    private final Thread _sessionTimeouterThread;
    private final Thread _scavengeSessionRemnantsThread;

    LivelinessAndTimeoutAndScavenger(DefaultMatsSocketServer matsSocketServer,
            ClusterStoreAndForward csaf,
            long millisBetweenLivelinessUpdate, long millisBetweenTimeoutRun,
            long millisBetweenScavengeSessionRemnantsRun, Supplier<Long> timeoutSessionsOlderThanTimestampSupplier) {
        _matsSocketServer = matsSocketServer;
        _csaf = csaf;
        _millisBetweenLivelinessUpdate = millisBetweenLivelinessUpdate;
        _millisBetweenTimeoutRun = millisBetweenTimeoutRun;
        _millisBetweenScavengeSessionRemnantsRun = millisBetweenScavengeSessionRemnantsRun;
        _timeoutSessionsOlderThanTimestampSupplier = timeoutSessionsOlderThanTimestampSupplier;

        _sessionLivelinessUpdaterThread = new Thread(this::sessionLivelinessUpdaterRunnable,
                THREAD_PREFIX + "CSAF LivelinessUpdater for " + _matsSocketServer.serverId());
        _sessionLivelinessUpdaterThread.start();

        _sessionTimeouterThread = new Thread(this::sessionTimeouterRunnable,
                THREAD_PREFIX + "CSAF SessionTimeouter for " + _matsSocketServer.serverId());
        _sessionTimeouterThread.start();

        _scavengeSessionRemnantsThread = new Thread(this::scavengeSessionRemnantsRunnable,
                THREAD_PREFIX + "CSAF ScavengeSessionRemnants for " + _matsSocketServer.serverId());
        _scavengeSessionRemnantsThread.start();

        log.info("Created [" + this.getClass().getSimpleName() + "] and Threads for"
                + " [" + _matsSocketServer.serverId() + "]");
    }

    private volatile boolean _runFlag = true;
    private final Object _timeoutThreadSynchAndSleepObject = new Object();

    void shutdown(int gracefulShutdownMillis) {
        _runFlag = false;
        log.info("Shutting down [" + this.getClass().getSimpleName() + "] for [" + _matsSocketServer.serverId()
                + "]..");

        // Interrupt the threads that aren't important to let finish.
        log.info(".. thread [" + _sessionLivelinessUpdaterThread + "]");
        _sessionLivelinessUpdaterThread.interrupt();
        log.info(".. thread [" + _scavengeSessionRemnantsThread + "]");
        _scavengeSessionRemnantsThread.interrupt();

        /*
         * Using a notify-style wakeup on the timeouter, to be able to shutdown nicely. Problem is that if the thread is
         * just running notification on the event listeners, then we do not want to /interrupt/ it. So instead notify it
         * nicely, asking it to shut down. If this doesn't work out, we interrupt.
         */
        // Notify it, in the 99.99% case that it is sleeping between timeouts.
        log.info(".. thread [" + _sessionTimeouterThread + "]");
        synchronized (_timeoutThreadSynchAndSleepObject) {
            _timeoutThreadSynchAndSleepObject.notifyAll();
        }
        // .. now check that it got the message.
        try {
            // Try to join the thread for a while
            _sessionTimeouterThread.join(gracefulShutdownMillis);
            // ?: Did it exit nicely?
            if (_sessionTimeouterThread.isAlive()) {
                // -> No, so interrupt it.
                log.info("The sessionTimeouter Thread didn't exit nicely, so we now interrupt it.");
                _sessionTimeouterThread.interrupt();
            }
        }
        catch (InterruptedException e) {
            log.info("Got interrupted while waiting for TimeoutThread to exit nicely. Interrupting it instead.");
            _sessionTimeouterThread.interrupt();
        }
    }

    private void sessionLivelinessUpdaterRunnable() {
        // Some constant randomness - which over time will spread out the updates on the different nodes.
        long randomExtraSleep = (int) (Math.random() * _millisBetweenLivelinessUpdate * 0.1);
        log.info("Started: CSAF LivelinessUpdater for [" + _matsSocketServer.serverId()
                + "], Thread [" + Thread.currentThread() + "]");
        while (_runFlag) {
            try {
                Thread.sleep(_millisBetweenLivelinessUpdate + randomExtraSleep);
            }
            catch (InterruptedException e) {
                log.debug("Got interrupted while sleeping between updating Session Liveliness,"
                        + " assuming shutdown, looping to check runFlag.");
                continue;
            }

            // NOTICE! The Map we do keySet() of is a ConcurrentMap, thus the keyset is "active". No problem.
            Set<String> matsSocketSessionIds = _matsSocketServer.getLiveMatsSocketSessions().keySet();
            log.debug("Updating CSAF Session Liveliness of about [" + matsSocketSessionIds.size()
                    + "] active MatsSocketSessions for [" + _matsSocketServer.serverId() + "]");
            if (!matsSocketSessionIds.isEmpty()) {
                try {
                    _csaf.notifySessionLiveliness(matsSocketSessionIds);
                }
                catch (Throwable t) {
                    log.warn("Got problems updating CSAF Session Liveliness of about [" + matsSocketSessionIds
                            .size() + "] MatsSocketSessions.", t);
                }
            }
        }
        log.info("Exiting: CSAF LivelinessUpdater Thread for [" + _matsSocketServer.serverId() + "].");
    }

    private void sessionTimeouterRunnable() {
        // Some constant randomness - which over time will spread out the updates on the different nodes.
        long randomExtraSleep = (int) (Math.random() * _millisBetweenTimeoutRun * 0.1);
        log.info("Started: CSAF SessionTimeouter Thread for [" + _matsSocketServer.serverId()
                + "], Thread [" + Thread.currentThread() + "]");
        while (_runFlag) {
            synchronized (_timeoutThreadSynchAndSleepObject) {
                try {
                    _timeoutThreadSynchAndSleepObject.wait(_millisBetweenTimeoutRun + randomExtraSleep);
                }
                catch (InterruptedException e) {
                    log.debug("Got interrupted while sleeping between performing a SessionTimeouter run,"
                            + " assuming shutdown, looping to check runFlag.");
                    continue;
                }
            }
            // ?: Re-check runFlag, if we got woken up to exit
            if (!_runFlag) {
                // -> Shouldn't run anymore, exit loop.
                break;
            }

            try {
                Long notLiveSinceTimestamp = _timeoutSessionsOlderThanTimestampSupplier.get();
                // Perform CSAF Timeout
                Collection<String> sessionIds = _csaf.timeoutSessions(notLiveSinceTimestamp);
                log.debug("Timed out [" + sessionIds.size() + "] MatsSocketSessions for [" + _matsSocketServer
                        .serverId() + "]");
                // Notify all SessionRemovedEventListeners about all MatsSocketSessions that was timed out.
                for (String sessionId : sessionIds) {
                    // Create event
                    SessionRemovedEventImpl sessionRemovedEvent = new SessionRemovedEventImpl(
                            SessionRemovedEventType.TIMEOUT, sessionId, null,
                            "Session timeout out since liveliness timestamp wasn't updated since [" + Instant
                                    .ofEpochMilli(notLiveSinceTimestamp) + "]");
                    // Notify listeners about event
                    _matsSocketServer.invokeSessionRemovedEventListeners(sessionRemovedEvent);
                }
            }
            catch (Throwable t) {
                log.warn("Got problems performing a SessionTimeouter run.", t);
            }
        }
        log.info("Exiting: CSAF SessionTimeouter Thread for [" + _matsSocketServer.serverId() + "].");
    }

    private void scavengeSessionRemnantsRunnable() {
        // Some constant randomness - which over time will spread out the updates on the different nodes.
        long randomExtraSleep = (long) (Math.random() * _millisBetweenScavengeSessionRemnantsRun * 0.1);
        log.info("Started: CSAF ScavengeSessionRemnants Thread for [" + _matsSocketServer.serverId()
                + "], Thread [" + Thread.currentThread() + "]");
        while (_runFlag) {
            try {
                Thread.sleep(_millisBetweenScavengeSessionRemnantsRun + randomExtraSleep);
            }
            catch (InterruptedException e) {
                log.debug("Got interrupted while sleeping between performing a ScavengeSessionRemnants run,"
                        + " assuming shutdown, looping to check runFlag.");
                continue;
            }

            try {
                int count = _csaf.scavengeSessionRemnants();
                log.debug("Performed ScavengeSessionRemnants run, resulting in [" + count + "] items scavenged, for ["
                        + _matsSocketServer.serverId() + "]");
            }
            catch (Throwable t) {
                log.warn("Got problems performing a ScavengeSessionRemnants run.", t);
            }
        }
        log.info("Exiting: CSAF ScavengeSessionRemnants Thread for [" + _matsSocketServer.serverId() + "].");
    }
}
