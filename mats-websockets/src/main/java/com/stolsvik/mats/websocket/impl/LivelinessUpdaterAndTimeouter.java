package com.stolsvik.mats.websocket.impl;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.websocket.ClusterStoreAndForward;

/**
 * Liveliness Updater and Timeouter: Notifies the CSAF about which MatsSocketSessionIds that are active every X
 * milliseconds (which should measure in 1 or single-digit minutes), and times out session that are older than X
 * milliseconds (which should measure in hours or days).
 *
 * @author Endre Stølsvik 2020-05-17 21:29 - http://stolsvik.com/, endre@stolsvik.com
 */
class LivelinessUpdaterAndTimeouter implements MatsSocketStatics {
    private static final Logger log = LoggerFactory.getLogger(LivelinessUpdaterAndTimeouter.class);

    // :: From Constructor params
    private final DefaultMatsSocketServer _matsSocketServer;
    private final ClusterStoreAndForward _csaf;
    private final int _millisecondsBetweenEachLivelinessUpdate;

    // :: Constructor inited
    private final Thread _updaterThread;

    LivelinessUpdaterAndTimeouter(DefaultMatsSocketServer matsSocketServer, ClusterStoreAndForward csaf,
            int millisecondsBetweenEachLivelinessUpdate) {
        _matsSocketServer = matsSocketServer;
        _csaf = csaf;
        _millisecondsBetweenEachLivelinessUpdate = millisecondsBetweenEachLivelinessUpdate;
        _updaterThread = new Thread(this::updaterRunnable, THREAD_PREFIX + "CSAF LivelinessUpdater for "
                + _matsSocketServer.serverId());
        _updaterThread.start();
        log.info("Instantiated [" + this.getClass().getSimpleName() + "] for [" + _matsSocketServer.serverId() + "]");
    }

    private volatile boolean _runFlag = true;

    void shutdown() {
        _runFlag = false;
        log.info("Shutting down [" + this.getClass().getSimpleName() + "] for [" + _matsSocketServer.serverId()
                + "], Thread [" + _updaterThread + "]");
        _updaterThread.interrupt();
    }

    private void updaterRunnable() {
        // Some constant randomness - which over time will spread out the updates on the different nodes.
        int randomExtraSleep = (int) (Math.random() * 2000);
        log.info("Started LivelinessUpdater for [" + _matsSocketServer.serverId()
                + "], Thread [" + Thread.currentThread() + "]");
        while (_runFlag) {
            try {
                Thread.sleep(_millisecondsBetweenEachLivelinessUpdate + randomExtraSleep);
            }
            catch (InterruptedException e) {
                log.info("Got interrupted while sleeping between updating active MatsSocketSessionIds,"
                        + " assuming shutdown, looping to check runFlag.");
                continue;
            }

            // NOTICE! The Map we do keySet() of is a ConcurrentMap, thus the keyset is "active". No problem.
            Set<String> matsSocketSessionIds = _matsSocketServer.getLiveMatsSocketSessions().keySet();
            log.debug("Updating CSAF liveliness of some [" + matsSocketSessionIds.size()
                    + "] active MatsSocketSessions");
            if (!matsSocketSessionIds.isEmpty()) {
                try {
                    _csaf.notifySessionLiveliness(matsSocketSessionIds);
                }
                catch (Throwable e) {
                    log.warn("Got problems updating liveliness of about [" + matsSocketSessionIds
                            .size() + "] MatsSocketSessions.");
                }
            }
        }
        log.info("LivelinessUpdater exiting.");
    }
}
