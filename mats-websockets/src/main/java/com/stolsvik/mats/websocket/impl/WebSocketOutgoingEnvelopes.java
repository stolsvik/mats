package com.stolsvik.mats.websocket.impl;

import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.ACK;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.ACK2;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.NACK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto.Direction;

/**
 * Handles async sending of ACKs and ACK2s.
 *
 * @author Endre Stølsvik 2020-05-22 00:00 - http://stolsvik.com/, endre@stolsvik.com
 */
public class WebSocketOutgoingEnvelopes implements MatsSocketStatics {

    private static final Logger log = LoggerFactory.getLogger(WebSocketOutgoingEnvelopes.class);

    private final DefaultMatsSocketServer _matsSocketServer;

    private final SaneThreadPoolExecutor _threadPool;

    private final ConcurrentHashMap<String, List<MatsSocketEnvelopeWithMetaDto>> _sessionIdToEnvelopes = new ConcurrentHashMap<>();

    private final ScheduledExecutorService _timer = Executors.newScheduledThreadPool(4);

    WebSocketOutgoingEnvelopes(DefaultMatsSocketServer defaultMatsSocketServer,
            int corePoolSize, int maxPoolSize) {
        _matsSocketServer = defaultMatsSocketServer;
        _threadPool = new SaneThreadPoolExecutor(corePoolSize, maxPoolSize, this.getClass().getSimpleName(),
                _matsSocketServer.serverId());
        log.info("Instantiated [" + this.getClass().getSimpleName() + "] for [" + _matsSocketServer.serverId()
                + "], ThreadPool:[" + _threadPool + "]");
    }

    void shutdown(int gracefulShutdownMillis) {
        log.info("Shutting down [" + this.getClass().getSimpleName() + "] for [" + _matsSocketServer.serverId()
                + "], Timer:[" + _timer + "], ThreadPool [" + _threadPool + "]");
        _timer.shutdown();
        _threadPool.shutdownNice(gracefulShutdownMillis);
    }

    private void scheduleRun(String matsSocketSessionId, long delay, TimeUnit timeUnit) {
        // ?: Special handling for 0 delay (PONGs do this).
        if (delay == 0) {
            // -> Yes, zero delay - immediate dispatch
            dispatchEnvelopesForSession_OnThreadpool(matsSocketSessionId);
            return;
        }
        // E-> No, not zero delay - so send to timer.
        _timer.schedule(() -> {
            dispatchEnvelopesForSession_OnThreadpool(matsSocketSessionId);
        }, delay, timeUnit);
    }

    void sendEnvelope(String matsSocketSessionId, MatsSocketEnvelopeWithMetaDto envelope, long delay,
            TimeUnit timeUnit) {
        // ConcurrentHashMap-atomically update the outstanding envelopes for the MatsSocketSessionId in question.
        _sessionIdToEnvelopes.compute(matsSocketSessionId, (key, currentEnvelopes) -> {
            if (currentEnvelopes == null) {
                currentEnvelopes = new ArrayList<>();
            }
            currentEnvelopes.add(envelope);
            return currentEnvelopes;
        });
        scheduleRun(matsSocketSessionId, delay, timeUnit);
    }

    void sendEnvelopes(String matsSocketSessionId, Collection<MatsSocketEnvelopeWithMetaDto> envelopes, long delay,
            TimeUnit timeUnit) {
        // ConcurrentHashMap-atomically update the outstanding envelopes for the MatsSocketSessionId in question.
        _sessionIdToEnvelopes.compute(matsSocketSessionId, (key, currentEnvelopes) -> {
            if (currentEnvelopes == null) {
                currentEnvelopes = new ArrayList<>();
            }
            currentEnvelopes.addAll(envelopes);
            return currentEnvelopes;
        });
        scheduleRun(matsSocketSessionId, delay, timeUnit);
    }

    public void removeAck(String matsSocketSessionId, String cmidAck) {
        // ConcurrentHashMap-atomically update the outstanding currentACKs for the MatsSocketSessionId in question.
        _sessionIdToEnvelopes.compute(matsSocketSessionId, (key, currentEnvelopes) -> {
            if (currentEnvelopes != null) {
                currentEnvelopes.removeIf(e -> (e.t == ACK) && (e.cmid.equals(cmidAck)));
                if (currentEnvelopes.isEmpty()) {
                    return null;
                }
            }
            return currentEnvelopes;
        });
        // NOT scheduling, since we /removed/ some messages.
    }

    void sendAck2s(String matsSocketSessionId, Collection<String> smidAck2s) {
        // ConcurrentHashMap-atomically update the outstanding currentACK2s for the MatsSocketSessionId in question.
        _sessionIdToEnvelopes.compute(matsSocketSessionId, (key, currentEnvelopes) -> {
            if (currentEnvelopes == null) {
                currentEnvelopes = new ArrayList<>();
            }
            for (String smidAck2 : smidAck2s) {
                MatsSocketEnvelopeWithMetaDto e = new MatsSocketEnvelopeWithMetaDto();
                e.t = ACK2;
                e.smid = smidAck2;
                currentEnvelopes.add(e);
            }
            return currentEnvelopes;
        });
        scheduleRun(matsSocketSessionId, 25, TimeUnit.MILLISECONDS);
    }

    private void dispatchEnvelopesForSession_OnThreadpool(String matsSocketSessionId) {
        // Pick out all Envelopes to send, do this ConcurrentHashMap-atomically:
        // (Any concurrently added Envelopes will either be picked up in this round, or in the next scheduled task)
        Object[] envelopes_x = new Object[] { null };
        _sessionIdToEnvelopes.compute(matsSocketSessionId, (key, currentEnvelopes) -> {
            envelopes_x[0] = currentEnvelopes;
            return null;
        });

        @SuppressWarnings("unchecked")
        List<MatsSocketEnvelopeWithMetaDto> envelopeList = (List<MatsSocketEnvelopeWithMetaDto>) envelopes_x[0];

        // Gotten-and-cleared, now check whether there was anything there
        if ((envelopeList == null) || envelopeList.isEmpty()) {
            return;
        }

        // .. and check whether we still have the MatsSocketSession
        Optional<MatsSocketSessionAndMessageHandler> session = _matsSocketServer.getRegisteredLocalMatsSocketSession(
                matsSocketSessionId);
        if (!session.isPresent()) {
            // Nothing to do, the session is gone.
            // If he comes back, he will have to send the causes for these Envelopes again.
            log.info("When about to send [" + envelopeList.size() + "] Envelopes for MatsSocketSession ["
                    + matsSocketSessionId + "], the session was gone. Ignoring, if the session comes back, he will"
                    + " send the causes for these Envelopes again.");
            return;
        }

        // Dispatch sending on thread pool
        _threadPool.execute(() -> actualSendOfEnvelopes(session.get(), envelopeList));
    }

    private void actualSendOfEnvelopes(MatsSocketSessionAndMessageHandler session,
            List<MatsSocketEnvelopeWithMetaDto> envelopeList) {

        // :: Do "ACK/NACK/ACK2 compaction"
        List<String> acks = null;
        List<String> nacks = null;
        List<String> ack2s = null;
        for (Iterator<MatsSocketEnvelopeWithMetaDto> it = envelopeList.iterator(); it.hasNext();) {
            MatsSocketEnvelopeWithMetaDto envelope = it.next();
            if (envelope.t == ACK && envelope.desc == null) {
                it.remove();
                acks = acks != null ? acks : new ArrayList<>();
                acks.add(envelope.cmid);
            }
            if (envelope.t == NACK && envelope.desc == null) {
                it.remove();
                nacks = nacks != null ? nacks : new ArrayList<>();
                nacks.add(envelope.cmid);
            }
            if (envelope.t == ACK2 && envelope.desc == null) {
                it.remove();
                ack2s = ack2s != null ? ack2s : new ArrayList<>();
                ack2s.add(envelope.smid);
            }
        }
        if (acks != null && acks.size() > 0) {
            MatsSocketEnvelopeWithMetaDto e_acks = new MatsSocketEnvelopeWithMetaDto();
            e_acks.t = ACK;
            if (acks.size() == 1) {
                e_acks.cmid = acks.get(0);
            }
            else {
                e_acks.ids = acks;
            }
            envelopeList.add(e_acks);
        }
        if (nacks != null && nacks.size() > 0) {
            MatsSocketEnvelopeWithMetaDto e_nacks = new MatsSocketEnvelopeWithMetaDto();
            e_nacks.t = NACK;
            if (nacks.size() == 1) {
                e_nacks.cmid = nacks.get(0);
            }
            else {
                e_nacks.ids = nacks;
            }
            envelopeList.add(e_nacks);
        }
        if (ack2s != null && ack2s.size() > 0) {
            MatsSocketEnvelopeWithMetaDto e_ack2s = new MatsSocketEnvelopeWithMetaDto();
            e_ack2s.t = ACK2;
            if (ack2s.size() == 1) {
                e_ack2s.smid = ack2s.get(0);
            }
            else {
                e_ack2s.ids = ack2s;
            }
            envelopeList.add(e_ack2s);
        }

        // :: Send them
        try {
            String outgoingEnvelopesJson = _matsSocketServer.getEnvelopeListObjectWriter()
                    .writeValueAsString(envelopeList);
            session.webSocketSendText(outgoingEnvelopesJson);
        }
        catch (IOException | RuntimeException e) { // Also RuntimeException, since Jetty throws WebSocketException
            // I believe IOExceptions here to be utterly final - the session is gone. So just ignore this.
            // If he comes back, he will have to send the causes for these Envelopes again.
            log.info("When trying to send [" + envelopeList.size() + "] Envelopes for MatsSocketSession ["
                    + session.getMatsSocketSessionId() + "], we got IOException. Assuming session is gone."
                    + " Ignoring, if the session comes back, he will send the causes for these Envelopes again.", e);
            return;
        }

        // :: Calculate roundtrip-time if incoming-nanos is set, then null incoming-nanos.
        for (MatsSocketEnvelopeWithMetaDto e : envelopeList) {
            if (e.icnanos != null) {
                e.rttm = msSince(e.icnanos);
            }
            e.icnanos = null;
        }

        // Record envelopes
        session.recordEnvelopes(envelopeList, System.currentTimeMillis(), Direction.S2C);
    }

}
