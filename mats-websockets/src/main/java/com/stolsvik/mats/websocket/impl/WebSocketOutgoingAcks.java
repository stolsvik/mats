package com.stolsvik.mats.websocket.impl;

import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.ACK;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.ACK2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto.Direction;

/**
 * Handles async sending of ACKs and ACK2s.
 *
 * @author Endre Stølsvik 2020-05-22 00:00 - http://stolsvik.com/, endre@stolsvik.com
 */
public class WebSocketOutgoingAcks implements MatsSocketStatics {

    private static final Logger log = LoggerFactory.getLogger(WebSocketOutgoingAcks.class);

    private final DefaultMatsSocketServer _matsSocketServer;

    private final SaneThreadPoolExecutor _threadPool;
    private final Thread _periodicFlushThread;

    private final ConcurrentHashMap<String, List<String>> _sessionIdToAcks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<String>> _sessionIdToAck2s = new ConcurrentHashMap<>();

    WebSocketOutgoingAcks(DefaultMatsSocketServer defaultMatsSocketServer,
            int corePoolSize, int maxPoolSize) {
        _matsSocketServer = defaultMatsSocketServer;
        _threadPool = new SaneThreadPoolExecutor(corePoolSize, maxPoolSize, this.getClass().getSimpleName(),
                _matsSocketServer.serverId());
        _periodicFlushThread = new Thread(this::periodicFlushRunnable, THREAD_PREFIX + getClass().getSimpleName()
                + " Periodic Flusher");
        _periodicFlushThread.start();
        log.info("Instantiated [" + this.getClass().getSimpleName() + "] for [" + _matsSocketServer.serverId()
                + "], PeriodicFlushThread:[" + _periodicFlushThread + "], ThreadPool:[" + _threadPool + "]");
    }

    void sendAck(String matsSocketSessionId, String cmidAck) {
        // ConcurrentHashMap-atomically update the outstanding currentACKs for the MatsSocketSessionId in question.
        _sessionIdToAcks.compute(matsSocketSessionId, (key, currentAcks) -> {
            if (currentAcks == null) {
                currentAcks = new ArrayList<>();
            }
            currentAcks.add(cmidAck);
            return currentAcks;
        });
    }

    void sendAcks(String matsSocketSessionId, Collection<String> cmidAcks) {
        // ConcurrentHashMap-atomically update the outstanding currentACKs for the MatsSocketSessionId in question.
        _sessionIdToAcks.compute(matsSocketSessionId, (key, currentAcks) -> {
            if (currentAcks == null) {
                currentAcks = new ArrayList<>();
            }
            currentAcks.addAll(cmidAcks);
            return currentAcks;
        });
    }


    public void removeAck(String matsSocketSessionId, String cmidAck) {
        // ConcurrentHashMap-atomically update the outstanding currentACKs for the MatsSocketSessionId in question.
        _sessionIdToAcks.compute(matsSocketSessionId, (key, currentAcks) -> {
            if (currentAcks != null) {
                currentAcks.remove(cmidAck);
                if (currentAcks.isEmpty()) {
                    return null;
                }
            }
            return currentAcks;
        });
    }

    void sendAck2(String matsSocketSessionId, String smidAck2) {
        // ConcurrentHashMap-atomically update the outstanding currentACK2s for the MatsSocketSessionId in question.
        _sessionIdToAck2s.compute(matsSocketSessionId, (key, currentAck2s) -> {
            if (currentAck2s == null) {
                currentAck2s = new ArrayList<>();
            }
            currentAck2s.add(smidAck2);
            return currentAck2s;
        });
    }

    void sendAck2s(String matsSocketSessionId, Collection<String> smidAck2s) {
        // ConcurrentHashMap-atomically update the outstanding currentACK2s for the MatsSocketSessionId in question.
        _sessionIdToAck2s.compute(matsSocketSessionId, (key, currentAck2s) -> {
            if (currentAck2s == null) {
                currentAck2s = new ArrayList<>();
            }
            currentAck2s.addAll(smidAck2s);
            return currentAck2s;
        });
    }

    void shutdown(int gracefulShutdownMillis) {
        _runFlag = false;
        log.info("Shutting down [" + this.getClass().getSimpleName() + "] for [" + _matsSocketServer.serverId()
                + "], PeriodicFlushThread:[" + _periodicFlushThread + "], ThreadPool [" + _threadPool + "]");
        _periodicFlushThread.interrupt();
        _threadPool.shutdownNice(gracefulShutdownMillis);
    }

    private volatile boolean _runFlag = true;

    void periodicFlushRunnable() {
        while (_runFlag) {
            try {
                Thread.sleep(MILLIS_BETWEEN_ACK_FLUSHES);
            }
            catch (InterruptedException e) {
                log.debug("Got interrupted while sleeping between performing a Periodic Acks Flush run,"
                        + " assuming shutdown, looping to check runFlag.");
                continue;
            }

            // Gather which MatsSocketSessionIds to do a flush run for
            Set<String> matsSocketSessionIds = new HashSet<>();
            matsSocketSessionIds.addAll(_sessionIdToAcks.keySet());
            matsSocketSessionIds.addAll(_sessionIdToAck2s.keySet());

            // Fire them off
            for (String matsSocketSessionId : matsSocketSessionIds) {
                _threadPool.execute(() -> sendAckAndAck2sForSession(matsSocketSessionId));
            }
        }
    }

    void sendAckAndAck2sForSession(String matsSocketSessionId) {
        // Pick out all ACKs and ACK2s to send, do this ConcurrentHashMap-atomically:
        // (Any concurrently added ACKs or ACK2s will either be picked up in this round, or in the next)
        Object[] acks_x = new Object[] { null };
        Object[] ack2s_x = new Object[] { null };
        _sessionIdToAcks.compute(matsSocketSessionId, (key, currentAcks) -> {
            acks_x[0] = currentAcks;
            return null;
        });
        _sessionIdToAck2s.compute(matsSocketSessionId, (key, currentAcks) -> {
            ack2s_x[0] = currentAcks;
            return null;
        });

        @SuppressWarnings("unchecked")
        List<String> acks = (List<String>) acks_x[0];
        @SuppressWarnings("unchecked")
        List<String> ack2s = (List<String>) ack2s_x[0];

        // Gotten-and-cleared, now check whether we still have the MatsSocketSession
        Optional<MatsSocketSessionAndMessageHandler> session = _matsSocketServer.getRegisteredLocalMatsSocketSession(
                matsSocketSessionId);
        if (!session.isPresent()) {
            // Nothing to do, the session is gone.
            // If he comes back, he will have to send the causes for these ACKs again.
            log.info("When about to send ACKs [" + acks + "] and ACK2s [" + ack2s + "] for MatsSocketSession ["
                    + matsSocketSessionId + "], the session was gone. Ignoring, if the session comes back, he will"
                    + " send the causes for these ACKs again.");
            return;
        }

        List<MatsSocketEnvelopeWithMetaDto> envelopeList = new ArrayList<>(2);
        // :: ACKs
        if (acks != null) {
            MatsSocketEnvelopeWithMetaDto ackEnvelope = new MatsSocketEnvelopeWithMetaDto();
            ackEnvelope.t = ACK;
            if (acks.size() > 1) {
                ackEnvelope.ids = acks;
            }
            else {
                ackEnvelope.cmid = acks.get(0);
            }
            envelopeList.add(ackEnvelope);
        }
        // ACK2s
        if (ack2s != null) {
            MatsSocketEnvelopeWithMetaDto ack2Envelope = new MatsSocketEnvelopeWithMetaDto();
            ack2Envelope.t = ACK2;
            if (ack2s.size() > 1) {
                ack2Envelope.ids = ack2s;
            }
            else {
                ack2Envelope.smid = ack2s.get(0);
            }
            envelopeList.add(ack2Envelope);
        }

        // Send it.
        try {
            String json = _matsSocketServer.getEnvelopeListObjectWriter().writeValueAsString(envelopeList);
            session.get().webSocketSendText(json);
        }
        catch (JsonProcessingException e) {
            log.error("Huh, couldn't serialize MatsSocketEnvelope?! This should definitely not happen!", e);
            return;
        }
        catch (IOException e) {
            // I believe IOExceptions here to be utterly final - the session is gone. So just ignore this.
            // If he comes back, he will have to send the causes for these ACKs again.
            log.info("When trying to send ACKs [" + acks + "] and ACK2s [" + ack2s + "] for MatsSocketSession ["
                    + matsSocketSessionId + "], we got IOException. Assuming session is"
                    + " gone. Ignoring, if the session comes back, he will send the causes for these ACKs again.", e);
            return;
        }

        // Record envelopes
        // TODO: Set the envelope.rttm??
        session.get().recordEnvelopes(envelopeList, System.currentTimeMillis(), Direction.S2C);
    }
}
