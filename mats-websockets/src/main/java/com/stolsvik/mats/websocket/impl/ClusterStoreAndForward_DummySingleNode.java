package com.stolsvik.mats.websocket.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import com.stolsvik.mats.websocket.ClusterStoreAndForward;

/**
 * Dummy in-memory implementation of {@link ClusterStoreAndForward} which only works for a single node.
 *
 * @author Endre Stølsvik 2019-12-07 11:25 - http://stolsvik.com/, endre@stolsvik.com
 */
public class ClusterStoreAndForward_DummySingleNode implements ClusterStoreAndForward {

    private final String _nodename;

    public ClusterStoreAndForward_DummySingleNode(String nodename) {
        _nodename = nodename;
    }

    private ConcurrentHashMap<String, MsStoreSession> _currentSessions = new ConcurrentHashMap<>();

    @Override
    public void boot() {
        /* no-op */
    }

    private static class MsStoreSession {
        private volatile String _connectionId;
        private final CopyOnWriteArrayList<SimpleStoredMessage> _messages = new CopyOnWriteArrayList<>();

        public MsStoreSession(String connectionId) {
            _connectionId = connectionId;
        }

        void registerConnection(String connectionId) {
            synchronized (this) {
                _connectionId = connectionId;
            }
        }

        void deregisterConnection(String connectionId) {
            synchronized (this) {
                // ?: Is it this Connection that wants to be deregistered?
                // This guards against deregistering a new registration that has happened concurrently with us
                // realizing that this current WebSocket Connection was dead
                if (_connectionId.equals(connectionId)) {
                    _connectionId = null;
                }
            }
        }

        boolean hasRegistration() {
            synchronized (this) {
                return _connectionId != null;
            }
        }

    }

    @Override
    public void registerSessionAtThisNode(String matsSocketSessionId, String connectionId) {
        MsStoreSession msStoreSession = _currentSessions.computeIfAbsent(matsSocketSessionId,
                s -> new MsStoreSession(connectionId));
        // Set registration to this connectionId (there is no concept of "node home" for this Single-Node CSAF impl.)
        msStoreSession.registerConnection(connectionId);
    }

    @Override
    public void deregisterSessionFromThisNode(String matsSocketSessionId, String connectionId) {
        MsStoreSession msStoreSession = _currentSessions.get(matsSocketSessionId);
        // ?: Did this Session exist?
        if (msStoreSession != null) {
            // -> Yes, Session exists.
            // Deregister this ConnectionId (i.e. if correct 'correctionId', null out)
            msStoreSession.deregisterConnection(connectionId);
        }
    }

    @Override
    public void closeSession(String matsSocketSessionId) {
        // Remove it unconditionally, as this is an explicit "terminate session" invocation from client.
        _currentSessions.remove(matsSocketSessionId);
    }

    @Override
    public Optional<CurrentNode> getCurrentRegisteredNodeForSession(String matsSocketSessionId) {
        MsStoreSession msStoreSession = _currentSessions.get(matsSocketSessionId);
        if (msStoreSession == null) {
            return Optional.empty();
        }
        // Return current node
        return msStoreSession.hasRegistration()
                ? Optional.of(new CurrentNode(_nodename, msStoreSession._connectionId))
                : Optional.empty();
    }

    @Override
    public void notifySessionLiveliness(Collection<String> matsSocketSessionIds) {
        /* no-op for the time being */
    }

    @Override
    public boolean isSessionExists(String matsSocketSessionId) throws DataAccessException {
        return _currentSessions.get(matsSocketSessionId) != null;
    }

    @Override
    public Optional<CurrentNode> storeMessageForSession(String matsSocketSessionId, String traceId,
            long messageSequence, String type, String message) {
        MsStoreSession msStoreSession = _currentSessions.get(matsSocketSessionId);
        // ?: Did we have such a MatsSocketSession?
        if (msStoreSession == null) {
            // -> No, so drop the message, and return "no current registration".
            return Optional.empty();
        }

        // :: Add the message (unconditionally - no matter whether the MatsSocketSession currently has a registration.

        // Make a random MessageId
        long messageId = ThreadLocalRandom.current().nextLong();
        // Store the message
        msStoreSession._messages.add(new SimpleStoredMessage(messageId, 0, System.currentTimeMillis(),
                type, traceId, messageSequence, message));

        // Return current node
        return msStoreSession.hasRegistration()
                ? Optional.of(new CurrentNode(_nodename, msStoreSession._connectionId))
                : Optional.empty();
    }

    @Override
    public List<StoredMessage> getMessagesForSession(String matsSocketSessionId, int maxNumberOfMessages) {
        MsStoreSession msStoreSession = _currentSessions.get(matsSocketSessionId);
        if (msStoreSession != null) {
            return msStoreSession._messages.stream().limit(maxNumberOfMessages).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public void messagesComplete(String sessionId, Collection<Long> messageIds) {
        HashSet<Long> messageIdsSet = new HashSet<>(messageIds);
        CopyOnWriteArrayList<SimpleStoredMessage> storedMessages = _currentSessions.get(sessionId)._messages;
        storedMessages.removeIf(next -> messageIdsSet.contains(next.getId()));
    }

    @Override
    public void messagesFailedDelivery(String matsSocketSessionId, Collection<Long> messageIds) {
        HashSet<Long> messageIdsSet = new HashSet<>(messageIds);
        CopyOnWriteArrayList<SimpleStoredMessage> storedMessages = _currentSessions.get(matsSocketSessionId)._messages;
        for (Iterator<SimpleStoredMessage> it = storedMessages.iterator(); it.hasNext();) {
            SimpleStoredMessage msg = it.next();
            if (messageIdsSet.contains(msg.getId())) {
                it.remove();
                // Add it back with deliveryAttempts increased + 1.
                // NOTE: This is OK since it is CopyOnWriteArrayList
                storedMessages.add(new SimpleStoredMessage(msg.getId(), msg.getDeliveryAttempt() + 1, msg
                        .getStoredTimestamp(), msg.getType(), msg.getTraceId(), msg.getMessageSequence(), msg.getEnvelopeJson()));
            }
        }
    }

}
