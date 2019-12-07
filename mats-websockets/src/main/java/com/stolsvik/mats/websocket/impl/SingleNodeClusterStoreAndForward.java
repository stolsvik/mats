package com.stolsvik.mats.websocket.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer.ClusterStoreAndForward;

/**
 * @author Endre Stølsvik 2019-12-07 11:25 - http://stolsvik.com/, endre@stolsvik.com
 */
public class SingleNodeClusterStoreAndForward implements ClusterStoreAndForward {

    private final String _nodename;

    public SingleNodeClusterStoreAndForward(String nodename) {
        _nodename = nodename;
    }

    private ConcurrentHashMap<String, CopyOnWriteArrayList<StoredMessage>> _currentSessions = new ConcurrentHashMap<>();

    @Override
    public void registerSessionAtThisNode(String matsSocketSessionId) {
        _currentSessions.put(matsSocketSessionId, new CopyOnWriteArrayList<>());
    }

    @Override
    public void deregisterSessionFromThisNode(String matsSocketSessionId) {
        _currentSessions.remove(matsSocketSessionId);
    }

    @Override
    public Optional<String> getCurrentNodeForSession(String matsSocketSessionId) {
        return Optional.of(_nodename);
    }

    @Override
    public void pingLivelinessForSessions(List<String> matsSocketSessionIds) {
        /* no-op for the time being */
    }

    @Override
    public void terminateSession(String matsSocketSessionId) {
        // Also just remove it, as with deregister..
        _currentSessions.remove(matsSocketSessionId);
    }

    private AtomicLong _ider = new AtomicLong(1);

    @Override
    public Optional<String> storeMessageForSession(String matsSocketSessionId, String traceId, String type,
            String message) {
        CopyOnWriteArrayList<StoredMessage> storedMessages = _currentSessions.get(matsSocketSessionId);
        if (storedMessages == null) {
            return Optional.empty();
        }

        storedMessages.add(new StoredMessageImpl(_ider.getAndIncrement(), System.currentTimeMillis(), type, traceId, message));
        return Optional.of(_nodename);
    }

    @Override
    public List<StoredMessage> getMessagesForSession(String sessionId) {
        CopyOnWriteArrayList<StoredMessage> storedMessages = _currentSessions.get(sessionId);
        if (storedMessages != null) {
            return new ArrayList<>(storedMessages);
        }
        return Collections.emptyList();
    }

    @Override
    public void messagesDelivered(String sessionId, List<Long> messageIds) {
        HashSet<Long> messageIdsSet = new HashSet<>(messageIds);
        CopyOnWriteArrayList<StoredMessage> storedMessages = _currentSessions.get(sessionId);
        storedMessages.removeIf(next -> messageIdsSet.contains(next.getId()));
    }

    static class StoredMessageImpl implements StoredMessage {

        private final long _id;
        private final long _storedTimestamp;

        private final String _type;
        private final String _traceId;
        private final String _envelopeJson;

        public StoredMessageImpl(long id, long storedTimestamp, String type, String traceId, String envelopeJson) {
            _id = id;
            _storedTimestamp = storedTimestamp;
            _type = type;
            _traceId = traceId;
            _envelopeJson = envelopeJson;
        }

        @Override
        public long getId() {
            return _id;
        }

        @Override
        public long getStoredTimestamp() {
            return _storedTimestamp;
        }

        @Override
        public String getType() {
            return _type;
        }

        @Override
        public String getTraceId() {
            return _traceId;
        }

        @Override
        public String getEnvelopeJson() {
            return _envelopeJson;
        }
    }
}
