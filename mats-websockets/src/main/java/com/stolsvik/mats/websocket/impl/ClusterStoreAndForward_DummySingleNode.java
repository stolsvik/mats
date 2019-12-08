package com.stolsvik.mats.websocket.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

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

    private ConcurrentHashMap<String, CopyOnWriteArrayList<StoredMessageImpl>> _currentSessions = new ConcurrentHashMap<>();

    @Override
    public void boot() {
        /* no-op */
    }

    @Override
    public void registerSessionAtThisNode(String matsSocketSessionId) {
        _currentSessions.put(matsSocketSessionId, new CopyOnWriteArrayList<>());
    }

    @Override
    public void deregisterSessionFromThisNode(String matsSocketSessionId) {
        _currentSessions.remove(matsSocketSessionId);
    }

    @Override
    public void terminateSession(String matsSocketSessionId) {
        // Also just remove it, as with deregister..
        _currentSessions.remove(matsSocketSessionId);
    }

    @Override
    public Optional<String> getCurrentNodeForSession(String matsSocketSessionId) {
        return Optional.of(_nodename);
    }

    @Override
    public void notifySessionLiveliness(List<String> matsSocketSessionIds) {
        /* no-op for the time being */
    }

    @Override
    public Optional<String> storeMessageForSession(String matsSocketSessionId, String traceId, String type,
            String message) {
        CopyOnWriteArrayList<StoredMessageImpl> storedMessages = _currentSessions.get(matsSocketSessionId);
        if (storedMessages == null) {
            return Optional.empty();
        }

        long messageId = ThreadLocalRandom.current().nextLong();

        storedMessages.add(new StoredMessageImpl(messageId, 0, System.currentTimeMillis(), type, traceId,
                message));
        return Optional.of(_nodename);
    }

    @Override
    public List<StoredMessage> getMessagesForSession(String matsSocketSessionId, int maxNumberOfMessages) {
        CopyOnWriteArrayList<StoredMessageImpl> storedMessages = _currentSessions.get(matsSocketSessionId);
        if (storedMessages != null) {
            return storedMessages.stream().limit(maxNumberOfMessages).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public void messagesComplete(String sessionId, List<Long> messageIds) {
        HashSet<Long> messageIdsSet = new HashSet<>(messageIds);
        CopyOnWriteArrayList<StoredMessageImpl> storedMessages = _currentSessions.get(sessionId);
        storedMessages.removeIf(next -> messageIdsSet.contains(next.getId()));
    }

    @Override
    public void messagesFailedDelivery(String matsSocketSessionId, List<Long> messageIds) {
        HashSet<Long> messageIdsSet = new HashSet<>(messageIds);
        CopyOnWriteArrayList<StoredMessageImpl> storedMessages = _currentSessions.get(matsSocketSessionId);
        storedMessages.stream().filter(m -> messageIdsSet.contains(m.getId())).forEach(m -> m._deliveryAttempt++);
    }

}
