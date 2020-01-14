package com.stolsvik.mats.websocket.impl;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.websocket.Session;

import com.stolsvik.mats.websocket.ClusterStoreAndForward;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.websocket.ClusterStoreAndForward.DataAccessException;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.StoredMessage;

/**
 * Gets a ping from the node-specific Topic, or when the client reconnects.
 *
 * @author Endre Stølsvik 2019-12 - http://stolsvik.com/, endre@stolsvik.com
 */
class MessageToWebSocketForwarder {
    private static final Logger log = LoggerFactory.getLogger(MessageToWebSocketForwarder.class);

    private final DefaultMatsSocketServer _matsSocketServer;
    private final ClusterStoreAndForward _clusterStoreAndForward;

    private final ThreadPoolExecutor _threadPool;

    private final Map<String, Long> _handlersCurrentlyRunningWithNotificationCount = new HashMap<>();
    private final AtomicInteger _threadNumber = new AtomicInteger();

    public MessageToWebSocketForwarder(DefaultMatsSocketServer defaultMatsSocketServer,
            ClusterStoreAndForward clusterStoreAndForward, int corePoolSize, int maximumPoolSize) {
        _matsSocketServer = defaultMatsSocketServer;
        _clusterStoreAndForward = clusterStoreAndForward;

        // Trick to make ThreadPoolExecutor work as anyone in the world would expect:
        // Have a constant pool of "corePoolSize", and then as more tasks are concurrently running than threads
        // available, you increase the number of threads until "maximumPoolSize", at which point the rest go on
        // queue.

        // Snitched from https://stackoverflow.com/a/24493856

        // Part 1: So, we extend a LinkedTransferQueue to behave a bit special on "offer(..)":
        LinkedTransferQueue<Runnable> runQueue = new LinkedTransferQueue<Runnable>() {
            @Override
            public boolean offer(Runnable e) {
                // If there are any pool thread waiting for job, give it the job, otherwise return false.
                // The TPE interprets false as "no more room on queue", so it rejects it. (cont'd on part 2)
                return tryTransfer(e);
            }
        };
        _threadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize,
                5L, TimeUnit.MINUTES, runQueue,
                r1 -> new Thread(r1, "MatsSockets WebSocket Forwarder #" + _threadNumber.getAndIncrement()));

        // Part 2: We make a special RejectionExecutionHandler ...
        _threadPool.setRejectedExecutionHandler((r, executor) -> {
            // ... which upon rejection due to "full queue" puts the task on queue nevertheless
            // (LTQ is not bounded).
            ((LinkedTransferQueue<Runnable>) _threadPool.getQueue()).put(r);
        });
    }

    void shutdown() {
        _threadPool.shutdown();
        try {
            _threadPool.awaitTermination(8, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            // Just re-set interrupted flag, and go on exiting.
            Thread.currentThread().interrupt();
        }
        _threadPool.shutdownNow();
    }

    void notifyMessageFor(MatsSocketSession matsSocketSession) {
        // :: Check if there is an existing handler for this MatsSocketSession
        String matsSocketSessionId = matsSocketSession.getId();
        synchronized (_handlersCurrentlyRunningWithNotificationCount) {
            // ?: Check if we already have a message handler running for this sessionId
            Long count = _handlersCurrentlyRunningWithNotificationCount.get(matsSocketSessionId);
            // ?: Did we have any count here?
            if (count != null) {
                // -> Yes, so just increase this, and the existing handler will take care of it.
                _handlersCurrentlyRunningWithNotificationCount.put(matsSocketSessionId, count
                        + 1);
                // We're done
                return;
            }

            // E-> No, there was not a handler running, so we must make one
            // We will now fire off a handler, with 1 in count
            _handlersCurrentlyRunningWithNotificationCount.put(matsSocketSessionId, 1L);
        }

        // ----- There was no existing handler for this MatsSocketSession

        // Fire off a new handler.
        _threadPool.execute(() -> this.handlerRunnable(matsSocketSession));
    }

    void handlerRunnable(MatsSocketSession matsSocketSession) {
        String matsSocketSessionId = matsSocketSession.getId();
        Session webSocketSession = matsSocketSession.getWebSocketSession();

        try { // try-finally: Remove ourselves from the "currently running handlers".

            RENOTIFY: while (true) { // LOOP: "Re-notifications"
                // ?: Check if WebSocket Session is still open.
                if (!webSocketSession.isOpen()) {
                    log.info("When about to run forward-messages-to-websocket handler, we found that the WebSocket"
                            + " Session was closed. Notifying '"
                            + _clusterStoreAndForward.getClass().getSimpleName()
                            + "' that this MatsSocketSession does not reside here ["
                            + _matsSocketServer.getMyNodename() + "] anymore, forwarding notification to new"
                            + " MatsSocketSession home (if any), and exiting handler.");

                    // :: Deregister from this node.
                    try {
                        _clusterStoreAndForward.deregisterSessionFromThisNode(matsSocketSessionId,
                                matsSocketSession.getConnectionId());
                    }
                    catch (DataAccessException e) {
                        log.warn("Got '" + e.getClass().getSimpleName() + "' when trying to notify "
                                + "[" + _clusterStoreAndForward.getClass().getSimpleName()
                                + "] about WebSocket Session being closed and thus MatsSocketSession not residing"
                                + " here [" + _matsSocketServer.getMyNodename() + "] anymore. Ignoring,"
                                + " exiting.", e);
                        // Since the first thing the notifyHomeNodeAboutNewMessage() needs to do is query store,
                        // this will pretty much guaranteed not work, so just exit out.
                        return;
                    }

                    // Forward to new home (Note: It can theoretically be us, due to race wrt. close & reconnect)
                    _matsSocketServer.notifyHomeNodeIfAnyAboutNewMessage(matsSocketSessionId);
                    // We're done, exit.
                    return;
                }

                while (true) { // LOOP: Clear out currently stored messages from ClusterStoreAndForward store
                    List<StoredMessage> messagesForSession;
                    try {
                        messagesForSession = _clusterStoreAndForward
                                .getMessagesForSession(matsSocketSessionId, 20);
                    }
                    catch (DataAccessException e) {
                        log.warn("Got '" + e.getClass().getSimpleName() + "' when trying to load messages from"
                                + " '" + _clusterStoreAndForward.getClass().getSimpleName()
                                + "'. Bailing out, hoping for self-healer process to figure it out.", e);
                        return;
                    }

                    // ?: Check if we're empty of messages
                    // (Notice how this logic always requires a final query which returns zero messages)
                    if (messagesForSession.isEmpty()) {
                        // -> Yes, it was empty. Break out of "Clear out stored messages.." loop.
                        break;
                    }

                    String traceIds = messagesForSession.stream()
                            .map(StoredMessage::getTraceId)
                            .collect(Collectors.joining(", "));
                    List<Long> messageIds = messagesForSession.stream().map(StoredMessage::getId)
                            .collect(Collectors.toList());

                    // :: Forward message(s) over WebSocket
                    try {
                        String nowString = Long.toString(System.currentTimeMillis());
                        // :: Feed the JSONs over, manually piecing together a JSON Array.
                        // Fetch the output sink
                        Writer writer = webSocketSession.getBasicRemote().getSendWriter();
                        // Create the JSON Array
                        writer.append('[');
                        boolean first = true;
                        for (StoredMessage storedMessage : messagesForSession) {
                            if (first) {
                                first = false;
                            }
                            else {
                                writer.append(',');
                            }
                            String json = storedMessage.getEnvelopeJson();
                            // :: Replace in the sent timestamp and this node's nodename.
                            json = DefaultMatsSocketServer.REPLACE_VALUE_TIMESTAMP_REGEX.matcher(json)
                                    .replaceFirst(nowString);
                            json = DefaultMatsSocketServer.REPLACE_VALUE_REPLY_NODENAME_REGEX.matcher(json)
                                    .replaceFirst(_matsSocketServer.getMyNodename());
                            writer.append(json);
                        }
                        writer.append(']');
                        writer.close();
                        log.info("Finished sending '" + messagesForSession.size() + "' message(s) with TraceIds ["
                                + traceIds + "] to MatsSession [" + matsSocketSession + "] over WebSocket session ["
                                + webSocketSession + "].");
                    }
                    catch (IOException ioe) {
                        // -> Evidently got problems forwarding the message over WebSocket
                        log.warn("Got [" + ioe.getClass().getSimpleName()
                                + "] while trying to send '" + messagesForSession.size()
                                + "' messages with TraceId [" + traceIds + "] to MatsSession [" + matsSocketSession
                                + "] over WebSocket session [" + webSocketSession
                                + "]. Increasing 'delivery_count' for message, will try again.", ioe);

                        // :: Increase delivery count
                        try {
                            _clusterStoreAndForward.messagesFailedDelivery(matsSocketSessionId, messageIds);
                        }
                        catch (DataAccessException e) {
                            log.warn("Got '" + e.getClass().getSimpleName()
                                    + "' when trying to invoke 'messagesFailedDelivery' on '" +
                                    _clusterStoreAndForward.getClass().getSimpleName() + "' for '"
                                    + messagesForSession.size() + "' messages with TraceIds [" + traceIds
                                    + "]. Bailing out, hoping for self-healer process to figure it out.", e);
                        }
                        // Run new "re-notification" loop, to check if socket still open, then try again.
                        continue RENOTIFY;
                    }

                    // :: Mark as complete (i.e. delete them).
                    try {
                        _clusterStoreAndForward.messagesComplete(matsSocketSessionId, messageIds);
                    }
                    catch (DataAccessException e) {
                        log.warn("Got '" + e.getClass().getSimpleName()
                                + "' when trying to invoke 'messagesComplete' on '" +
                                _clusterStoreAndForward.getClass().getSimpleName() + "' for '"
                                + messagesForSession.size() + "' messages with TraceIds [" + traceIds
                                + "]. Bailing out, hoping for self-healer process to figure it out.", e);
                        // Bailing out
                        return;
                    }
                }

                // ----- The database is (was) CURRENTLY empty of messages for this session.

                /*
                 * Since we're finished with these messages, we reduce the number of outstanding count, and if zero -
                 * remove and exit. There IS a race here: There can come in a new message WHILE we are exiting, so we
                 * might exit right when we're being notified about a new message having come in.
                 *
                 * However, this decrease vs. increase of count is done transactionally within a synchronized:
                 *
                 * Either:
                 *
                 * 1. The new message comes in. It sees the count is 1 (because a MatsSessionId- specific handler is
                 * already running), and thus increases to 2 and do NOT fire off a new handler. This has then (due to
                 * synchronized) happened before the handler come and read and reduce it, so when the handler reduce, it
                 * reduces to 1, and thus go back for one more message pull loop.
                 *
                 * 2. The new message comes in. The handler is JUST about to exit, so it reduces the count to 0, and
                 * thus remove it - and then exits. The new message then do not see a handler running (the map does not
                 * have an entry for the MatsSessionId, since it was just removed by the existing handler), puts the
                 * count in with 1, and fires off a new handler. There might now be two handlers for a brief time, but
                 * the old one is exiting, not touching the data store anymore, while the new is just starting.
                 *
                 * Furthermore: We might already have handled the new message by the SELECT already having pulled it in,
                 * before the code got time to notify us. This is not a problem: The only thing that will happen is that
                 * we loop, ask for new messages, get ZERO back, and are thus finished. Such a thing could conceivably
                 * happen many times in a row, but the ending result is always that there will ALWAYS be a
                 * "last handler round", which might, or might not, get zero messages. For every message, there will
                 * guaranteed be one handler that AFTER the INSERT will evaluate whether it is still on store. Again:
                 * Either it was sweeped up in a previous handler round, or a new handler will be dispatched.
                 */
                synchronized (_handlersCurrentlyRunningWithNotificationCount) {
                    Long count = _handlersCurrentlyRunningWithNotificationCount.get(matsSocketSessionId);
                    // ?: Is this 1, meaning that we are finishing off our handler rounds?
                    if (count == 1) {
                        // -> Yes, so this would be a decrease to zero
                        // Finally handler will remove us from currently running handlers - we're done, exit.
                        return;
                    }
                    // E-> It was MORE than 1.
                    /*
                     * Now we'll do "coalescing": First, observe: One pass through the handler will clear out all
                     * messages stored for this MatsSocketSession. So, the point is that if we've got notified about
                     * several messages while doing the rounds, we've EITHER already handled them in one of the loop
                     * rounds, OR one final pass would handle any remaining. We can thus set the count down to 1, and be
                     * sure that we will have handled any outstanding messages that we've been notified about /until
                     * now/.
                     */
                    _handlersCurrentlyRunningWithNotificationCount.put(matsSocketSessionId, 1L);
                }
            }
        }
        finally {
            // Before exiting, we must remove us from the "currently running handlers" map.
            synchronized (_handlersCurrentlyRunningWithNotificationCount) {
                _handlersCurrentlyRunningWithNotificationCount.remove(matsSocketSessionId);
            }
        }

    }
}
