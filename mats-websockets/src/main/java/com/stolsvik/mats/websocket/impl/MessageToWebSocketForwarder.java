package com.stolsvik.mats.websocket.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.stolsvik.mats.websocket.AuthenticationPlugin.DebugOption;
import com.stolsvik.mats.websocket.ClusterStoreAndForward;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.DataAccessException;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.StoredMessage;
import com.stolsvik.mats.websocket.MatsSocketServer.MessageType;
import com.stolsvik.mats.websocket.impl.MatsSocketEnvelopeDto.DirectJsonMessage;
import com.stolsvik.mats.websocket.impl.MatsSocketSessionAndMessageHandler.MatsSocketSessionState;

/**
 * Gets a ping from the node-specific Topic, or when the client reconnects.
 *
 * @author Endre Stølsvik 2019-12 - http://stolsvik.com/, endre@stolsvik.com
 */
class MessageToWebSocketForwarder implements MatsSocketStatics {
    private static final Logger log = LoggerFactory.getLogger(MessageToWebSocketForwarder.class);

    private final DefaultMatsSocketServer _matsSocketServer;
    private final ClusterStoreAndForward _clusterStoreAndForward;

    private final ThreadPoolExecutor _threadPool;

    private final ConcurrentHashMap<String, Integer> _handlersCurrentlyRunningWithNotificationCount = new ConcurrentHashMap<>();
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

    void newMessagesInCsafNotify(MatsSocketSessionAndMessageHandler matsSocketSessionAndMessageHandler) {
        log.info("newMessagesInCsafNotify for MatsSocketSessionId:[" + matsSocketSessionAndMessageHandler
                .getMatsSocketSessionId()
                + "]");
        // :: Check if there is an existing handler for this MatsSocketSession
        String uniqueId = matsSocketSessionAndMessageHandler.getMatsSocketSessionId()
                + matsSocketSessionAndMessageHandler
                        .getConnectionId();
        boolean[] fireOffNewHandler = new boolean[1];

        _handlersCurrentlyRunningWithNotificationCount.compute(uniqueId, (s, count) -> {
            // ?: Check whether there is an existing handler in place
            if (count == null) {
                // -> No there are not, so we need to fire off one
                fireOffNewHandler[0] = true;
                // The new count is 1
                return 1;
            }
            // E-> There was existing.
            // The new count is whatever it was + 1
            return count + 1;
        });

        // ?: Should we fire off new handler?
        if (fireOffNewHandler[0]) {
            // -> Yes, none were running, so fire off new handler.
            _threadPool.execute(() -> this.handlerRunnable(matsSocketSessionAndMessageHandler, uniqueId));
        }
    }

    void handlerRunnable(MatsSocketSessionAndMessageHandler matsSocketSessionAndMessageHandler, String uniqueId) {
        String matsSocketSessionId = matsSocketSessionAndMessageHandler.getMatsSocketSessionId();

        boolean removeOnExit = true;

        try { // try-catchAll: Log heavily.

            RENOTIFY: while (true) { // LOOP: "Re-notifications"
                // ?: Should we hold outgoing messages? (Waiting for "AUTH" answer from Client to our "REAUTH" request)
                if (matsSocketSessionAndMessageHandler.isHoldOutgoingMessages()) {
                    // -> Yes, so bail out
                    return;
                }

                // ?: Check if the MatsSocketSessionAndMessageHandler is still active
                if (!matsSocketSessionAndMessageHandler.isActive()) {
                    log.info("When about to run forward-messages-to-websocket handler, we found that the WebSocket"
                            + " Session was not state [" + MatsSocketSessionState.ACTIVE + "], so exit out.");
                    return;
                }

                // ?: Check if WebSocket Session (i.e. the Connection) is still open.
                if (!matsSocketSessionAndMessageHandler.isWebSocketSessionOpen()) {
                    log.info("When about to run forward-messages-to-websocket handler, we found that the WebSocket"
                            + " Session was closed. Deregistering this MatsSocketSessionHandler, forwarding"
                            + " notification to new MatsSocketSession home (if any), and exiting handler.");

                    matsSocketSessionAndMessageHandler.deregisterSession();

                    // Forward to new home (Note: It can theoretically be us, in another MatsSocketMessageHandler,
                    // .. due to race wrt. close & reconnect)
                    _matsSocketServer.newMessageOnWrongNode_NotifyCorrectHome(matsSocketSessionId);
                    // We're done, exit.
                    return;
                }

                while (true) { // LOOP: Clear out (i.e. forward) currently stored messages from ClusterStoreAndForward

                    // :: Get messages from CSAF
                    long nanos_start_GetMessages = System.nanoTime();
                    List<StoredMessage> messagesToDeliver;
                    try {
                        messagesToDeliver = _clusterStoreAndForward
                                .getMessagesFromOutbox(matsSocketSessionId, 20);
                    }
                    catch (DataAccessException e) {
                        log.warn("Got problems when trying to load messages from CSAF."
                                + " Bailing out, hoping for self-healer process to figure it out.", e);
                        // Bail out.
                        return;
                    }
                    float millisGetMessages = msSince(nanos_start_GetMessages);

                    // ?: Check if we're empty of messages
                    // (Notice how this logic always requires a final query which returns zero messages)
                    if (messagesToDeliver.isEmpty()) {
                        // -> Yes, it was empty. Break out of "Clear out stored messages.." loop.
                        // ----- Good path!
                        break;
                    }

                    // :: Now do authentication check for whether we're still good to go wrt. sending these messages.
                    boolean authOk = matsSocketSessionAndMessageHandler.reevaluateAuthenticationForOutgoingMessage();
                    if (!authOk) {
                        // Send "REAUTH" message, to get Client to send us new auth
                        matsSocketSessionAndMessageHandler.webSocketSendText("[{\"t\":\"" + MessageType.REAUTH
                                + "\"}]");
                        // Bail out and wait for new auth to come in, which will re-start sending.
                        // NOTICE: The not-ok return above will also have set "holdOutgoingMessages".
                        return;
                    }

                    // :: If there are any messages with deliveryCount > 0, then try to send these alone.
                    List<StoredMessage> redeliveryMessages = messagesToDeliver.stream()
                            .filter(m -> m.getDeliveryCount() > 0)
                            .collect(Collectors.toList());
                    // ?: Did we have any with deliveryCount > 0?
                    if (!redeliveryMessages.isEmpty()) {
                        // -> Yes, there are redeliveries here. Pick the first of them and try to deliver alone.
                        log.info("Of the [" + messagesToDeliver.size() + "] messages for MatsSocketSessionId ["
                                + matsSocketSessionId + "], [" + redeliveryMessages.size() + "] had deliveryCount > 0."
                                + " Trying to deliver these one by one, by picking first.");
                        // Set the 'messagesToDeliver' to first of the ones with delivery count > 0.
                        messagesToDeliver = Collections.singletonList(redeliveryMessages.get(0));
                    }

                    // :: Get the MessageIds to deliver (as List, for CSAF) and TraceIds (as String, for logging)
                    List<String> messageIds = messagesToDeliver.stream()
                            .map(StoredMessage::getServerMessageId)
                            .collect(Collectors.toList());
                    String messageTypesAndTraceIds = messagesToDeliver.stream()
                            .map(msg -> "{" + msg.getType() + "} " + msg.getTraceId())
                            .collect(Collectors.joining(", "));

                    long now = System.currentTimeMillis();

                    // Deserialize envelopes back to DTO, and stick in the message
                    List<MatsSocketEnvelopeDto> envelopeList = messagesToDeliver.stream().map(storedMessage -> {
                        MatsSocketEnvelopeDto envelope;
                        try {
                            envelope = _matsSocketServer.getEnvelopeObjectReader().readValue(storedMessage
                                    .getEnvelope());
                        }
                        catch (JsonProcessingException e) {
                            throw new AssertionError("Could not deserialize Envelope DTO.");
                        }
                        // Set the message onto the envelope, in "raw" mode (it is already json)
                        envelope.msg = new DirectJsonMessage(storedMessage.getMessageText());
                        // Handle debug
                        if (envelope.debug != null) {
                            /*
                             * Now, for Client-initiated, things have already been resolved - if we have a DebugDto,
                             * then it is because the user both requests to query for /something/, and are allowed to
                             * query for this.
                             *
                             * However, for Server-initiated, we do not know until now, and thus the initiation always
                             * adds it. We thus need to check with the AuthenticationPlugin's resolved auth vs. what the
                             * client has asked for wrt. Server-initiated.
                             */
                            // ?: Is this a Reply to a Client-to-Server REQUEST? (RESOLVE or REJECT)?
                            if ((MessageType.RESOLVE == storedMessage.getType())
                                    || MessageType.REJECT == storedMessage.getType()) {
                                // -> Yes, Reply (RESOLVE or REJECT)
                                // Find which resolved DebugOptions are in effect for this message
                                EnumSet<DebugOption> debugOptions = DebugOption.enumSetOf(envelope.debug.resd);
                                // Add timestamp and nodename depending on options
                                if (debugOptions.contains(DebugOption.TIMESTAMPS)) {
                                    envelope.debug.mscts = now;
                                }
                                if (debugOptions.contains(DebugOption.NODES)) {
                                    envelope.debug.mscnn = _matsSocketServer.getMyNodename();
                                }
                            }
                            // ?: Is this a Server-initiated message (SEND or REQUEST)?
                            if ((MessageType.SEND == storedMessage.getType())
                                    || MessageType.REQUEST == storedMessage.getType()) {
                                // -> Yes, Server-to-Client (SEND or REQUEST)
                                // Find what the client requests along with what authentication allows
                                EnumSet<DebugOption> debugOptions = matsSocketSessionAndMessageHandler
                                        .getCurrentResolvedServerToClientDebugOptions();
                                // ?: How's the standing wrt. DebugOptions?
                                if (debugOptions.isEmpty()) {
                                    // -> Client either do not request anything, or server does not allow anything for
                                    // this user.
                                    // Null out the already existing DebugDto
                                    envelope.debug = null;
                                }
                                else {
                                    // -> Client requests, and user is allowed, to query for some DebugOptions.
                                    // Set which flags are resolved
                                    envelope.debug.resd = DebugOption.flags(debugOptions);
                                    // Add timestamp and nodename depending on options
                                    if (debugOptions.contains(DebugOption.TIMESTAMPS)) {
                                        envelope.debug.mscts = now;
                                    }
                                    else {
                                        // Need to null this out, since set unconditionally upon server send/request
                                        envelope.debug.smcts = null;
                                    }
                                    if (debugOptions.contains(DebugOption.NODES)) {
                                        envelope.debug.mscnn = _matsSocketServer.getMyNodename();
                                    }
                                    else {
                                        // Need to null this out, since set unconditionally upon server send/request
                                        envelope.debug.smcnn = null;
                                    }
                                }
                            }
                        }
                        return envelope;
                    }).collect(Collectors.toList());

                    // Serialize the list of Envelopes
                    String jsonEnvelopeList = _matsSocketServer.getEnvelopeListObjectWriter()
                            .writeValueAsString(envelopeList);

                    // :: Forward message(s) over WebSocket
                    try {
                        // :: Actually send message(s) over WebSocket.
                        long nanos_start_SendMessage = System.nanoTime();
                        matsSocketSessionAndMessageHandler.webSocketSendText(jsonEnvelopeList);
                        float millisSendMessages = msSince(nanos_start_SendMessage);

                        // :: Mark as attempted delivered (set attempt timestamp, and increase delivery count)
                        // Result: will not be picked up on the next round of fetching messages.
                        // NOTE! They are COMPLETED when we get the ACK for the messageId from Client.
                        long nanos_start_MarkComplete = System.nanoTime();
                        try {
                            _clusterStoreAndForward.outboxMessagesAttemptedDelivery(matsSocketSessionId, messageIds);
                        }
                        catch (DataAccessException e) {
                            log.warn("Got problems when trying to invoke 'messagesAttemptedDelivery' on CSAF for "
                                    + messagesToDeliver.size() + " message(s) with TraceIds [" + messageTypesAndTraceIds
                                    + "]. Bailing out, hoping for self-healer process to figure it out.", e);
                            // Bail out
                            return;
                        }
                        float millisMarkComplete = msSince(nanos_start_MarkComplete);

                        // ----- Good path!

                        log.info("Finished sending " + messagesToDeliver.size() + " message(s) with TraceIds ["
                                + messageTypesAndTraceIds + "] to [" + matsSocketSessionAndMessageHandler
                                + "], get-from-CSAF took ["
                                + millisGetMessages + " ms], send over websocket took:[" + millisSendMessages + " ms],"
                                + " mark delivery attempt in CSAF took [" + millisMarkComplete + " ms].");

                        // Loop to check if empty of messages.
                        continue;
                    }
                    catch (IOException ioe) {
                        // -> Evidently got problems forwarding the message over WebSocket
                        log.warn("Got [" + ioe.getClass().getSimpleName()
                                + "] while trying to send " + messagesToDeliver.size()
                                + " message(s) with TraceIds [" + messageTypesAndTraceIds + "]."
                                + " Increasing 'delivery_count' for message, will try again.", ioe);

                        // :: Mark as attempted delivered (set attempt timestamp, and increase delivery count)
                        try {
                            _clusterStoreAndForward.outboxMessagesAttemptedDelivery(matsSocketSessionId, messageIds);
                        }
                        catch (DataAccessException e) {
                            log.warn("Got problems when trying to invoke 'messagesAttemptedDelivery' on CSAF for "
                                    + messagesToDeliver.size() + " message(s) with TraceIds [" + messageTypesAndTraceIds
                                    + "]. Bailing out, hoping for self-healer process to figure it out.", e);
                            // Bail out
                            return;
                        }

                        // :: Find messages with too many redelivery attempts (hardcoded 5 now)
                        // (Note: For current code, this should always only be max one..)
                        // (Note: We're using the "old" delivery_count number (before above increase) -> no problem)
                        List<StoredMessage> dlqMessages = messagesToDeliver.stream()
                                .filter(m -> m.getDeliveryCount() > 5)
                                .collect(Collectors.toList());
                        // ?: Did we have messages above threshold?
                        if (!dlqMessages.isEmpty()) {
                            // :: DLQ messages
                            try {
                                _clusterStoreAndForward.outboxMessagesDeadLetterQueue(matsSocketSessionId, messageIds);
                            }
                            catch (DataAccessException e) {
                                String dlqMessageTypesAndTraceIds = dlqMessages.stream()
                                        .map(msg -> "{" + msg.getType() + "} " + msg.getTraceId())
                                        .collect(Collectors.joining(", "));

                                log.warn("Got problems when trying to invoke 'messagesDeadLetterQueue' on CSAF for "
                                        + dlqMessages.size() + " message(s) with TraceIds ["
                                        + dlqMessageTypesAndTraceIds + "]. Bailing out, hoping for"
                                        + " self-healer process to figure it out.", e);
                                // Bail out
                                return;
                            }
                        }

                        // Chill-wait a bit to avoid total tight-loop if weird problem with socket saying it is open
                        // but cannot send messages over it.
                        try {
                            Thread.sleep(2500);
                        }
                        catch (InterruptedException e) {
                            log.info("Got interrupted while chill-waiting after trying to send a message over socket"
                                    + " which failed. Assuming that someone wants us to shut down, bailing out.");
                            // Bail out
                            return;
                        }
                        // Run new "re-notification" loop, to check if socket still open, then try again.
                        continue RENOTIFY;
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
                boolean shouldExit[] = new boolean[1];
                _handlersCurrentlyRunningWithNotificationCount.compute(uniqueId, (s, count) -> {
                    // ?: Is this 1, meaning that we are finishing off our handler rounds?
                    if (count == 1) {
                        // -> Yes, so this would be a decrease to zero - we're done, remove ourselves, exit.
                        shouldExit[0] = true;
                        // Returning null removes this handler from the Map.
                        return null;
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
                    return 1;
                });

                // ----- Now, either we loop since there was more to do, or we exit out since we were empty.

                // NOTICE! There is no race here, as if a new notification is just coming in, he will see an empty
                // slot in the map, and fire off a new handler. Thus, there might be two handlers running at the same
                // time: This one, which is exiting, and the new one, which is just starting.

                // ?: Should we exit?
                if (shouldExit[0]) {
                    // -> Yes, so do.
                    // We've already removed this handler (in above 'compute'), so don't bother doing it again
                    removeOnExit = false;
                    // Return out.
                    return;
                }
            }
        }
        catch (Throwable t) {
            log.error("This should never happen.", t);
        }
        finally {
            // ?: If we exited out in any other fashion than "end of messages", then we must clean up after ourselves.
            if (removeOnExit) {
                _handlersCurrentlyRunningWithNotificationCount.remove(uniqueId);
            }
        }
    }
}
