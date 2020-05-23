package com.stolsvik.mats.websocket.impl;

import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.REJECT;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.RESOLVE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.stolsvik.mats.websocket.AuthenticationPlugin.DebugOption;
import com.stolsvik.mats.websocket.ClusterStoreAndForward;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.DataAccessException;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.StoredOutMessage;
import com.stolsvik.mats.websocket.MatsSocketServer.ActiveMatsSocketSession.MatsSocketSessionState;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto.Direction;
import com.stolsvik.mats.websocket.MatsSocketServer.MessageType;

/**
 * Gets a ping from the node-specific Topic with information about new messages, and also if the client reconnects, and
 * also if we are "on hold" due to awaiting new authentication from Client.
 *
 * @author Endre Stølsvik 2019-12 - http://stolsvik.com/, endre@stolsvik.com
 */
class WebSocketOutboxForwarder implements MatsSocketStatics {
    private static final Logger log = LoggerFactory.getLogger(WebSocketOutboxForwarder.class);

    private final DefaultMatsSocketServer _matsSocketServer;
    private final ClusterStoreAndForward _clusterStoreAndForward;

    private final SaneThreadPoolExecutor _threadPool;
    private final ConcurrentHashMap<String, Integer> _forwardersCurrentlyRunningWithNotificationCount = new ConcurrentHashMap<>();

    public WebSocketOutboxForwarder(DefaultMatsSocketServer defaultMatsSocketServer,
            ClusterStoreAndForward clusterStoreAndForward, int corePoolSize, int maxPoolSize) {
        _matsSocketServer = defaultMatsSocketServer;
        _clusterStoreAndForward = clusterStoreAndForward;

        _threadPool = new SaneThreadPoolExecutor(corePoolSize, maxPoolSize, this.getClass().getSimpleName(),
                _matsSocketServer.serverId());

        log.info("Instantiated [" + this.getClass().getSimpleName() + "] for [" + _matsSocketServer.serverId()
                + "], ThreadPool:[" + _threadPool + "]");
    }

    void shutdown(int gracefulShutdownMillis) {
        log.info("Shutting down [" + this.getClass().getSimpleName() + "] for [" + _matsSocketServer.serverId()
                + "], ThreadPool [" + _threadPool + "]");
        _threadPool.shutdownNice(gracefulShutdownMillis);
    }

    void newMessagesInCsafNotify(MatsSocketSessionAndMessageHandler matsSocketSessionAndMessageHandler) {
        log.info("newMessagesInCsafNotify for MatsSocketSessionId:[" + matsSocketSessionAndMessageHandler
                .getMatsSocketSessionId()
                + "]");
        /*
         * Either there is already running a Forwarder for this MatsSocketSessionAndMessageHandler, in which case with
         * "latch on to" that one, or there is not, in which case we create one.
         */
        String instanceId = matsSocketSessionAndMessageHandler.getInstanceId();
        boolean[] fireOffNewForwarder = new boolean[1];
        _forwardersCurrentlyRunningWithNotificationCount.compute(instanceId, (s, count) -> {
            // ?: Check whether there is an existing handler in place
            if (count == null) {
                // -> No there are not, so we need to fire off one
                fireOffNewForwarder[0] = true;
                // The new count is 1
                return 1;
            }
            // E-> There was existing.
            // The new count is whatever it was + 1
            return count + 1;
        });

        // ?: Should we fire off new handler?
        if (fireOffNewForwarder[0]) {
            // -> Yes, none were running, so fire off new handler.
            _threadPool.execute(() -> this.forwarderRunnable(matsSocketSessionAndMessageHandler));
        }
    }

    void forwarderRunnable(MatsSocketSessionAndMessageHandler matsSocketSessionAndMessageHandler) {
        String matsSocketSessionId = matsSocketSessionAndMessageHandler.getMatsSocketSessionId();
        String instanceId = matsSocketSessionAndMessageHandler.getInstanceId();

        // If we exit out uncontrolled, we must clean up this forwarder from map of currently running forwarders.
        // (This flag is set to false when we do a controlled exit).
        boolean removeOnExit = true;

        try { // try-finally: clear MDC, try-catchAll: Log Exception (should not occur!)
            MDC.put(MDC_SESSION_ID, matsSocketSessionId);
            MDC.put(MDC_USER_ID, matsSocketSessionAndMessageHandler.getUserId());
            MDC.put(MDC_PRINCIPAL_NAME, matsSocketSessionAndMessageHandler.getPrincipalName().orElse(
                    "{EMPTY:Should not happen}"));

            // LOOP: "Re-notifications" - i.e. a new message have come in while we run this Forward-round
            RENOTIFY: while (true) {
                // ?: Should we do a forward-round, or hold outgoing messages?
                // (Waiting for "AUTH" answer from Client to our "REAUTH" request)
                boolean runMessageForward = !matsSocketSessionAndMessageHandler.isHoldOutgoingMessages();
                /*
                 * NOTE: When the auth comes in, it will set the hold to 'false', and /then/ do a notify
                 * [newMessagesInCsafNotify(..)] , thus /either/ force a new round for /this/ handlerRunnable, /or/
                 * start a new handlerRunnable.
                 */

                // LOOP: Clear out messages currently outbox'ed messages in CSAF, by forwarding and mark delivered.
                while (runMessageForward) {

                    // ?: Check if the MatsSocketSessionAndMessageHandler is still SESSION_ESTABLISHED
                    if (!matsSocketSessionAndMessageHandler.isSessionEstablished()) {
                        // -> Not SESSION_ESTABLISHED - forward to new home if relevant
                        log.info("When about to run forward-messages-to-websocket handler, we found that the "
                                + "MatsSocketSession was not in state " + MatsSocketSessionState.SESSION_ESTABLISHED
                                + ", but [" + matsSocketSessionAndMessageHandler.getState()
                                + "], forwarding to new MatsSocketSession home (if any), and exiting handler.");
                        // Forward to new home, if any (Note: It can theoretically be us (i.e. this node), in another
                        // MatsSocketMessageHandler, due to race wrt. WebSocket close & reconnect)
                        _matsSocketServer.newMessageOnWrongNode_NotifyCorrectHome(matsSocketSessionId);
                        // We're done, nothing more we can do - exit.
                        return;
                    }

                    // ?: Check if WebSocket Session (i.e. the connection) is still open.
                    /*
                     * (This should really not ever happen, as the check above should have caught the situation.
                     * However, one could envision that the WebSocket has just closed, but not yet gotten to notify us
                     * about the situation - if it had notified us, we would NOT have been in state SESSION_ESTABLISHED.
                     * This check is just for good measure, ensuring that we start out with a WebSocket that is open..
                     * It could of course close right after the check, which then will cause an IOException when trying
                     * to send the message.)
                     */
                    if (!matsSocketSessionAndMessageHandler.isWebSocketSessionOpen()) {
                        // -> Not Open WebSocket - forward to new home if relevant
                        log.info("When about to run forward-messages-to-websocket handler, we found that the WebSocket"
                                + " Session was closed. Forwarding notification to new MatsSocketSession home (if any),"
                                + " and exiting handler.");
                        // Forward to new home, if any (Note: It can theoretically be us (i.e. this node), in another
                        // MatsSocketMessageHandler, due to race wrt. WebSocket close & reconnect)
                        _matsSocketServer.newMessageOnWrongNode_NotifyCorrectHome(matsSocketSessionId);
                        // We're done, nothing more we can do - exit.
                        return;
                    }

                    // :: Get messages from CSAF
                    long nanos_start_ClearOutRound = System.nanoTime();
                    List<StoredOutMessage> messagesToDeliver;
                    try {
                        messagesToDeliver = _clusterStoreAndForward
                                .getMessagesFromOutbox(matsSocketSessionId, MAX_NUMBER_OF_MESSAGES_PER_FORWARD_LOOP);
                    }
                    catch (DataAccessException e) {
                        log.warn("Got problems when trying to load messages from CSAF."
                                + " Bailing out, hoping for self-healer process to figure it out.", e);
                        // Bail out.
                        return;
                    }
                    double millisGetMessages = msSince(nanos_start_ClearOutRound);

                    // If we got LESS than the MAX, it means that we at this point had cleared out outbox.
                    boolean clearedOutbox = messagesToDeliver.size() < MAX_NUMBER_OF_MESSAGES_PER_FORWARD_LOOP;

                    // ?: Did we end up with zero messages?
                    if (messagesToDeliver.isEmpty()) {
                        // -> Yes, it was empty - break out to ordinary 'RENOTIFY' evaluation.
                        break;
                    }

                    // :: Now do authentication check for whether we're still good to go wrt. sending these messages.
                    boolean authOk = matsSocketSessionAndMessageHandler.reevaluateAuthenticationForOutgoingMessage();
                    // ?: Was Auth OK for doing a outgoing message forward round?
                    if (!authOk) {
                        // -> No, Auth not OK: Send "REAUTH" message, to get Client to send us new auth
                        matsSocketSessionAndMessageHandler.webSocketSendText(
                                "[{\"t\":\"" + MessageType.REAUTH + "\"}]");
                        /*
                         * NOTE: The not-ok return above will also have set "holdOutgoingMessages", which is evaluated
                         * at the very top of the outer 'RENOTIFY' loop.
                         *
                         * NOTE: When the auth comes in, it will set the hold to 'false', and /then/ do a notify
                         * [newMessagesInCsafNotify(..)] , thus /either/ force a new round for /this/ handlerRunnable,
                         * /or/ start a new handlerRunnable.
                         */
                        // We're finished with this particular round - break out to ordinary 'RENOTIFY' evaluation.
                        break;
                    }

                    // :: If there are any messages with deliveryCount > 0, then try to send these alone.
                    List<StoredOutMessage> redeliveryMessages = messagesToDeliver.stream()
                            .filter(m -> m.getDeliveryCount() > 0)
                            .collect(Collectors.toList());
                    // ?: Did we have any with deliveryCount > 0?
                    if (!redeliveryMessages.isEmpty()) {
                        // -> Yes, there are redeliveries here. Pick the first of them and try to deliver alone.
                        /*
                         * NOTICE that this is a very crude implementation, as it will simply ditch all the other gotten
                         * messages, relying on getting them again in the next round. However, since this should happen
                         * rather seldom (like pretty much never), it shouldn't make a big impact in overall
                         * performance.
                         */
                        log.info("Of the [" + messagesToDeliver.size() + "] messages for MatsSocketSessionId ["
                                + matsSocketSessionId + "], [" + redeliveryMessages.size() + "] had deliveryCount > 0."
                                + " Trying to deliver these one by one, by picking first.");
                        // Set the 'messagesToDeliver' to first of the ones with delivery count > 0.
                        messagesToDeliver = Collections.singletonList(redeliveryMessages.get(0));

                        // We're quite probably not cleared out now, as we possibly ditched some of the gotten msgs.
                        clearedOutbox = false;
                    }

                    // :: Get the MessageIds to deliver (as List, for CSAF) and TraceIds (as String, for logging)
                    List<String> messageIds = messagesToDeliver.stream()
                            .map(StoredOutMessage::getServerMessageId)
                            .collect(Collectors.toList());
                    String messageTypesAndTraceIds = messagesToDeliver.stream()
                            .map(msg -> "{" + msg.getType() + "} " + msg.getTraceId())
                            .collect(Collectors.joining(", "));
                    String traceIds = messagesToDeliver.stream()
                            .map(StoredOutMessage::getTraceId)
                            .collect(Collectors.joining(", "));
                    MDC.put(MDC_TRACE_ID, traceIds);

                    long now = System.currentTimeMillis();

                    class Meta {
                        Long requestTimestamp;
                        String cmid;

                        Long serverInitiatedTimestamp;
                        String serverInitiatedNodeName;
                    }

                    // :: Deserialize envelopes back to DTO, and stick in the message

                    // The resulting envelopes to send
                    List<MatsSocketEnvelopeWithMetaDto> envelopeList = new ArrayList<>(messagesToDeliver.size());
                    // Identity-mapping to "Meta" for these messages.
                    IdentityHashMap<MatsSocketEnvelopeWithMetaDto, Meta> envelopeToMeta = new IdentityHashMap<>(
                            messagesToDeliver.size());
                    messagesToDeliver.forEach(storedOutMessage -> {
                        MatsSocketEnvelopeWithMetaDto envelope;

                        try {
                            envelope = _matsSocketServer.getEnvelopeObjectReader().readValue(
                                    storedOutMessage.getEnvelope());
                        }
                        catch (JsonProcessingException e) {
                            throw new AssertionError("Could not deserialize Envelope DTO.", e);
                        }
                        // Make the "Meta" for this envelope
                        Meta meta = new Meta();
                        envelopeToMeta.put(envelope, meta);

                        // If we have requestTimestamp in StoredOutMessage, then store it in Meta
                        meta.cmid = storedOutMessage.getClientMessageId().orElse(null);
                        meta.requestTimestamp = storedOutMessage.getRequestTimestamp().orElse(null);

                        // Set the message onto the envelope, in "DirectJson" mode (it is already json)
                        envelope.msg = DirectJson.of(storedOutMessage.getMessageText());

                        // Add the Envelope to the pipeline we should send.
                        envelopeList.add(envelope);

                        // :: Handle Debug-part of Envelope depending on DebugOptions.
                        // Note:
                        // # Replies to Client-initiated (RESOLVE/REJECT) has Debug if it was requested in REQUEST
                        // # Server-initiated (SEND/REQUEST) /always/ has Debug, which we remove if not wanted.
                        // ?: Do we have the Debug-part of the Envelope?
                        if (envelope.debug != null) {
                            // -> Yes, we do have Debug-part of Envelope. Amend it, or ditch it.
                            /*
                             * Client- vs. Server-initiated:
                             *
                             * Now, for Client-initiated (i.e. REQUEST - this message is a RESOLVE or REJECT), things
                             * have already been resolved: If we have a DebugDto, then it is because the user both
                             * requests debug for /something/, and he is allowed to query for this.
                             *
                             * However, for Server-initiated, we do not know until now (in context of the
                             * MatsSocketSession), and thus the initiation always adds it. We thus need to check with
                             * the AuthenticationPlugin's resolved auth vs. what the client has asked for wrt.
                             * Server-initiated.
                             */

                            // ?: Is this a Reply to a Client-to-Server REQUEST? (RESOLVE or REJECT)?
                            if ((RESOLVE == storedOutMessage.getType())
                                    || REJECT == storedOutMessage.getType()) {
                                // -> Yes, Reply (RESOLVE or REJECT)
                                // Find which resolved DebugOptions are in effect for this message
                                EnumSet<DebugOption> debugOptions = DebugOption.enumSetOf(
                                        envelope.debug.resd);
                                // Add timestamp and nodename depending on options
                                if (debugOptions.contains(DebugOption.TIMESTAMPS)) {
                                    envelope.debug.mscts = now;
                                }
                                if (debugOptions.contains(DebugOption.NODES)) {
                                    envelope.debug.mscnn = _matsSocketServer.getMyNodename();
                                }
                            }

                            // ?: Is this a Server-initiated message (SEND or REQUEST)?
                            if ((MessageType.SEND == storedOutMessage.getType())
                                    || MessageType.REQUEST == storedOutMessage.getType()) {
                                // -> Yes, Server-to-Client (SEND or REQUEST)
                                // Store the initiation timestamp in the Meta
                                meta.serverInitiatedTimestamp = envelope.debug.smcts;
                                meta.serverInitiatedNodeName = envelope.debug.smcnn;

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
                                        // Need to null this out, since set unconditionally upon server
                                        // send/request
                                        envelope.debug.smcts = null;
                                    }
                                    if (debugOptions.contains(DebugOption.NODES)) {
                                        envelope.debug.mscnn = _matsSocketServer.getMyNodename();
                                    }
                                    else {
                                        // Need to null this out, since set unconditionally upon server
                                        // send/request
                                        envelope.debug.smcnn = null;
                                    }
                                }
                            }
                        }
                    });

                    // ----- We have all Envelopes that should go into this pipeline

                    // Now, if there are any Replies, we can elide the sending of ACKs if it is not done yet
                    for (MatsSocketEnvelopeWithMetaDto envelope : envelopeList) {
                        if ((envelope.t == RESOLVE) || (envelope.t == REJECT)) {
                            _matsSocketServer.getWebSocketOutgoingAcks().removeAck(matsSocketSessionId, envelope.cmid);
                        }
                    }

                    // Serialize the list of Envelopes
                    String jsonEnvelopeList = _matsSocketServer.getEnvelopeListObjectWriter()
                            .writeValueAsString(envelopeList);

                    // Register last activity time
                    matsSocketSessionAndMessageHandler.registerActivityTimestamp(System.currentTimeMillis());

                    // ===== SENDING OVER WEBSOCKET!

                    // :: Forward message(s) over WebSocket
                    double millisSendMessages;
                    try {
                        // :: Actually send message(s) over WebSocket.
                        long nanos_start_SendMessage = System.nanoTime();
                        matsSocketSessionAndMessageHandler.webSocketSendText(jsonEnvelopeList);
                        millisSendMessages = msSince(nanos_start_SendMessage);

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
                            // Bail out, since if DB is down, we won't be able to load-and-forward any more msgs.
                            return;
                        }

                        // :: Find messages with too many redelivery attempts
                        // (Note: For current code, this should always only be max one..)
                        // (Note: We're using the "old" delivery_count number (before above increase) -> no problem)
                        List<StoredOutMessage> dlqMessages = messagesToDeliver.stream()
                                .filter(m -> m.getDeliveryCount() > MAX_NUMBER_OF_REDELIVERY_ATTEMPTS)
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
                                // Bail out, since if DB is down, we won't be able to load-and-forward any more msgs.
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
                            // Bail out, since nothing else than shutdown should result in interrupt.
                            return;
                        }
                        /*
                         * Run new "re-notification" loop, to check if socket still open, then try again - or if the
                         * MatsSocketSession is closed/deregistered, try to forward to new home if it has changed.
                         */
                        continue RENOTIFY;
                    }

                    // :: Notify of Envelope sent
                    // Add meta to envelope
                    envelopeList.forEach(envelope -> {
                        Meta meta = envelopeToMeta.get(envelope);

                        envelope.ints = meta.serverInitiatedTimestamp;
                        envelope.innn = meta.serverInitiatedNodeName;

                        envelope.icts = meta.requestTimestamp;
                        envelope.rttm = (double) (System.currentTimeMillis() - meta.requestTimestamp);
                    });

                    // Record the envelope
                    matsSocketSessionAndMessageHandler.recordEnvelopes(envelopeList, System.currentTimeMillis(),
                            Direction.S2C);

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
                    double millisMarkComplete = msSince(nanos_start_MarkComplete);

                    // ----- Good path!

                    double millisTotalClearRound = msSince(nanos_start_ClearOutRound);

                    log.info("Finished sending " + messagesToDeliver.size() + " message(s) ["
                            + messageTypesAndTraceIds + "] to [" + matsSocketSessionAndMessageHandler
                            + "]. Total clear round took:[" + millisTotalClearRound + " ms], from which Get-from-CSAF"
                            + " took:[" + millisGetMessages + " ms], send over websocket took:[" + millisSendMessages
                            + " ms], mark delivery attempt in CSAF took:[" + millisMarkComplete + " ms].");

                    // ?: Did we empty out the outbox (i.e. got less than MAX number of messages)
                    if (clearedOutbox) {
                        // -> Yes, so prepare to exit - but check the Notification Count
                        break;
                    }
                }

                // ----- The database outbox was empty of messages for this session.

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
                _forwardersCurrentlyRunningWithNotificationCount.compute(instanceId, (s, count) -> {
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
                     *
                     * Example: If two more messages came in while we were doing these at-the-time final rounds, the
                     * count would be increased to 3. The messages would either be swept in the rounds currently
                     * running, or not. But the count is 3, so when we come here, we reduce it to 1, and do a sweep,
                     * either then not finding any messages (since they were already swept), or picking those two up and
                     * forward them - and then reduce from 1 to 0 and exit. This argument holds forever..!
                     */
                    return 1;
                });

                // ----- Now, either we loop since there was more to do, or we exit out since we were empty.

                /*
                 * NOTICE! IF there is a race here because a new notification is /just coming in/, that notification
                 * will see an empty slot in the map, and fire off a new handler. Thus, there might be two handlers
                 * running at the same time: This one, which is exiting, and the new one, which is just starting.
                 */

                // ?: Should we exit?
                if (shouldExit[0]) {
                    // -> Yes, so do.
                    // We've already removed this handler (in above 'compute'), so must not do it again in the finally
                    // handling: It could be a new instance that was just fired up.
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
                // -> Evidently a "crash out" of some sort, so clean up as best we can, hoping for self healer.
                _forwardersCurrentlyRunningWithNotificationCount.remove(instanceId);
            }
            MDC.clear();
        }
    }
}
