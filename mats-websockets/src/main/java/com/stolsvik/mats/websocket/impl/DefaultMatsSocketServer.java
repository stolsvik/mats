package com.stolsvik.mats.websocket.impl;

import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCode;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.DeploymentException;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler.Whole;
import javax.websocket.Session;
import javax.websocket.server.ServerContainer;
import javax.websocket.server.ServerEndpointConfig.Builder;
import javax.websocket.server.ServerEndpointConfig.Configurator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.stolsvik.mats.MatsEndpoint.DetachedProcessContext;
import com.stolsvik.mats.MatsEndpoint.MatsObject;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.websocket.MatsSocketServer;
import com.stolsvik.mats.websocket.impl.ClusterStoreAndForward.DataAccessException;
import com.stolsvik.mats.websocket.impl.ClusterStoreAndForward.StoredMessage;

/**
 * @author Endre Stølsvik 2019-11-28 12:17 - http://stolsvik.com/, endre@stolsvik.com
 */
public class DefaultMatsSocketServer implements MatsSocketServer {
    private static final Logger log = LoggerFactory.getLogger(DefaultMatsSocketServer.class);

    private static final String REPLY_TERMINATOR_ID_PREFIX = "MatsSockets.replyHandler.";

    public static MatsSocketServer createMatsSocketServer(ServerContainer serverContainer, MatsFactory matsFactory,
            ClusterStoreAndForward clusterStoreAndForward) {
        // Boot ClusterStoreAndForward
        clusterStoreAndForward.boot();

        // TODO: "Escape" the AppName.
        String replyTerminatorId = REPLY_TERMINATOR_ID_PREFIX + matsFactory.getFactoryConfig().getAppName();
        DefaultMatsSocketServer matsSocketServer = new DefaultMatsSocketServer(matsFactory, clusterStoreAndForward,
                replyTerminatorId);

        log.info("Registering MatsSockets' sole WebSocket endpoint.");
        Configurator configurator = new Configurator() {
            @Override
            @SuppressWarnings("unchecked") // The cast to (T) is not dodgy.
            public <T> T getEndpointInstance(Class<T> endpointClass) {
                if (endpointClass != MatsWebSocketInstance.class) {
                    throw new AssertionError("Cannot create Endpoints of type [" + endpointClass.getName()
                            + "]");
                }
                log.info("Instantiating a MatsSocketEndpoint!");
                return (T) new MatsWebSocketInstance(matsSocketServer);
            }
        };
        try {
            serverContainer.addEndpoint(Builder.create(MatsWebSocketInstance.class, "/matssocket/json")
                    .subprotocols(Collections.singletonList("mats"))
                    .configurator(configurator)
                    .build());
        }
        catch (DeploymentException e) {
            throw new AssertionError("Could not register MatsSocket endpoint", e);
        }
        return matsSocketServer;
    }

    // Constructor init
    private final MatsFactory _matsFactory;
    private final ClusterStoreAndForward _clusterStoreAndForward;
    private final ObjectMapper _jackson;
    private final String _replyTerminatorId;
    private final MessageToWebSocketForwarder _messageToWebSocketForwarder;

    // In-line init
    private final ConcurrentHashMap<String, MatsSocketEndpointRegistration<?, ?, ?, ?>> _matsSocketEndpointsByMatsSocketEndpointId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MatsSocketSession> _activeSessionsByMatsSocketSessionId = new ConcurrentHashMap<>();

    private DefaultMatsSocketServer(MatsFactory matsFactory, ClusterStoreAndForward clusterStoreAndForward,
            String replyTerminatorId) {
        _matsFactory = matsFactory;
        _clusterStoreAndForward = clusterStoreAndForward;
        _jackson = jacksonMapper();
        _replyTerminatorId = replyTerminatorId;

        int corePoolSize = Math.max(5, matsFactory.getFactoryConfig().getConcurrency() * 4);
        int maximumPoolSize = Math.max(100, matsFactory.getFactoryConfig().getConcurrency() * 20);
        _messageToWebSocketForwarder = new MessageToWebSocketForwarder(this,
                corePoolSize, maximumPoolSize);

        // Register our Reply-handler (common on all nodes - need forwarding to correct node)
        matsFactory.terminator(replyTerminatorId, ReplyHandleStateDto.class, MatsObject.class,
                this::mats_processMatsReply);

        // Register our Forward to WebSocket handler (common on all nodes).
        matsFactory.subscriptionTerminator(pingTerminatorIdForNode(getMyNodename()), Void.class, String.class,
                (processContext, state, matsSocketSessionId) -> mats_localSessionMessageNotify(matsSocketSessionId));
    }

    private String getMyNodename() {
        return _matsFactory.getFactoryConfig().getNodename();
    }

    private String pingTerminatorIdForNode(String nodename) {
        // TODO: "Escape" the nodename (just in case)
        return _replyTerminatorId + '.' + nodename;
    }

    private volatile Function<String, Principal> _authorizationToPrincipalFunction;

    @Override
    public <I, MI, MR, R> MatsSocketEndpoint<I, MI, MR, R> matsSocketEndpoint(String matsSocketEndpointId,
            Class<I> msIncomingClass, Class<MI> matsIncomingClass, Class<MR> matsReplyClass, Class<R> msReplyClass,
            MatsSocketEndpointIncomingAuthEval<I, MI, R> incomingAuthEval) {
        MatsSocketEndpointRegistration<I, MI, MR, R> matsSocketRegistration = new MatsSocketEndpointRegistration<>(
                matsSocketEndpointId, msIncomingClass, matsIncomingClass, matsReplyClass, msReplyClass,
                incomingAuthEval);
        MatsSocketEndpointRegistration existing = _matsSocketEndpointsByMatsSocketEndpointId.putIfAbsent(
                matsSocketEndpointId,
                matsSocketRegistration);
        // Assert that there was no existing mapping
        if (existing != null) {
            // -> There was existing mapping - shall not happen.
            throw new IllegalStateException("Cannot register a MatsSocket onto an EndpointId which already is"
                    + " taken, existing: [" + existing._incomingAuthEval + "].");
        }
        return matsSocketRegistration;
    }

    @Override
    public void setAuthorizationToPrincipalFunction(Function<String, Principal> authorizationToPrincipalFunction) {
        _authorizationToPrincipalFunction = authorizationToPrincipalFunction;
    }

    @Override
    public void shutdown() {
        log.info("Asked to shut down MatsSocketServer [" + id(this)
                + "], containing [" + _activeSessionsByMatsSocketSessionId.size() + "] sessions.");

        _messageToWebSocketForwarder.shutdown();

        _activeSessionsByMatsSocketSessionId.values().forEach(s -> {
            s.closeWebSocket(CloseCodes.SERVICE_RESTART,
                    "From Server: Server instance is going down, please reconnect.");
        });
    }

    private MatsSocketEndpointRegistration getMatsSocketRegistration(String eid) {
        MatsSocketEndpointRegistration matsSocketRegistration = _matsSocketEndpointsByMatsSocketEndpointId.get(eid);
        log.info("MatsSocketRegistration for [" + eid + "]: " + matsSocketRegistration);
        if (matsSocketRegistration == null) {
            throw new IllegalArgumentException("Cannot find MatsSocketRegistration for [" + escape(eid) + "]");
        }
        return matsSocketRegistration;
    }

    private static class ReplyHandleStateDto {
        private final String sid;
        private final String cid;
        private final String ms_eid;
        private final String ms_reid;

        private final long cmcts; // Client Message Created TimeStamp (Client timestamp)
        private final long cmrts; // Client Message Received Timestamp (Server timestamp)
        private final long mmsts; // Mats Message Sent Timestamp (when the message was sent onto Mats MQ fabric, Server
                                  // timestamp)

        private ReplyHandleStateDto() {
            /* no-args constructor for Jackson */
            sid = null;
            cid = null;
            ms_eid = null;
            ms_reid = null;
            this.cmcts = 0;
            this.cmrts = 0;
            this.mmsts = 0;
        }

        public ReplyHandleStateDto(String matsSocketSessionId, String matsSocketEndpointId, String replyEndpointId,
                String correlationId, long clientMessageCreatedTimestamp,
                long clientMessageReceivedTimestamp, long matsMessageSentTimestamp) {
            sid = matsSocketSessionId;
            cid = correlationId;
            ms_eid = matsSocketEndpointId;
            ms_reid = replyEndpointId;
            this.cmcts = clientMessageCreatedTimestamp;
            this.cmrts = clientMessageReceivedTimestamp;
            this.mmsts = matsMessageSentTimestamp;
        }
    }

    /**
     * A "random" number picked by basically hammering on the keyboard and then carefully manually randomize it some
     * more ;-) - used to string-replace the sending timestamp into the envelope on the WebSocket-forward side. If this
     * by sheer randomness appears in the actual payload message (as ASCII literals), then it might be accidentally
     * swapped out. If this happens, sorry - but you should <i>definitely</i> also sell your house and put all proceeds
     * into all of your country's Lotto systems.
     */
    private static final Long REPLACE_VALUE = 3_945_608_518_157_027_723L;

    private void mats_processMatsReply(ProcessContext<Void> processContext,
            ReplyHandleStateDto state, MatsObject incomingMsg) {
        long matsMessageReplyReceivedTimestamp = System.currentTimeMillis();

        // Find the MatsSocketEndpoint for this reply
        MatsSocketEndpointRegistration registration = getMatsSocketRegistration(state.ms_eid);

        Object matsReply = incomingMsg.toClass(registration._matsReplyClass);

        Object msReply;
        if (registration._replyAdapter != null) {
            MatsSocketEndpointReplyContextImpl replyContext = new MatsSocketEndpointReplyContextImpl(
                    registration._matsSocketEndpointId, processContext);
            msReply = registration._replyAdapter.adaptReply(replyContext, matsReply);
        }
        else if (registration._matsReplyClass == registration._msReplyClass) {
            // -> Return same class
            msReply = matsReply;
        }
        else {
            throw new AssertionError("No adapter present, but the class from Mats ["
                    + registration._matsReplyClass.getName() + "] != the expected reply from MatsSocketEndpoint ["
                    + registration._msReplyClass.getName() + "].");
        }

        // Create Envelope
        MatsSocketEnvelopeDto msReplyEnvelope = new MatsSocketEnvelopeDto();
        msReplyEnvelope.t = "REPLY";
        msReplyEnvelope.eid = state.ms_reid;
        msReplyEnvelope.cid = state.cid;
        msReplyEnvelope.tid = processContext.getTraceId(); // TODO: Chop off last ":xyz", as that is added serverside.
        msReplyEnvelope.cmcts = state.cmcts;
        msReplyEnvelope.cmrts = state.cmrts;
        msReplyEnvelope.mmsts = state.mmsts;
        msReplyEnvelope.mmrts = matsMessageReplyReceivedTimestamp;
        msReplyEnvelope.rmcts = REPLACE_VALUE;
        msReplyEnvelope.msg = msReply;

        String serializedEnvelope = serializeEnvelope(msReplyEnvelope);
        Optional<String> nodeNameHoldingWebSocket;
        try {
            nodeNameHoldingWebSocket = _clusterStoreAndForward.storeMessageForSession(
                    state.sid, processContext.getTraceId(), msReplyEnvelope.t, serializedEnvelope);
        }
        catch (DataAccessException e) {
            // TODO: Fix
            throw new AssertionError("Damn", e);
        }

        // ?: If we do have a nodename, ping it about new message
        // TODO: Local ping if it is us. Just check directly on _activeSessions...
        if (nodeNameHoldingWebSocket.isPresent()) {
            // -> Yes we got a nodename, ping it.
            processContext.initiate(init -> {
                init.traceId("PingWebSocketHolder")
                        .from(_replyTerminatorId)
                        .to(pingTerminatorIdForNode(nodeNameHoldingWebSocket.get()))
                        .publish(state.sid);
            });
        }
    }

    private void mats_localSessionMessageNotify(String matsSocketSessionId) {
        MatsSocketSession matsSocketSession = _activeSessionsByMatsSocketSessionId.get(matsSocketSessionId);
        // ?: If this Session does not exist at this node, we cannot deliver
        if (matsSocketSession == null) {
            // -> No Session here.
            log.info("Got notified about new message to MatsSocketSessionId [" + matsSocketSessionId + "], but we"
                    + " did not have that session anymore.");

            // Since someone thought that we had it, but we did not, tell ClusterStoreAndForward that we do not have it
            // (This is not dangerous, because it won't deregister any other Session home)
            try {
                _clusterStoreAndForward.deregisterSessionFromThisNode(matsSocketSessionId);
            }
            catch (DataAccessException e) {
                log.warn("Got '" + e.getClass().getSimpleName() + "' when trying to deregister MatsSocketSession"
                        + " [ms_sid:" + matsSocketSessionId + "] from this node '" + getMyNodename() + "' using '"
                        + _clusterStoreAndForward.getClass().getSimpleName() + "'. Bailing out, hoping for"
                        + " self-healer process to figure it out.", e);
                return;
            }

            notifyHomeNodeAboutNewMessage(matsSocketSessionId);
        }

        // ----- At this point, we have determined that MatsSocketSession has home here

        _messageToWebSocketForwarder.notifyMessageFor(matsSocketSession);
    }

    /**
     * Gets a ping from the node-specific Topic, or when the client reconnects.
     */
    private static class MessageToWebSocketForwarder {
        private final DefaultMatsSocketServer _matsSocketServer;
        private final ClusterStoreAndForward _clusterStoreAndForward;

        private final ThreadPoolExecutor _threadPool;

        private final Map<String, Long> _handlersCurrentlyRunningWithNotificationCount = new HashMap<>();
        private final AtomicInteger _threadNumber = new AtomicInteger();

        public MessageToWebSocketForwarder(DefaultMatsSocketServer defaultMatsSocketServer,
                int corePoolSize, int maximumPoolSize) {
            _matsSocketServer = defaultMatsSocketServer;
            _clusterStoreAndForward = defaultMatsSocketServer._clusterStoreAndForward;

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
            synchronized (_handlersCurrentlyRunningWithNotificationCount) {
                // ?: Check if we already have a message handler running for this sessionId
                Long count = _handlersCurrentlyRunningWithNotificationCount.get(matsSocketSession._matsSocketSessionId);
                // ?: Did we have any count here?
                if (count != null) {
                    // -> Yes, so just increase this, and the existing handler will take care of it.
                    _handlersCurrentlyRunningWithNotificationCount.put(matsSocketSession._matsSocketSessionId, count
                            + 1);
                    // We're done
                    return;
                }

                // E-> No, there was not a handler running, so we must make one
                // We will now fire off a handler, with 1 in count
                _handlersCurrentlyRunningWithNotificationCount.put(matsSocketSession._matsSocketSessionId, 1L);
            }

            // ----- There was no existing handler for this MatsSocketSession

            // Fire off a new handler.
            _threadPool.execute(() -> this.handlerRunnable(matsSocketSession));
        }

        void handlerRunnable(MatsSocketSession matsSocketSession) {
            String matsSocketSessionId = matsSocketSession._matsSocketSessionId;
            Session session = matsSocketSession._webSocketSession;

            try { // try-finally: Remove ourselves from the "currently running handlers".
                  // LOOP: "Re-notifications"
                while (true) {
                    // ?: Check if WebSocket Session is still open.
                    if (!session.isOpen()) {
                        log.info("When about to run forward-messages-to-websocket handler, we found that the WebSocket"
                                + " Session was closed. Notifying '"
                                + _clusterStoreAndForward.getClass().getSimpleName()
                                + "' that this MatsSocketSession does not reside here ["
                                + _matsSocketServer.getMyNodename() + "] anymore, forwarding notification to new"
                                + " MatsSocketSession home (if any), and exiting handler.");

                        // :: Deregister from this node.
                        try {
                            _clusterStoreAndForward.deregisterSessionFromThisNode(matsSocketSessionId);
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

                        // Forward to new home.
                        _matsSocketServer.notifyHomeNodeAboutNewMessage(matsSocketSessionId);
                        // We're done, exit.
                        return;
                    }

                    // LOOP: Clear out stored messages from ClusterStoreAndForward store
                    while (true) {
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

                        String concatMessages = messagesForSession.stream()
                                .map(StoredMessage::getEnvelopeJson)
                                .collect(Collectors.joining(", ", "[", "]"));

                        String traceIds = messagesForSession.stream()
                                .map(StoredMessage::getTraceId)
                                .collect(Collectors.joining(", "));

                        List<Long> messageIds = messagesForSession.stream().map(StoredMessage::getId)
                                .collect(Collectors.toList());

                        try {
                            session.getBasicRemote().sendText(concatMessages);

                            log.info("Finished sending '" + messagesForSession.size() + "' message(s) with TraceIds ["
                                    + traceIds + "] to MatsSession [" + matsSocketSession + "] over WebSocket session ["
                                    + session + "].");

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
                                return;
                            }
                        }
                        catch (IOException ioe) {
                            log.warn("Got [" + ioe.getClass().getSimpleName()
                                    + "] while trying to send '" + messagesForSession.size()
                                    + "' messages with TraceId [" + traceIds + "] to MatsSession [" + matsSocketSession
                                    + "] over WebSocket session [" + session
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
                                return;
                            }
                        }
                    }

                    // ----- The database is (was) CURRENTLY empty of messages for this session.

                    /*
                     * Since we're finished with these messages, we reduce the number of outstanding count, and if zero
                     * - remove and exit. There IS a race here: There can come in a new message WHILE we are exiting, so
                     * we might exit right when we're being notified about a new message having come in.
                     *
                     * However, this decrease vs. increase of count is done transactionally within a synchronized:
                     *
                     * Either:
                     *
                     * 1. The new message comes in. It sees the count is 1 (because a MatsSessionId- specific handler is
                     * already running), and thus increases to 2 and do NOT fire off a new handler. This has then (due
                     * to synchronized) happened before the handler come and read and reduce it, so when the handler
                     * reduce, it reduces to 1, and thus go back for one more message pull loop.
                     *
                     * 2. The new message comes in. The handler is JUST about to exit, so it reduces the count to 0, and
                     * thus remove it - and then exits. The new message then do not see a handler running (the map does
                     * not have an entry for the MatsSessionId, since it was just removed by the existing handler), puts
                     * the count in with 1, and fires off a new handler. There might now be two handlers for a brief
                     * time, but the old one is exiting, not touching the data store anymore, while the new is just
                     * starting.
                     *
                     * Furthermore: We might already have handled the new message by the SELECT already having pulled it
                     * in, before the code got time to notify us. This is not a problem: The only thing that will happen
                     * is that we loop, ask for new messages, get ZERO back, and are thus finished. Such a thing could
                     * conceivably happen many times in a row, but the ending result is always that there will ALWAYS be
                     * a "last handler round", which might, or might not, get zero messages. For every message, there
                     * will guaranteed be one handler that AFTER the INSERT will evaluate whether it is still on store.
                     * Again: Either it was sweeped up in a previous handler round, or a new handler will be dispatched.
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
                         * Now we'll do "coalescing": Firstly, one pass through the handler will clear out all messages
                         * stored for this MatsSocketSession. So, the point is that if we've got notified about several
                         * messages while doing the rounds, we've EITHER already handled them in one of the loop rounds,
                         * OR one final pass would handle any remaining. We can thus set the count down to 1, and be
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

    private void notifyHomeNodeAboutNewMessage(String matsSocketSessionId) {
        Optional<String> currentNode;
        try {
            // Find if the session resides on a different node
            currentNode = _clusterStoreAndForward.getCurrentNodeForSession(matsSocketSessionId);
        }
        catch (DataAccessException e) {
            log.warn("Got '" + e.getClass().getSimpleName() + "' when trying to find node home for"
                    + " [ms_sid:" + matsSocketSessionId + "] using '"
                    + _clusterStoreAndForward.getClass().getSimpleName() + "'. Bailing out, hoping for"
                    + " self-healer process to figure it out.", e);
            return;
        }

        // ?: Did we get a node?
        if (!currentNode.isPresent()) {
            // -> No, so nothing to do - MatsSocket will get messages when he reconnect.
            log.info("MatsSocketSession [" + matsSocketSessionId + "] is not present on any node.");
            return;
        }

        // Send message to home for MatsSocketSession
        _matsFactory.getDefaultInitiator().initiateUnchecked(init -> {
            init.traceId("MatsSystemOutgoingMessageNotify[ms_sid:" + matsSocketSessionId + "]" + rnd(5))
                    .from("MatsSocketSystem.notifyNodeAboutMessage")
                    .to(pingTerminatorIdForNode(currentNode.get()))
                    .publish(matsSocketSessionId);
        });
    }

    private void serializeAndSendSingleEnvelope(Session session, MatsSocketEnvelopeDto msReplyEnvelope) {
        String msReplyEnvelopeJson = serializeEnvelope(msReplyEnvelope);
        // TODO: Need to think this Async through wrt. to "finishing" the stored message from the storage.
        // TODO: Also, need to consider what to do wrt. timeouts that cannot be transmitted.
        log.info("Sending reply [" + msReplyEnvelopeJson + "]");
        // NOTICE: We must stick the single envelope in an JSON Array.
        session.getAsyncRemote().sendText('[' + msReplyEnvelopeJson + ']');
    }

    private String serializeEnvelope(MatsSocketEnvelopeDto msReplyEnvelope) {
        if (msReplyEnvelope.t == null) {
            throw new IllegalStateException("Type ('t') cannot be null.");
        }
        if (msReplyEnvelope.cmrts == null) {
            throw new IllegalStateException("ClientMessageReceivedTimestamp ('cmrts') cannot be null.");
        }
        if (msReplyEnvelope.rmcts == null) {
            throw new IllegalStateException("ReplyMessageClientTimestamp ('rmcts') cannot be null.");
        }
        // JSONify the MatsSocket Reply.
        try {
            return _jackson.writeValueAsString(msReplyEnvelope);
        }
        catch (JsonProcessingException e) {
            throw new AssertionError("Huh, couldn't serialize message?!");
        }
    }

    private static class MatsSocketEndpointRegistration<I, MI, MR, R> implements MatsSocketEndpoint<I, MI, MR, R> {
        private final String _matsSocketEndpointId;
        private final Class<I> _msIncomingClass;
        private final Class<MI> _matsIncomingClass;
        private final Class<MR> _matsReplyClass;
        private final Class<R> _msReplyClass;
        private final MatsSocketEndpointIncomingAuthEval _incomingAuthEval;

        public MatsSocketEndpointRegistration(String matsSocketEndpointId, Class<I> msIncomingClass,
                Class<MI> matsIncomingClass, Class<MR> matsReplyClass, Class<R> msReplyClass,
                MatsSocketEndpointIncomingAuthEval<I, MI, R> incomingAuthEval) {
            _matsSocketEndpointId = matsSocketEndpointId;
            _msIncomingClass = msIncomingClass;
            _matsIncomingClass = matsIncomingClass;
            _matsReplyClass = matsReplyClass;
            _msReplyClass = msReplyClass;
            _incomingAuthEval = incomingAuthEval;
        }

        private volatile MatsSocketEndpointReplyAdapter<MR, R> _replyAdapter;

        @Override
        public void replyAdapter(MatsSocketEndpointReplyAdapter<MR, R> replyAdapter) {
            _replyAdapter = replyAdapter;
        }
    }

    /**
     * Shall be one instance per socket (i.e. from the docs: "..there will be precisely one endpoint instance per active
     * client connection"), thus there should be 1:1 correlation between this instance and the single Session object for
     * the same cardinality (per client:server connection).
     */
    public static class MatsWebSocketInstance<I, MI, MR, R> extends Endpoint {
        private final DefaultMatsSocketServer _matsSocketServer;
        private final ObjectMapper _jackson;

        private MatsSocketSession _matsSocketSession;

        public MatsWebSocketInstance(DefaultMatsSocketServer matsSocketServer) {
            log.info("Created MatsWebSocketEndpointInstance: " + id(this));
            _matsSocketServer = matsSocketServer;
            _jackson = matsSocketServer._jackson;
        }

        @Override
        public void onOpen(Session session, EndpointConfig config) {
            log.info("WebSocket opened, session:" + session.getId() + ", endpointConfig:" + config
                    + ", endpointInstance:" + id(this) + ", session:" + id(session));

            // Set high limits, don't want to be held back on the protocol side of things.
            session.setMaxBinaryMessageBufferSize(50 * 1024 * 1024);
            session.setMaxTextMessageBufferSize(50 * 1024 * 1024);
            // TODO: Experimenting with low idle timeouts
            session.setMaxIdleTimeout(20_000);
            _matsSocketSession = new MatsSocketSession(this, session);
            session.addMessageHandler(_matsSocketSession);
        }

        @Override
        public void onError(Session session, Throwable thr) {
            log.info("WebSocket @OnError, session:" + session.getId() + ", this:" + id(this), thr);
        }

        @Override
        public void onClose(Session session, CloseReason closeReason) {
            log.info("WebSocket @OnClose, session:" + session.getId() + ", reason:" + closeReason.getReasonPhrase()
                    + ", this:" + id(this));
            // ?: Have we gotten MatsSocketSession yet?
            if (_matsSocketSession != null) {
                // -> Yes, so remove us from local and global views
                // We do not have it locally anymore
                _matsSocketServer._activeSessionsByMatsSocketSessionId.remove(_matsSocketSession._matsSocketSessionId);
                // Deregister session from the ClusterStoreAndForward
                try {
                    _matsSocketServer._clusterStoreAndForward.deregisterSessionFromThisNode(
                            _matsSocketSession._matsSocketSessionId);
                }
                catch (DataAccessException e) {
                    // TODO: Fix
                    throw new AssertionError("Damn", e);
                }
            }
        }
    }

    private static class MatsSocketSession implements Whole<String> {
        private static final JavaType LIST_OF_MSG_TYPE = TypeFactory.defaultInstance().constructType(
                new TypeReference<List<MatsSocketEnvelopeDto>>() {
                });

        private final MatsWebSocketInstance _matsWebSocketInstance;
        private final Session _webSocketSession;

        // Derived
        private final DefaultMatsSocketServer _matsSocketServer;

        // Set
        private String _matsSocketSessionId;
        private String _authorization;
        private Principal _principal;

        public MatsSocketSession(
                MatsWebSocketInstance matsWebSocketInstance, Session webSocketSession) {
            _matsWebSocketInstance = matsWebSocketInstance;
            _webSocketSession = webSocketSession;

            // Derived
            _matsSocketServer = _matsWebSocketInstance._matsSocketServer;
        }

        @Override
        public void onMessage(String message) {
            long clientMessageReceivedTimestamp = System.currentTimeMillis();
            log.info("WebSocket received message:" + message + ", session:" + _webSocketSession.getId() + ", this:"
                    + id(this));

            List<MatsSocketEnvelopeDto> envelopes;
            try {
                envelopes = _matsWebSocketInstance._jackson.readValue(message, LIST_OF_MSG_TYPE);
            }
            catch (JsonProcessingException e) {
                // TODO: Handle parse exceptions.
                throw new AssertionError("Parse exception", e);
            }

            log.info("Messages: " + envelopes);
            for (int i = 0; i < envelopes.size(); i++) {
                MatsSocketEnvelopeDto envelope = envelopes.get(i);

                // ?: Pick out any Authorization header, i.e. the auth-string
                if (envelope.auth != null) {
                    // -> Yes, there was authorization string here
                    _principal = _matsSocketServer._authorizationToPrincipalFunction.apply(envelope.auth);
                    if (_principal == null) {
                        // TODO: SEND AUTH_FAILED (also if auth function throws)
                        throw new AssertionError("The authorization header [" + escape(envelope.auth)
                                + "] did not produce a Principal.");
                    }
                    _authorization = envelope.auth;
                }

                if ("HELLO".equals(envelope.t)) {
                    // ?: Auth is required
                    if ((_principal == null) || (_authorization == null)) {
                        // TODO: SEND AUTH_FAILED
                        throw new AssertionError("The message [" + envelope.t + "] is missing Authorization header.");
                    }

                    boolean expectExisting = "EXPECT_EXISTING".equals(envelope.st);

                    // ----- We're authenticated.

                    // ?: Do we assume that there is an already existing session?
                    if (envelope.sid != null) {
                        // -> Yes, try to find it
                        // TODO: CHECK WITH THE FORWARDING WHETHER THIS SESSION ID EXISTS
                        MatsSocketSession existingSession = _matsSocketServer._activeSessionsByMatsSocketSessionId.get(
                                envelope.sid);
                        // TODO: Implement remote invalidation
                        // ?: Is there an existing?
                        if (existingSession != null) {
                            // -> Yes, thus you can use it.
                            /*
                             * NOTE: If it is open - which it "by definition" should not be - we close the *previous*.
                             * The question of whether to close this or previous: We chose previous because there might
                             * be reasons where the client feels that it has lost the connection, but the server hasn't
                             * yet found out. The client will then try to reconnect, and that is ok. So we close the
                             * existing. Since it is always the server that creates session Ids and they are large and
                             * globally unique, AND since we've already authenticated the user so things should be OK,
                             * this ID is obviously the one the client got the last time. So if he really wants to screw
                             * up his life by doing reconnects when he does not need to, then OK.
                             */
                            // ?: If the existing is open, then close it.
                            if (existingSession._webSocketSession.isOpen()) {
                                try {
                                    existingSession._webSocketSession.close(new CloseReason(CloseCodes.PROTOCOL_ERROR,
                                            "Cannot have two MatsSockets with the same SessionId - closing the previous"));
                                }
                                catch (IOException e) {
                                    log.warn("Got IOException when trying to close an existing session upon reconnect.",
                                            e);
                                }
                            }
                            // You're allowed to use this, since the sessionId was already existing.
                            _matsSocketSessionId = envelope.sid;
                        }
                    }
                    // ?: Do we have a MatsSocketSessionId by now?
                    if (_matsSocketSessionId == null) {
                        // -> No, so make one.
                        _matsSocketSessionId = rnd(16);
                    }

                    // Add Session to our active-map
                    _matsSocketServer._activeSessionsByMatsSocketSessionId.put(_matsSocketSessionId, this);
                    try {
                        _matsSocketServer._clusterStoreAndForward.registerSessionAtThisNode(_matsSocketSessionId);
                    }
                    catch (DataAccessException e) {
                        // TODO: Fix
                        throw new AssertionError("Damn", e);
                    }

                    // ----- We're now a live MatsSocketSession

                    // Send reply for the HELLO message

                    MatsSocketEnvelopeDto replyEnvelope = new MatsSocketEnvelopeDto();
                    replyEnvelope.t = "WELCOME";
                    replyEnvelope.st = (_matsSocketSessionId.equalsIgnoreCase(envelope.sid) ? "RECONNECTED" : "NEW");
                    replyEnvelope.sid = _matsSocketSessionId;
                    replyEnvelope.cid = envelope.cid;
                    replyEnvelope.tid = envelope.tid;
                    replyEnvelope.cmcts = envelope.cmcts;
                    replyEnvelope.cmrts = clientMessageReceivedTimestamp;
                    replyEnvelope.rmcts = System.currentTimeMillis();

                    // ?: Did the client expect existing session, but there was none?
                    if (expectExisting) {
                        // -> Yes, so then we drop any pipelined messages
                        replyEnvelope.drop = envelopes.size() - 1 - i;
                        // Set i to size() to stop iteration.
                        i = envelopes.size();
                    }
                    // Send message
                    _matsSocketServer.serializeAndSendSingleEnvelope(_webSocketSession, replyEnvelope);
                    continue;
                }

                if ("PING".equals(envelope.t)) {
                    // TODO: HANDLE PING
                    continue;
                }

                if ("CLOSE_SESSION".equals(envelope.t)) {
                    _matsSocketServer._activeSessionsByMatsSocketSessionId.remove(_matsSocketSessionId);
                    try {
                        _matsSocketServer._clusterStoreAndForward.terminateSession(_matsSocketSessionId);
                    }
                    catch (DataAccessException e) {
                        // TODO: Fix
                        throw new AssertionError("Damn", e);
                    }
                    closeWebSocket(CloseCodes.NORMAL_CLOSURE, "From Server: Client said CLOSE_SESSION (" +
                            escape(envelope.desc) + "): Deleting session, closing WebSocket.");
                    continue;
                }

                // ?: We do not accept other messages before authentication
                if (_matsSocketSessionId == null) {
                    // TODO: Handle error
                    throw new AssertionError("Introduce yourself with HELLO. You can also do PING.");
                }

                // ----- We are authenticated.

                if ("SEND".equals(envelope.t) || "REQUEST".equals(envelope.t)) {
                    String eid = envelope.eid;
                    log.info("  \\- " + envelope.t + " to:[" + eid + "], reply:[" + envelope.reid + "], msg:["
                            + envelope.msg + "].");
                    MatsSocketEndpointRegistration registration = _matsSocketServer.getMatsSocketRegistration(eid);
                    MatsSocketEndpointIncomingAuthEval matsSocketEndpointIncomingForwarder = registration._incomingAuthEval;
                    log.info("MatsSocketEndpointHandler for [" + eid + "]: " + matsSocketEndpointIncomingForwarder);

                    Object msg = deserialize((String) envelope.msg, registration._msIncomingClass);
                    MatsSocketEndpointRequestContextImpl<?, ?> matsSocketContext = new MatsSocketEndpointRequestContextImpl(
                            _matsSocketServer, registration, _matsSocketSessionId, envelope,
                            clientMessageReceivedTimestamp, _authorization, _principal, msg);
                    matsSocketEndpointIncomingForwarder.handleIncoming(matsSocketContext, _principal, msg);
                    long sentTimestamp = System.currentTimeMillis();

                    // ?: If SEND and we got a reply address, then insta-reply
                    if ("SEND".equals(envelope.t) && envelope.reid != null) {
                        // -> Yes, SEND, so create the reply message right here
                        MatsSocketEnvelopeDto replyEnvelope = new MatsSocketEnvelopeDto();
                        replyEnvelope.t = "RECEIVED";
                        replyEnvelope.eid = envelope.reid;
                        replyEnvelope.sid = _matsSocketSessionId;
                        replyEnvelope.cid = envelope.cid;
                        replyEnvelope.tid = envelope.tid;
                        replyEnvelope.cmcts = envelope.cmcts;
                        replyEnvelope.cmrts = clientMessageReceivedTimestamp;
                        replyEnvelope.mmsts = sentTimestamp;
                        replyEnvelope.rmcts = sentTimestamp;
                        // Send message
                        _matsSocketServer.serializeAndSendSingleEnvelope(_webSocketSession, replyEnvelope);
                    }

                    continue;
                }
            }
        }

        private void closeWebSocket(CloseCode closeCode, String reasonPhrase) {
            log.info("Shutting down WebSocket Session [" + _webSocketSession + "]");
            try {
                _webSocketSession.close(new CloseReason(closeCode, reasonPhrase));
            }
            catch (IOException e) {
                log.warn("Got Exception when trying to close WebSocket Session [" + _webSocketSession
                        + "], ignoring.", e);
            }
        }

        private <T> T deserialize(String serialized, Class<T> clazz) {
            try {
                return _matsWebSocketInstance._jackson.readValue(serialized, clazz);
            }
            catch (JsonProcessingException e) {
                // TODO: Handle parse exceptions.
                throw new AssertionError("Damn", e);
            }
        }
    }

    private static class MatsSocketEnvelopeDto {
        String clv; // Client Lib and Versions, e.g.
        // "MatsSockLibCsharp,v2.0.3; iOS,v13.2"
        // "MatsSockLibAlternativeJava,v12.3; ASDKAndroid,vKitKat.4.4"
        // Java lib: "MatsSockLibJava,v0.8.9; Java,v11.03:Windows,v2019"
        // browsers/JS: "MatsSocketJs,v0.8.9; User-Agent: <navigator.userAgent string>",
        String an; // AppName
        String av; // AppVersion

        String auth; // Authorization header

        String t; // Type
        String st; // "SubType": AUTH_FAIL:"enum", EXCEPTION:Classname, MSGERROR:"enum"
        String desc; // Description of "st" of failure, exception message, multiline, may include stacktrace if authz.
        String inMsg; // On MSGERROR: Incoming Message, BASE64 encoded.
        Integer drop; // How many messages was dropped on an "EXPECT" that went sour.

        Long cmcts; // Client Message Created TimeStamp (when message was created on Client side, Client timestamp)
        Long cmrts; // Client Message Received Timestamp (when message was received on Server side, Server timestamp)
        Long mmsts; // Mats Message Sent Timestamp (when the message was sent onto Mats MQ fabric, Server timestamp)
        Long mmrts; // Mats Message Reply/Recv Timestamp (when the message was received from Mats, Server timestamp)
        Long rmcts; // Reply Message to Client Timestamp (when the message was replied to Client side, Server timestamp)

        String tid; // TraceId
        String sid; // SessionId
        String cid; // CorrelationId
        String eid; // target MatsSocketEndpointId
        String reid; // reply MatsSocketEndpointId

        @JsonDeserialize(using = MessageToStringDeserializer.class)
        Object msg; // Message, JSON

        DebugDto dbg; // Debug

        @Override
        public String toString() {
            return "[" + t + (st == null ? "" : ":" + st) + "]->"
                    + eid + (reid == null ? "" : ",reid:" + reid)
                    + ",tid:" + tid + ",cid:" + cid;
        }
    }

    private static class DebugDto {
        String d; // Description
        List<LogLineDto> l; // Log - this will be appended to if debugging is active.
    }

    private static class LogLineDto {
        long ts; // TimeStamp
        String s; // System: "MatsSockets", "Mats", "MS SQL" or similar.
        String hos; // Host OS, e.g. "iOS,v13.2", "Android,vKitKat.4.4", "Chrome,v123:Windows,vXP",
                    // "Java,v11.03:Windows,v2019"
        String an; // AppName
        String av; // AppVersion
        String t; // Thread name
        int level; // 0=TRACE, 1=DEBUG, 2=INFO, 3=WARN, 4=ERROR
        String m; // Message
        String x; // Exception if any, null otherwise.
        Map<String, String> mdc; // The MDC
    }

    private static class MatsSocketEndpointRequestContextImpl<MI, R> implements
            MatsSocketEndpointRequestContext<MI, R> {
        private final DefaultMatsSocketServer _matsSocketServer;
        private final MatsSocketEndpointRegistration _matsSocketEndpointRegistration;

        private final String _matsSocketSessionId;

        private final MatsSocketEnvelopeDto _envelope;
        private final long _clientMessageReceivedTimestamp;

        private final String _authorization;
        private final Principal _principal;
        private final MI _incomingMessage;

        public MatsSocketEndpointRequestContextImpl(DefaultMatsSocketServer matsSocketServer,
                MatsSocketEndpointRegistration matsSocketEndpointRegistration, String matsSocketSessionId,
                MatsSocketEnvelopeDto envelope, long clientMessageReceivedTimestamp, String authorization,
                Principal principal, MI incomingMessage) {
            _matsSocketServer = matsSocketServer;
            _matsSocketEndpointRegistration = matsSocketEndpointRegistration;
            _matsSocketSessionId = matsSocketSessionId;
            _envelope = envelope;
            _clientMessageReceivedTimestamp = clientMessageReceivedTimestamp;
            _authorization = authorization;
            _principal = principal;
            _incomingMessage = incomingMessage;
        }

        @Override
        public String getMatsSocketEndpointId() {
            return _envelope.eid;
        }

        @Override
        public String getAuthorization() {
            return _authorization;
        }

        @Override
        public Principal getPrincipal() {
            return _principal;
        }

        @Override
        public MI getMatsSocketIncomingMessage() {
            return _incomingMessage;
        }

        @Override
        public void forwardInteractiveUnreliable(MI matsMessage) {
            forwardCustom(matsMessage, customInit -> {
                customInit.to(getMatsSocketEndpointId());
                customInit.nonPersistent();
                customInit.interactive();
            });
        }

        @Override
        public void forwardInteractivePersistent(MI matsMessage) {
            forwardCustom(matsMessage, customInit -> {
                customInit.to(getMatsSocketEndpointId());
                customInit.interactive();
            });
        }

        @Override
        public void forwardCustom(MI matsMessage, InitiateLambda customInit) {
            _matsSocketServer._matsFactory.getDefaultInitiator().initiateUnchecked(init -> {
                init.from("MatsSocketEndpoint." + _envelope.eid)
                        .traceId(_envelope.tid);
                if (isRequest()) {
                    ReplyHandleStateDto sto = new ReplyHandleStateDto(_matsSocketSessionId,
                            _matsSocketEndpointRegistration._matsSocketEndpointId, _envelope.reid,
                            _envelope.cid, _envelope.cmcts, _clientMessageReceivedTimestamp,
                            System.currentTimeMillis());
                    // Set ReplyTo parameter
                    init.replyTo(_matsSocketServer._replyTerminatorId, sto);
                    // Invoke the customizer
                    customInit.initiate(init);
                    // Send the REQUEST message
                    init.request(matsMessage);
                }
                else {
                    // Invoke the customizer
                    customInit.initiate(init);
                    // Send the SEND message
                    init.send(matsMessage);
                }
            });
        }

        @Override
        public boolean isRequest() {
            return _envelope.t.equals("REQUEST");
        }

        @Override
        public void reply(Object matsSocketReplyMessage) {

        }
    }

    private static class MatsSocketEndpointReplyContextImpl implements MatsSocketEndpointReplyContext {
        private final String _matsSocketEndpointId;
        private final DetachedProcessContext _detachedProcessContext;

        public MatsSocketEndpointReplyContextImpl(String matsSocketEndpointId,
                DetachedProcessContext detachedProcessContext) {
            _matsSocketEndpointId = matsSocketEndpointId;
            _detachedProcessContext = detachedProcessContext;
        }

        @Override
        public String getMatsSocketEndpointId() {
            return _matsSocketEndpointId;
        }

        @Override
        public DetachedProcessContext getMatsContext() {
            return _detachedProcessContext;
        }

        @Override
        public void addBinary(String key, byte[] payload) {
            throw new IllegalStateException("Not implemented. Yet.");
        }
    }

    protected ObjectMapper jacksonMapper() {
        // NOTE: This is stolen directly from MatsSerializer_DefaultJson.
        ObjectMapper mapper = new ObjectMapper();

        // Read and write any access modifier fields (e.g. private)
        mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

        // Drop nulls
        mapper.setSerializationInclusion(Include.NON_NULL);

        // If props are in JSON that aren't in Java DTO, do not fail.
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // Write e.g. Dates as "1975-03-11" instead of timestamp, and instead of array-of-ints [1975, 3, 11].
        // Uses ISO8601 with milliseconds and timezone (if present).
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        // Handle Optional, OptionalLong, OptionalDouble
        mapper.registerModule(new Jdk8Module());

        return mapper;
    }

    private static class MessageToStringDeserializer extends JsonDeserializer<Object> {
        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            // TODO / OPTIMIZE: Find faster way to get as String, avoiding tons of JsonNode objects.
            // TODO: Trick must be to just consume from the START_OBJECT to the /corresponding/ END_OBJECT.
            return p.readValueAsTree().toString();
        }
    }

    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    /**
     * @param length
     *            the desired length of the returned random string.
     * @return a random string of the specified length.
     */
    public static String rnd(int length) {
        StringBuilder buf = new StringBuilder(length);
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        for (int i = 0; i < length; i++)
            buf.append(ALPHABET.charAt(tlr.nextInt(ALPHABET.length())));
        return buf.toString();
    }

    private static String id(Object x) {
        return x.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(x));
    }

    private static String escape(String string) {
        // TODO: Implement HTML escaping (No messages from us should not go through JSONifying already).
        return string;
    }
}
