package com.stolsvik.mats.websocket.impl;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.regex.Pattern;

import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.DeploymentException;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.Session;
import javax.websocket.server.ServerContainer;
import javax.websocket.server.ServerEndpointConfig.Builder;
import javax.websocket.server.ServerEndpointConfig.Configurator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.stolsvik.mats.MatsEndpoint.DetachedProcessContext;
import com.stolsvik.mats.MatsEndpoint.MatsObject;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.websocket.MatsSocketServer;
import com.stolsvik.mats.websocket.impl.ClusterStoreAndForward.CurrentNode;
import com.stolsvik.mats.websocket.impl.ClusterStoreAndForward.DataAccessException;

/**
 * @author Endre Stølsvik 2019-11-28 12:17 - http://stolsvik.com/, endre@stolsvik.com
 */
public class DefaultMatsSocketServer implements MatsSocketServer {
    private static final Logger log = LoggerFactory.getLogger(DefaultMatsSocketServer.class);

    private static final String REPLY_TERMINATOR_ID_PREFIX = "MatsSockets.replyHandler.";

    /**
     * Create a MatsSocketServer, piecing together necessary bits.
     *
     * @param serverContainer
     *            the WebSocket {@link ServerContainer}, typically gotten from the Servlet Container.
     * @param matsFactory
     *            The {@link MatsFactory} which we should hook into for both sending requests and setting up endpoints
     *            to receive replies.
     * @param clusterStoreAndForward
     *            an implementation of {@link ClusterStoreAndForward} which temporarily holds replies while finding the
     *            right node that holds the WebSocket connection - and hold them till the client reconnects in case he
     *            has disconnected in the mean time.
     * @param authorizationToPrincipalFunction
     *            a Function that turns an Authorization String into a Principal. This is mandatory. Must be pretty
     *            fast, as it is invoked synchronously - keep any IPC fast and keep relatively short timeouts, otherwise
     *            all your threads of the container might be used up. If the function throws or returns null,
     *            authorization did not go through.
     *
     * @return a MatsSocketServer instance, now hooked into both the WebSocket {@link ServerContainer} and the
     *         {@link MatsFactory}.
     */
    public static MatsSocketServer createMatsSocketServer(ServerContainer serverContainer,
            MatsFactory matsFactory,
            ClusterStoreAndForward clusterStoreAndForward,
            Function<String, Principal> authorizationToPrincipalFunction) {
        // Boot ClusterStoreAndForward
        clusterStoreAndForward.boot();

        // TODO: "Escape" the AppName.
        String replyTerminatorId = REPLY_TERMINATOR_ID_PREFIX + matsFactory.getFactoryConfig().getAppName();
        DefaultMatsSocketServer matsSocketServer = new DefaultMatsSocketServer(matsFactory, clusterStoreAndForward,
                replyTerminatorId, authorizationToPrincipalFunction);

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
                    .subprotocols(Collections.singletonList("matssocket"))
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
    private final Function<String, Principal> _authorizationToPrincipalFunction;

    // In-line init
    private final ConcurrentHashMap<String, MatsSocketEndpointRegistration<?, ?, ?, ?>> _matsSocketEndpointsByMatsSocketEndpointId = new ConcurrentHashMap<>();
    private final Map<String, MatsSocketSession> _activeSessionsByMatsSocketSessionId_x = new LinkedHashMap<>();

    private DefaultMatsSocketServer(MatsFactory matsFactory, ClusterStoreAndForward clusterStoreAndForward,
            String replyTerminatorId, Function<String, Principal> authorizationToPrincipalFunction) {
        _matsFactory = matsFactory;
        _clusterStoreAndForward = clusterStoreAndForward;
        _jackson = jacksonMapper();
        _replyTerminatorId = replyTerminatorId;
        _authorizationToPrincipalFunction = authorizationToPrincipalFunction;

        int corePoolSize = Math.max(5, matsFactory.getFactoryConfig().getConcurrency() * 4);
        int maximumPoolSize = Math.max(100, matsFactory.getFactoryConfig().getConcurrency() * 20);
        _messageToWebSocketForwarder = new MessageToWebSocketForwarder(this,
                clusterStoreAndForward, corePoolSize, maximumPoolSize);

        // Register our Reply-handler (common on all nodes - need forwarding to correct node)
        matsFactory.terminator(replyTerminatorId, ReplyHandleStateDto.class, MatsObject.class,
                this::mats_processMatsReply);

        // Register our Forward to WebSocket handler (common on all nodes).
        matsFactory.subscriptionTerminator(pingTerminatorIdForNode(getMyNodename()), Void.class, String.class,
                (processContext, state, matsSocketSessionId) -> mats_localSessionMessageNotify(matsSocketSessionId));
    }

    String getMyNodename() {
        return _matsFactory.getFactoryConfig().getNodename();
    }

    public MatsFactory getMatsFactory() {
        return _matsFactory;
    }

    public ClusterStoreAndForward getClusterStoreAndForward() {
        return _clusterStoreAndForward;
    }

    public ObjectMapper getJackson() {
        return _jackson;
    }

    public MessageToWebSocketForwarder getMessageToWebSocketForwarder() {
        return _messageToWebSocketForwarder;
    }

    public String getReplyTerminatorId() {
        return _replyTerminatorId;
    }

    public Function<String, Principal> getAuthorizationToPrincipalFunction() {
        return _authorizationToPrincipalFunction;
    }

    private String pingTerminatorIdForNode(String nodename) {
        // TODO: "Escape" the nodename (just in case)
        return _replyTerminatorId + '.' + nodename;
    }

    @Override
    public <I, MI, MR, R> MatsSocketEndpoint<I, MI, MR, R> matsSocketEndpoint(String matsSocketEndpointId,
            Class<I> msIncomingClass, Class<MI> matsIncomingClass, Class<MR> matsReplyClass, Class<R> msReplyClass,
            MatsSocketEndpointIncomingAuthEval<I, MI, R> incomingAuthEval) {
        MatsSocketEndpointRegistration<I, MI, MR, R> matsSocketRegistration = new MatsSocketEndpointRegistration<>(
                matsSocketEndpointId, msIncomingClass, matsIncomingClass, matsReplyClass, msReplyClass,
                incomingAuthEval);
        MatsSocketEndpointRegistration existing = _matsSocketEndpointsByMatsSocketEndpointId.putIfAbsent(
                matsSocketEndpointId, matsSocketRegistration);
        // Assert that there was no existing mapping
        if (existing != null) {
            // -> There was existing mapping - shall not happen.
            throw new IllegalStateException("Cannot register a MatsSocket onto an EndpointId which already is"
                    + " taken, existing: [" + existing._incomingAuthEval + "].");
        }
        return matsSocketRegistration;
    }

    void registerLocalMatsSocketSession(MatsSocketSession matsSocketSession) {
        synchronized (_activeSessionsByMatsSocketSessionId_x) {
            _activeSessionsByMatsSocketSessionId_x.put(matsSocketSession.getId(), matsSocketSession);
        }
    }

    Optional<MatsSocketSession> getRegisteredLocalMatsSocketSession(String matsSocketSessionId) {
        synchronized (_activeSessionsByMatsSocketSessionId_x) {
            return Optional.ofNullable(_activeSessionsByMatsSocketSessionId_x.get(matsSocketSessionId));
        }
    }

    private int numberOfRegisteredLocalMatsSocketSessions() {
        synchronized (_activeSessionsByMatsSocketSessionId_x) {
            return _activeSessionsByMatsSocketSessionId_x.size();
        }
    }

    private List<MatsSocketSession> getCurrentLocalRegisteredMatsSocketSessions() {
        synchronized (_activeSessionsByMatsSocketSessionId_x) {
            return new ArrayList<>(_activeSessionsByMatsSocketSessionId_x.values());
        }
    }

    void deregisterLocalMatsSocketSession(String matsSocketSessionId, String connectionId) {
        synchronized (_activeSessionsByMatsSocketSessionId_x) {
            MatsSocketSession matsSocketSession = _activeSessionsByMatsSocketSessionId_x.get(matsSocketSessionId);
            if (matsSocketSession != null) {
                if (matsSocketSession.getConnectionId().equals(connectionId)) {
                    _activeSessionsByMatsSocketSessionId_x.remove(matsSocketSessionId);
                }
            }
        }
    }

    @Override
    public void shutdown() {
        log.info("Asked to shut down MatsSocketServer [" + id(this)
                + "], containing [" + getCurrentLocalRegisteredMatsSocketSessions() + "] sessions.");

        // TODO: Hinder further accepts.

        _messageToWebSocketForwarder.shutdown();

        getCurrentLocalRegisteredMatsSocketSessions().forEach(s -> {
            // Close WebSocket
            s.closeWebSocket(CloseCodes.SERVICE_RESTART,
                    "From Server: Server instance is going down, please reconnect.");
            // Local deregister
            deregisterLocalMatsSocketSession(s.getId(), s.getConnectionId());
            // CSAF deregister
            try {
                _clusterStoreAndForward.deregisterSessionFromThisNode(s.getId(), s.getConnectionId());
            }
            catch (DataAccessException e) {
                log.warn("Could not deregister MatsSocketSession [" + s.getId() + "] from CSAF, ignoring.",
                        e);
            }
        });
    }

    MatsSocketEndpointRegistration<?, ?, ?, ?> getMatsSocketEndpointRegistration(String eid) {
        MatsSocketEndpointRegistration matsSocketRegistration = _matsSocketEndpointsByMatsSocketEndpointId.get(eid);
        log.info("MatsSocketRegistration for [" + eid + "]: " + matsSocketRegistration);
        if (matsSocketRegistration == null) {
            throw new IllegalArgumentException("Cannot find MatsSocketRegistration for [" + escape(eid) + "]");
        }
        return matsSocketRegistration;
    }

    static class ReplyHandleStateDto {
        private final String sid;
        private final String cid;
        private final String mseq;
        private final String ms_eid;
        private final String ms_reid;

        private final long cmcts; // Client Message Created TimeStamp (Client timestamp)
        private final long cmrts; // Client Message Received Timestamp (Server timestamp)
        private final long mmsts; // Mats Message Sent Timestamp (when the message was sent onto Mats MQ fabric, Server
                                  // timestamp)

        private final String recnn; // Received Nodeanme

        private ReplyHandleStateDto() {
            /* no-args constructor for Jackson */
            sid = null;
            cid = null;
            mseq = null;
            ms_eid = null;
            ms_reid = null;
            cmcts = 0;
            cmrts = 0;
            mmsts = 0;
            recnn = null;
        }

        ReplyHandleStateDto(String matsSocketSessionId, String matsSocketEndpointId, String replyEndpointId,
                String correlationId, String messageSequence, long clientMessageCreatedTimestamp,
                long clientMessageReceivedTimestamp, long matsMessageSentTimestamp,
                String receivedNodename) {
            sid = matsSocketSessionId;
            cid = correlationId;
            mseq = messageSequence;
            ms_eid = matsSocketEndpointId;
            ms_reid = replyEndpointId;
            cmcts = clientMessageCreatedTimestamp;
            cmrts = clientMessageReceivedTimestamp;
            mmsts = matsMessageSentTimestamp;
            recnn = receivedNodename;
        }
    }

    /**
     * A "random" number picked by basically hammering on the keyboard and then carefully manually randomize it some
     * more ;-) - used to string-replace the sending timestamp into the envelope on the WebSocket-forward side. If this
     * by sheer randomness appears in the actual payload message (as ASCII literals), then it might be accidentally
     * swapped out. If this happens, sorry - but you should <i>definitely</i> also sell your house and put all proceeds
     * into all of your country's Lotto systems.
     */
    private static final long REPLACE_VALUE_TIMESTAMP = 3_945_608_518_157_027_723L;
    static final Pattern REPLACE_VALUE_TIMESTAMP_REGEX = Pattern.compile(
            Long.toString(REPLACE_VALUE_TIMESTAMP), Pattern.LITERAL);

    /**
     * A "random" String made manually by hammering a little on the keyboard, and trying to find a very implausible
     * string. This will be string-replaced by the sending hostname on the WebSocket-forward side. Must not include
     * characters that will be JSON-encoded, as the replace will be literal.
     */
    private static final String REPLACE_VALUE_REPLY_NODENAME = "X&~est,O}@w.h£X";
    static final Pattern REPLACE_VALUE_REPLY_NODENAME_REGEX = Pattern.compile(
            REPLACE_VALUE_REPLY_NODENAME, Pattern.LITERAL);

    private void mats_processMatsReply(ProcessContext<Void> processContext,
            ReplyHandleStateDto state, MatsObject incomingMsg) {
        long matsMessageReplyReceivedTimestamp = System.currentTimeMillis();

        // Find the MatsSocketEndpoint for this reply
        MatsSocketEndpointRegistration registration = getMatsSocketEndpointRegistration(state.ms_eid);

        Object matsReply = incomingMsg.toClass(registration._matsReplyClass);

        Object msReply;
        // TODO: If replyAdapter throws, it is a REJECT
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
        // TODO: If replyAdapter throws, it is a REJECT
        msReplyEnvelope.st = "RESOLVE";
        msReplyEnvelope.eid = state.ms_reid;
        msReplyEnvelope.cid = state.cid;
        msReplyEnvelope.mseq = state.mseq;
        msReplyEnvelope.tid = processContext.getTraceId(); // TODO: Chop off last ":xyz", as that is added serverside.
        msReplyEnvelope.cmcts = state.cmcts;
        msReplyEnvelope.cmrts = state.cmrts;
        msReplyEnvelope.mmsts = state.mmsts;
        msReplyEnvelope.mmrrts = matsMessageReplyReceivedTimestamp;
        msReplyEnvelope.rmcts = REPLACE_VALUE_TIMESTAMP; // Reply Message to Client Timestamp (not yet determined)
        msReplyEnvelope.cmrnn = state.recnn; // The receiving nodename
        msReplyEnvelope.mmrrnn = getMyNodename(); // The Mats-receiving nodename (this processing)
        msReplyEnvelope.rmcnn = REPLACE_VALUE_REPLY_NODENAME; // The replying nodename (not yet determined)
        msReplyEnvelope.msg = msReply; // This object will be serialized.

        // Serialize and store the message for forward ("StoreAndForward")
        String serializedEnvelope = serializeEnvelope(msReplyEnvelope);
        Optional<CurrentNode> nodeNameHoldingWebSocket;
        try {
            nodeNameHoldingWebSocket = _clusterStoreAndForward.storeMessageForSession(
                    state.sid, processContext.getTraceId(), msReplyEnvelope.t, serializedEnvelope);
        }
        catch (DataAccessException e) {
            // TODO: Fix
            throw new AssertionError("Damn", e);
        }

        // ?: Check if WE have the session locally
        Optional<MatsSocketSession> localMatsSocketSession = getRegisteredLocalMatsSocketSession(state.sid);
        if (localMatsSocketSession.isPresent()) {
            // -> Yes, evidently we have it! Do local forward.
            _messageToWebSocketForwarder.notifyMessageFor(localMatsSocketSession.get());
            return;
        }

        // ?: If we do have a nodename, ping it about new message
        if (nodeNameHoldingWebSocket.isPresent()) {
            // -> Yes we got a nodename, ping it.
            processContext.initiate(init -> {
                init.traceId("PingWebSocketHolder")
                        .from(_replyTerminatorId)
                        .to(pingTerminatorIdForNode(nodeNameHoldingWebSocket.get().getNodename()))
                        .publish(state.sid);
            });
        }
    }

    private void mats_localSessionMessageNotify(String matsSocketSessionId) {
        Optional<MatsSocketSession> localMatsSocketSession = getRegisteredLocalMatsSocketSession(matsSocketSessionId);
        // ?: If this Session does not exist at this node, we cannot deliver.
        if (!localMatsSocketSession.isPresent()) {
            // -> Someone must have found that we had it, but this must have asynchronously have been deregistered.
            // Do forward of notification - it will handle if we're the node being registered.
            notifyHomeNodeIfAnyAboutNewMessage(matsSocketSessionId);
        }

        // ----- We have determined that MatsSocketSession has home here

        _messageToWebSocketForwarder.notifyMessageFor(localMatsSocketSession.get());
    }

    void notifyHomeNodeIfAnyAboutNewMessage(String matsSocketSessionId) {
        Optional<CurrentNode> currentNode;
        try {
            // Find if the session resides on a different node
            currentNode = _clusterStoreAndForward.getCurrentRegisteredNodeForSession(matsSocketSessionId);

            // ?: Did we get a node?
            if (!currentNode.isPresent()) {
                // -> No, so nothing to do - MatsSocket will get messages when he reconnect.
                log.info("MatsSocketSession [" + matsSocketSessionId + "] is not present on any node. Ignoring,"
                        + " hoping that client will come back and get his messages later.");
                return;
            }

            // ?: Was the node /this/ node?
            if (currentNode.get().getNodename().equalsIgnoreCase(getMyNodename())) {
                // -> Oops, yes.
                // Find the local session.
                Optional<MatsSocketSession> localSession = getRegisteredLocalMatsSocketSession(matsSocketSessionId);
                // ?: Do we have this session locally?!
                if (!localSession.isPresent()) {
                    // -> No, we do NOT have this session locally!
                    // NOTICE: This could e.g. happen if DB down when trying to deregister the MatsSocketSession.
                    log.info("MatsSocketSession [" + matsSocketSessionId + "] is said to live on this node, but we do"
                            + " not have it. Tell the CSAF this, and ignore, hoping that client will come back and get"
                            + " his messages later.");
                    // Fix this wrongness: Tell CSAF that we do not have this session!
                    _clusterStoreAndForward.deregisterSessionFromThisNode(matsSocketSessionId, currentNode.get()
                            .getConnectionId());
                    // No can do.
                    return;
                }
            }
        }
        catch (DataAccessException e) {
            log.warn("Got '" + e.getClass().getSimpleName() + "' when trying to find node home for"
                    + " MatsSocketSession [" + matsSocketSessionId + "] using '"
                    + _clusterStoreAndForward.getClass().getSimpleName() + "'. Bailing out, hoping for"
                    + " self-healer process to figure it out.", e);
            return;
        }

        // Send message to home for MatsSocketSession (it /might/ be us if massive async, but that'll be handled.)
        _matsFactory.getDefaultInitiator().initiateUnchecked(init -> {
            init.traceId("MatsSystemOutgoingMessageNotify[ms_sid:" + matsSocketSessionId + "]" + rnd(5))
                    .from("MatsSocketSystem.notifyNodeAboutMessage")
                    .to(pingTerminatorIdForNode(currentNode.get().getNodename()))
                    .publish(matsSocketSessionId);
        });
    }

    void serializeAndSendSingleEnvelope(Session session, MatsSocketEnvelopeDto msReplyEnvelope) {
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
        // JSONify the MatsSocket Reply.
        try {
            return _jackson.writeValueAsString(msReplyEnvelope);
        }
        catch (JsonProcessingException e) {
            throw new AssertionError("Huh, couldn't serialize message?!");
        }
    }

    static class MatsSocketEndpointRegistration<I, MI, MR, R> implements MatsSocketEndpoint<I, MI, MR, R> {
        private final String _matsSocketEndpointId;
        private final Class<I> _msIncomingClass;
        private final Class<MI> _matsIncomingClass;
        private final Class<MR> _matsReplyClass;
        private final Class<R> _msReplyClass;
        private final MatsSocketEndpointIncomingAuthEval<I, MI, R> _incomingAuthEval;

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

        String getMatsSocketEndpointId() {
            return _matsSocketEndpointId;
        }

        public Class<I> getMsIncomingClass() {
            return _msIncomingClass;
        }

        MatsSocketEndpointIncomingAuthEval<I, MI, R> getIncomingAuthEval() {
            return _incomingAuthEval;
        }

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
    public static class MatsWebSocketInstance extends Endpoint {
        private final DefaultMatsSocketServer _matsSocketServer;
        private final ObjectMapper _jackson;

        private MatsSocketSession _matsSocketSession;

        public MatsWebSocketInstance(DefaultMatsSocketServer matsSocketServer) {
            log.info("Created MatsWebSocketEndpointInstance: " + id(this));
            _matsSocketServer = matsSocketServer;
            _jackson = matsSocketServer._jackson;
        }

        public DefaultMatsSocketServer getMatsSocketServer() {
            return _matsSocketServer;
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
            _matsSocketSession = new MatsSocketSession(_matsSocketServer, session);
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
            // ?: Have we gotten MatsSocketSession yet? (In case "onOpen" has not been invoked yet. Can it happen?!).
            if (_matsSocketSession != null) {
                // -> Yes, so remove us from local and global views
                // Deregister session locally
                _matsSocketServer.deregisterLocalMatsSocketSession(_matsSocketSession.getId(),
                        _matsSocketSession.getConnectionId());
                // Deregister session from the ClusterStoreAndForward
                try {
                    _matsSocketServer._clusterStoreAndForward.deregisterSessionFromThisNode(
                            _matsSocketSession.getId(), _matsSocketSession.getConnectionId());
                }
                catch (DataAccessException e) {
                    // TODO: Fix
                    throw new AssertionError("Damn", e);
                }
            }
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

    static String id(Object x) {
        return x.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(x));
    }

    static String escape(String string) {
        // TODO: Implement HTML escaping (No messages from us should not go through JSONifying already).
        return string;
    }
}
