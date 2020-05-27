package com.stolsvik.mats.websocket.impl;

import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.PUB;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.REJECT;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.REQUEST;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.RESOLVE;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.SEND;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCode;
import javax.websocket.DeploymentException;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.Extension;
import javax.websocket.HandshakeResponse;
import javax.websocket.Session;
import javax.websocket.server.HandshakeRequest;
import javax.websocket.server.ServerContainer;
import javax.websocket.server.ServerEndpointConfig;
import javax.websocket.server.ServerEndpointConfig.Builder;
import javax.websocket.server.ServerEndpointConfig.Configurator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.DetachedProcessContext;
import com.stolsvik.mats.MatsEndpoint.MatsObject;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsInitiator.MatsBackendException;
import com.stolsvik.mats.MatsInitiator.MatsBackendRuntimeException;
import com.stolsvik.mats.MatsInitiator.MatsMessageSendException;
import com.stolsvik.mats.MatsInitiator.MatsMessageSendRuntimeException;
import com.stolsvik.mats.websocket.AuthenticationPlugin;
import com.stolsvik.mats.websocket.AuthenticationPlugin.DebugOption;
import com.stolsvik.mats.websocket.AuthenticationPlugin.SessionAuthenticator;
import com.stolsvik.mats.websocket.ClusterStoreAndForward;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.CurrentNode;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.DataAccessException;
import com.stolsvik.mats.websocket.MatsSocketServer;
import com.stolsvik.mats.websocket.MatsSocketServer.ActiveMatsSocketSession.MatsSocketSessionState;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeDto.DebugDto;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto.IncomingResolution;
import com.stolsvik.mats.websocket.MatsSocketServer.SessionRemovedEvent.SessionRemovedEventType;

/**
 * @author Endre Stølsvik 2019-11-28 12:17 - http://stolsvik.com/, endre@stolsvik.com
 */
public class DefaultMatsSocketServer implements MatsSocketServer, MatsSocketStatics {
    private static final Logger log = LoggerFactory.getLogger(DefaultMatsSocketServer.class);

    private static final String MATS_EP_PREFIX = "MatsSocket";

    private static final String MATS_EP_POSTFIX_REPLY_HANDLER = "replyHandler";

    private static final String MATS_SUBSTERM_POSTFIX_PUBLISH = "publish";

    private static final String MATS_SUBSTERM_MIDFIX_NODECONTROL = "nodeControl";

    private static final String MATS_EP_POSTFIX_MATS_PING = "matsPing";

    private static final JavaType TYPE_LIST_OF_MSG_WITH_META = TypeFactory.defaultInstance().constructType(
            new TypeReference<List<MatsSocketEnvelopeWithMetaDto>>() {
            });

    private static final JavaType TYPE_LIST_OF_MSG_WITHOUT_META = TypeFactory.defaultInstance().constructType(
            new TypeReference<List<MatsSocketEnvelopeDto>>() {
            });

    /**
     * Variant of the
     * {@link #createMatsSocketServer(ServerContainer, MatsFactory, ClusterStoreAndForward, AuthenticationPlugin, String, String)
     * full method} that uses the {@link FactoryConfig#getAppName() appName} that the MatsFactory is configured with as
     * the 'instanceName' parameter.
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
     * @param authenticationPlugin
     *            the piece of code that turns an Authorization String into a Principal. Must be pretty fast, as it is
     *            invoked synchronously - keep any IPC fast, otherwise all your threads of the container might be used
     *            up. If the function throws or returns null, authorization did not go through.
     * @param websocketPath
     *            The path onto which the WebSocket Server Endpoint will be mounted. Suggestion: "/matssocket". If you
     *            need multiple {@link MatsSocketServer}s, e.g. because you need two types of authentication, they need
     *            to be mounted on different paths.
     *
     * @return a MatsSocketServer instance, now hooked into both the WebSocket {@link ServerContainer} and the
     *         {@link MatsFactory}.
     */
    public static MatsSocketServer createMatsSocketServer(
            ServerContainer serverContainer,
            MatsFactory matsFactory,
            ClusterStoreAndForward clusterStoreAndForward,
            AuthenticationPlugin authenticationPlugin,
            String websocketPath) {
        String instanceName = matsFactory.getFactoryConfig().getAppName();
        return createMatsSocketServer(serverContainer, matsFactory, clusterStoreAndForward, authenticationPlugin,
                instanceName, websocketPath);
    }

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
     * @param authenticationPlugin
     *            the piece of code that turns an Authorization String into a Principal. Must be pretty fast, as it is
     *            invoked synchronously - keep any IPC fast, otherwise all your threads of the container might be used
     *            up. If the function throws or returns null, authorization did not go through.
     * @param instanceName
     *            a unique name of this MatsSocketServer setup, at least within the MQ system the MatsFactory is
     *            connected to, as it is used to postfix/uniquify the endpoints that the MatsSocketServer creates on the
     *            MatsFactory. To illustrate: The variant of this factory method that does not take 'instanceName' uses
     *            the {@link FactoryConfig#getAppName() appName} that the MatsFactory is configured with.
     * @param websocketPath
     *            The path onto which the WebSocket Server Endpoint will be mounted. Suggestion: "/matssocket". If you
     *            need multiple {@link MatsSocketServer}s, e.g. because you need two types of authentication, they need
     *            to be mounted on different paths.
     *
     * @return a MatsSocketServer instance, now hooked into both the WebSocket {@link ServerContainer} and the
     *         {@link MatsFactory}.
     */
    public static MatsSocketServer createMatsSocketServer(
            ServerContainer serverContainer,
            MatsFactory matsFactory,
            ClusterStoreAndForward clusterStoreAndForward,
            AuthenticationPlugin authenticationPlugin,
            String instanceName,
            String websocketPath) {
        // Boot ClusterStoreAndForward
        clusterStoreAndForward.boot();

        // Create the MatsSocketServer..
        DefaultMatsSocketServer matsSocketServer = new DefaultMatsSocketServer(matsFactory, clusterStoreAndForward,
                instanceName, authenticationPlugin);

        log.info("Registering MatsSockets' sole WebSocket endpoint.");
        Configurator configurator = new Configurator() {

            ThreadLocal<HandshakeRequestResponse> _threadLocal_HandShake = new ThreadLocal<>();
            ThreadLocal<SessionAuthenticator> _threadLocal_SessionAuthenticator = new ThreadLocal<>();

            private SessionAuthenticator getSessionAuthenticator() {
                SessionAuthenticator sessionAuthenticator = _threadLocal_SessionAuthenticator.get();
                if (sessionAuthenticator == null) {
                    sessionAuthenticator = matsSocketServer.getAuthenticationPlugin().newSessionAuthenticator();
                    _threadLocal_SessionAuthenticator.set(sessionAuthenticator);
                }
                return sessionAuthenticator;
            }

            @Override
            public boolean checkOrigin(String originHeaderValue) {
                log.info("WebSocket connection!");
                boolean ok = getSessionAuthenticator().checkOrigin(originHeaderValue);
                log.info(" \\- checkOrigin(" + originHeaderValue + "). Asked SessionAuthenticator, returned: "
                        + (ok ? "OK" : "NOT OK!"));
                return ok;
            }

            @Override
            public String getNegotiatedSubprotocol(List<String> supported, List<String> requested) {
                log.info(" \\- getNegotiatedSubprotocol: supported by MatsSocket impl:" + supported
                        + ", requested by client:" + requested);
                return super.getNegotiatedSubprotocol(supported, requested);
            }

            @Override
            public List<Extension> getNegotiatedExtensions(List<Extension> installed, List<Extension> requested) {
                log.info(" \\- getNegotiatedExtensions: installed in server:" + installed + ", requested by client:"
                        + requested);
                return super.getNegotiatedExtensions(installed, requested);
            }

            @Override
            public void modifyHandshake(ServerEndpointConfig sec, HandshakeRequest request,
                    HandshakeResponse response) {
                _threadLocal_HandShake.set(new HandshakeRequestResponse(request, response));
                boolean ok = getSessionAuthenticator().checkHandshake(sec, request, response);
                log.info(" \\- modifyHandshake(config, request, response). Asked SessionAuthenticator, returned: "
                        + (ok ? "OK" : "NOT OK!"));
                if (!ok) {
                    throw new IllegalStateException("SessionAuthenticator did nok like the"
                            + " WebSocket HTTP Handshake Request.");
                }
                super.modifyHandshake(sec, request, response);
            }

            @Override
            @SuppressWarnings("unchecked") // The cast to (T) is not dodgy.
            public <T> T getEndpointInstance(Class<T> endpointClass) {
                if (endpointClass != MatsWebSocketInstance.class) {
                    throw new AssertionError("Cannot create Endpoints of type [" + endpointClass.getName()
                            + "]");
                }
                log.info(" \\- Instantiating a " + MatsWebSocketInstance.class.getSimpleName() + "!");
                HandshakeRequestResponse handshakeRequestResponse = _threadLocal_HandShake.get();
                _threadLocal_HandShake.remove();
                return (T) new MatsWebSocketInstance(matsSocketServer, getSessionAuthenticator(),
                        handshakeRequestResponse);
            }
        };
        try {
            serverContainer.addEndpoint(Builder.create(MatsWebSocketInstance.class, websocketPath)
                    .subprotocols(Collections.singletonList("matssocket"))
                    .configurator(configurator)
                    .build());
        }
        catch (DeploymentException e) {
            throw new AssertionError("Could not register MatsSocket endpoint", e);
        }
        return matsSocketServer;
    }

    private static class HandshakeRequestResponse {
        private final HandshakeRequest _handshakeRequest;
        private final HandshakeResponse _handshakeResponse;

        public HandshakeRequestResponse(HandshakeRequest handshakeRequest, HandshakeResponse handshakeResponse) {
            _handshakeRequest = handshakeRequest;
            _handshakeResponse = handshakeResponse;
        }
    }

    // Constructor init
    private final MatsFactory _matsFactory;
    private final String _instanceName;
    private final ClusterStoreAndForward _clusterStoreAndForward;
    private final ObjectMapper _jackson;
    private final ObjectReader _envelopeObjectReader;
    private final ObjectWriter _envelopeObjectWriter;
    private final ObjectReader _envelopeListObjectReader;
    private final ObjectWriter _envelopeListObjectWriter;
    private final String _terminatorId_ReplyHandler;
    private final String _subscriptionTerminatorId_Publish;
    private final String _subscriptionTerminatorId_NodeControl_NodePrefix;
    private final String _endpointId_MatsPing;
    private final IncomingSrrMsgHandler _incomingSrrMsgHandler;
    private final WebSocketOutboxForwarder _webSocketOutboxForwarder;
    private final WebSocketOutgoingAcks _webSocketOutgoingAcks;
    private final LivelinessAndTimeoutAndScavenger _casfUpdateAndTimeouter;
    private final AuthenticationPlugin _authenticationPlugin;
    private final String _serverId;

    // In-line init
    private final ConcurrentHashMap<String, MatsSocketEndpointRegistration<?, ?, ?>> _matsSocketEndpointsByMatsSocketEndpointId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MatsSocketSessionAndMessageHandler> _activeSessionsByMatsSocketSessionId = new ConcurrentHashMap<>();

    private DefaultMatsSocketServer(MatsFactory matsFactory, ClusterStoreAndForward clusterStoreAndForward,
            String instanceName, AuthenticationPlugin authenticationPlugin) {
        _matsFactory = matsFactory;
        _instanceName = instanceName;

        // .. must first have set MatsFactory
        _serverId = instanceName + '@' + getMyNodename() + '@' + Integer.toHexString(System.identityHashCode(this));

        _clusterStoreAndForward = clusterStoreAndForward;
        _jackson = jacksonMapper();
        _authenticationPlugin = authenticationPlugin;
        _envelopeObjectReader = _jackson.readerFor(MatsSocketEnvelopeWithMetaDto.class);
        _envelopeObjectWriter = _jackson.writerFor(MatsSocketEnvelopeDto.class);
        _envelopeListObjectReader = _jackson.readerFor(TYPE_LIST_OF_MSG_WITH_META);
        _envelopeListObjectWriter = _jackson.writerFor(TYPE_LIST_OF_MSG_WITHOUT_META);
        // TODO: "Escape" the instanceName.
        _terminatorId_ReplyHandler = MATS_EP_PREFIX + '.' + instanceName + '.' + MATS_EP_POSTFIX_REPLY_HANDLER;
        // TODO: "Escape" the instanceName.
        _subscriptionTerminatorId_Publish = MATS_EP_PREFIX + '.' + instanceName + '.' + MATS_SUBSTERM_POSTFIX_PUBLISH;
        // TODO: "Escape" the instanceName.
        _subscriptionTerminatorId_NodeControl_NodePrefix = MATS_EP_PREFIX + '.' + instanceName + '.'
                + MATS_SUBSTERM_MIDFIX_NODECONTROL;
        // TODO: "Escape" the instanceName.
        _endpointId_MatsPing = MATS_EP_PREFIX + '.' + instanceName + '.' + MATS_EP_POSTFIX_MATS_PING;

        // :: Create active (using threads) Message Forwarder (employs the out-side of the WebSocket).
        int forwarder_corePoolSize = Math.max(MIN_FORWARDER_CORE_POOL_SIZE,
                matsFactory.getFactoryConfig().getConcurrency() * 4);
        int forwarder_maxPoolSize = Math.max(MIN_FORWARDER_MAX_POOL_SIZE,
                matsFactory.getFactoryConfig().getConcurrency() * 20);
        _webSocketOutboxForwarder = new WebSocketOutboxForwarder(this,
                clusterStoreAndForward, forwarder_corePoolSize, forwarder_maxPoolSize);

        _webSocketOutgoingAcks = new WebSocketOutgoingAcks(this, forwarder_corePoolSize, forwarder_maxPoolSize);

        _incomingSrrMsgHandler = new IncomingSrrMsgHandler(this,
                forwarder_corePoolSize, forwarder_maxPoolSize);

        // :: Create active (using thread) CSAF Liveliness Updater and Timeouter
        _casfUpdateAndTimeouter = new LivelinessAndTimeoutAndScavenger(this,
                _clusterStoreAndForward,
                MILLIS_BETWEEN_LIVELINESS_UPDATE_RUN,
                MILLIS_BETWEEN_SESSION_TIMEOUT_RUN,
                MILLIS_BETWEEN_SCAVENGE_SESSION_REMNANTS_RUN,
                MILLIS_SESSION_TIMEOUT_SUPPLIER);

        // MATS: Register our Mats Reply handler Terminator (common on all nodes, note: QUEUE-based!).
        // (Note that the reply often comes in on wrong note, in which case we forward it using NodeControl.)
        MatsEndpoint<Void, ReplyHandleStateDto> replyHandler = matsFactory.terminator(_terminatorId_ReplyHandler,
                ReplyHandleStateDto.class, MatsObject.class, epConfig -> {
                    int currentConcurrency = epConfig.getConcurrency();
                    // TODO: constant
                    if (currentConcurrency < 8) {
                        epConfig.setConcurrency(8);
                    }
                }, MatsFactory.NO_CONFIG, this::mats_replyHandler);

        // MATS: Register Publish subscriptionTerminator (common on all nodes, note: TOPIC-based!)
        matsFactory.subscriptionTerminator(nodeSubscriptionTerminatorId_NodeControl_ForNode(getMyNodename()),
                NodeControlStateDto.class, MatsObject.class, this::mats_nodeControl);

        // MATS: Register NodeControl subscriptionTerminator (node-specific)
        matsFactory.subscriptionTerminator(_subscriptionTerminatorId_Publish,
                Void.TYPE, PublishedMessageDto.class, this::mats_publish);

        // MATS: Register handler for MatsSocketEndpoint "MatsSocket.matsPing"
        matsFactory.single(_endpointId_MatsPing, MatsPingPongDto.class, MatsPingPongDto.class, (ctx, ping) -> {
            // Directly return the Pong
            return new MatsPingPongDto(ping.payload, ping.number != null ? ping.number * Math.PI : null);
        });

        // MatsSocket: "MatsSocket.matsPing". Need to register directly, since API method won't allow this prefix.
        // .. create it
        MatsSocketEndpointRegistration<MatsPingPongDto, MatsPingPongDto, MatsPingPongDto> pingReg = new MatsSocketEndpointRegistration<>(
                "MatsSocket.matsPing", MatsPingPongDto.class, MatsPingPongDto.class, MatsPingPongDto.class,
                (ctx, principal, ping) -> ctx.forwardCustom(ping, init -> init.interactive().to(_endpointId_MatsPing)),
                null);
        // .. register it.
        _matsSocketEndpointsByMatsSocketEndpointId.putIfAbsent(pingReg.getMatsSocketEndpointId(), pingReg);
    }

    /**
     * TODO: Consider putting this in the API. But as of 2020-05-25, I've decided to hold it off a bit.
     */
    static class MatsPingPongDto {
        private final String payload;
        private final Double number;

        public MatsPingPongDto() {
            this.payload = null;
            this.number = null;
        }

        public MatsPingPongDto(String payload, Double number) {
            this.payload = payload;
            this.number = number;
        }
    }

    private volatile boolean _stopped = false;

    String getMyNodename() {
        return _matsFactory.getFactoryConfig().getNodename();
    }

    MatsFactory getMatsFactory() {
        return _matsFactory;
    }

    ClusterStoreAndForward getClusterStoreAndForward() {
        return _clusterStoreAndForward;
    }

    ObjectMapper getJackson() {
        return _jackson;
    }

    ObjectReader getEnvelopeObjectReader() {
        return _envelopeObjectReader;
    }

    ObjectWriter getEnvelopeObjectWriter() {
        return _envelopeObjectWriter;
    }

    ObjectReader getEnvelopeListObjectReader() {
        return _envelopeListObjectReader;
    }

    ObjectWriter getEnvelopeListObjectWriter() {
        return _envelopeListObjectWriter;
    }

    WebSocketOutboxForwarder getWebSocketOutboxForwarder() {
        return _webSocketOutboxForwarder;
    }

    public WebSocketOutgoingAcks getWebSocketOutgoingAcks() {
        return _webSocketOutgoingAcks;
    }

    public IncomingSrrMsgHandler getIncomingSrrMsgHandler() {
        return _incomingSrrMsgHandler;
    }

    String getReplyTerminatorId() {
        return _terminatorId_ReplyHandler;
    }

    AuthenticationPlugin getAuthenticationPlugin() {
        return _authenticationPlugin;
    }

    String nodeSubscriptionTerminatorId_NodeControl_ForNode(String nodename) {
        // TODO: "Escape" the nodename (just in case)
        return _subscriptionTerminatorId_NodeControl_NodePrefix + '.' + nodename;
    }

    /**
     * Purely for logging.
     */
    String serverId() {
        return _serverId;
    }

    @Override
    public <I, MR, R> MatsSocketEndpoint<I, MR, R> matsSocketEndpoint(String matsSocketEndpointId,
            Class<I> incomingClass, Class<MR> matsReplyClass, Class<R> msReplyClass,
            IncomingAuthorizationAndAdapter<I, MR, R> incomingAuthEval, ReplyAdapter<I, MR, R> replyAdapter) {

        // :: Do some asserts on the parameters - some combos do not make sense.
        // ?: With no ReplyAdapter, specifying Object from Mats and MatsSocket does not work
        if (((matsReplyClass == Object.class) || (matsReplyClass == MatsObject.class))
                && (msReplyClass == Object.class) && (replyAdapter == null)) {
            throw new IllegalArgumentException("Having no ReplyAdapter and at the same time specifying [Mats]Object"
                    + " as both matsReplyClass and replyClass makes very little sense due to how MatsSocket interacts"
                    + " with Mats - you will have to consider another approach to accomplish what you want. Read the"
                    + " JavaDoc of the different MatsSocket endpoint creation methods carefully,"
                    + " including any 'Notes'! EndpointId:[" + matsSocketEndpointId + "]");
        }
        // Note: Cannot check for the situation where user says he won't forward to Mats, but do it anyway..

        // :: Not allowed to make MatsSocket endpoints which starts with "MatsSocket" or "ms.".
        if (matsSocketEndpointId.toLowerCase().startsWith("ms.")
                || matsSocketEndpointId.toLowerCase().startsWith("matssocket")) {
            throw new IllegalStateException("EndpointIds starting with 'MatsSocket' or 'ms.' are reserved."
                    + " EndpointId:[" + matsSocketEndpointId + "].");
        }

        // :: Create it
        MatsSocketEndpointRegistration<I, MR, R> matsSocketRegistration = new MatsSocketEndpointRegistration<>(
                matsSocketEndpointId, incomingClass, matsReplyClass, msReplyClass,
                incomingAuthEval, replyAdapter);
        // Register it.
        MatsSocketEndpointRegistration<?, ?, ?> existing = _matsSocketEndpointsByMatsSocketEndpointId.putIfAbsent(
                matsSocketEndpointId, matsSocketRegistration);
        // Assert that there was no existing mapping
        if (existing != null) {
            // -> There was existing mapping - shall not happen.
            throw new IllegalStateException("Cannot register a MatsSocket onto an EndpointId [" + matsSocketEndpointId
                    + "] which already is taken, existing registration: [" + existing
                    + "]. NOTE: The Cause of this exception is the DebugStacktrace of the existing registration!",
                    existing._registrationPoint);
        }
        return matsSocketRegistration;
    }

    @Override
    public void send(String sessionId, String traceId, String clientTerminatorId, Object messageDto)
            throws DataStoreException {
        long now = System.currentTimeMillis();
        // Create ServerMessageId
        String serverMessageId = serverMessageId();
        // Create DebugDto - must do this "eagerly", as we do not know what the client actually wants.
        DebugDto debug = new DebugDto();
        debug.smcts = now;
        debug.smcnn = getMyNodename();

        // Create Envelope
        MatsSocketEnvelopeDto envelope = new MatsSocketEnvelopeDto();
        envelope.t = SEND;
        envelope.eid = clientTerminatorId;
        envelope.smid = serverMessageId;
        envelope.tid = traceId;
        envelope.debug = debug;

        // Serialize and store the envelope for forward ("StoreAndForward")
        String serializedEnvelope = serializeEnvelope(envelope);
        // Serialize the actual message
        String serializedMessage = serializeMessageObject(messageDto);

        Optional<CurrentNode> currentNode;
        try {
            currentNode = _clusterStoreAndForward.storeMessageInOutbox(
                    sessionId, serverMessageId, null, traceId, envelope.t, now, serializedEnvelope, serializedMessage,
                    null);
        }
        catch (DataAccessException e) {
            throw new DataStoreException("Could not store outgoing 'send' message in data store.", e);
        }

        /*
         * NOTICE! It is tempting to do a shortcut if the MatsSocketSession's WebSocket resides on the "this" node, i.e.
         * the one currently running this code. However, this posed a problem when this method was invoked from a Mats
         * Stage after the change of CSAF_SQL where it utilizes the same SQL Connection that the MatsTransactionManager
         * has set up: We then get the situation where the DB with the new message has not yet been committed, but the
         * forwarder.newMessagesInCsafNotify(..) is invoked locally and then immediately performs a SELECT - not finding
         * the message. Thus, if we just change to always do a "remote ping" instead, we circumvent the problem by
         * relying on the transactional demarcation of the MatsStage: The outgoing "remote ping" Mats message won't be
         * committed before the DB has been committed.
         */
        pingRemoteNodeAfterMessageStored(sessionId, currentNode, "MatsSocketServer.send");
    }

    @Override
    public void request(String sessionId, String traceId, String clientEndpointId, Object requestDto,
            String replyToMatsSocketTerminatorId, String correlationString, byte[] correlationBinary)
            throws DataStoreException {
        long now = System.currentTimeMillis();
        // Create ServerMessageId
        String serverMessageId = serverMessageId();
        // Create DebugDto - must do this "eagerly", as we do not know what the client actually wants.
        DebugDto debug = new DebugDto();
        debug.smcts = now;
        debug.smcnn = getMyNodename();

        // Create Envelope
        MatsSocketEnvelopeDto envelope = new MatsSocketEnvelopeDto();
        envelope.t = REQUEST;
        envelope.eid = clientEndpointId;
        envelope.smid = serverMessageId;
        envelope.tid = traceId;
        envelope.debug = debug;

        // Serialize and store the envelope for forward ("StoreAndForward")
        String serializedEnvelope = serializeEnvelope(envelope);
        // Serialize the actual message
        String serializedMessage = serializeMessageObject(requestDto);

        Optional<CurrentNode> currentNode;
        try {
            // Store Correlation information
            _clusterStoreAndForward.storeRequestCorrelation(sessionId, serverMessageId, now,
                    replyToMatsSocketTerminatorId, correlationString, correlationBinary);
            // Stick the message in Outbox
            currentNode = _clusterStoreAndForward.storeMessageInOutbox(
                    sessionId, serverMessageId, null, traceId, envelope.t, now, serializedEnvelope, serializedMessage,
                    null);
        }
        catch (DataAccessException e) {
            throw new DataStoreException("Could not store outgoing 'request' message in data store.", e);
        }

        /*
         * NOTICE! It is tempting to do a shortcut if the MatsSocketSession's WebSocket resides on the "this" node, i.e.
         * the one currently running this code. However, this posed a problem when this method was invoked from a Mats
         * Stage after the change of CSAF_SQL where it utilizes the same SQL Connection that the MatsTransactionManager
         * has set up: We then get the situation where the DB with the new message has not yet been committed, but the
         * forwarder.newMessagesInCsafNotify(..) is invoked locally and then immediately performs a SELECT - not finding
         * the message. Thus, if we just change to always do a "remote ping" instead, we circumvent the problem by
         * relying on the transactional demarcation of the MatsStage: The outgoing "remote ping" Mats message won't be
         * committed before the DB has been committed.
         */
        pingRemoteNodeAfterMessageStored(sessionId, currentNode, "MatsSocketServer.request");
    }

    ConcurrentHashMap<String, ConcurrentHashMap<String, MatsSocketSessionAndMessageHandler>> _topicSubscriptions = new ConcurrentHashMap<>();

    void registerMatsSocketSessionWithTopic(String topicId, MatsSocketSessionAndMessageHandler session) {
        _topicSubscriptions.compute(topicId, (x, currentSubs) -> {
            // ?: Do we already have a Map over current subscriptions?
            if (currentSubs == null) {
                // -> Yes, so create it now.
                currentSubs = new ConcurrentHashMap<>();
            }
            // E-> No, so just add us to the existing
            currentSubs.put(session.getConnectionId(), session);
            return currentSubs;
        });
    }

    void deregisterMatsSocketSessionFromTopic(String topicId, String connectionId) {
        _topicSubscriptions.computeIfPresent(topicId, (x, currentSubs) -> {
            currentSubs.remove(connectionId);
            return currentSubs.isEmpty() ? null : currentSubs;
        });
    }

    private static class PublishedMessageDto {
        private final String topicId;
        private final String env;
        private final String msg;

        public PublishedMessageDto() {
            topicId = null;
            env = null;
            msg = null;
            /* for Jackson */
        }

        public PublishedMessageDto(String topicId, String envelope, String message) {
            this.topicId = topicId;
            this.env = envelope;
            this.msg = message;
        }
    }

    @Override
    public void publish(String traceId, String topicId, Object messageDto) throws MatsBackendRuntimeException {
        // Create DebugDto - must do this "eagerly", as we do not know what the client actually wants.
        DebugDto debug = new DebugDto();
        debug.smcts = System.currentTimeMillis();
        debug.smcnn = getMyNodename();

        // Create Envelope
        MatsSocketEnvelopeWithMetaDto publishEnvelope = new MatsSocketEnvelopeWithMetaDto();
        publishEnvelope.t = PUB;
        publishEnvelope.eid = topicId;
        publishEnvelope.smid = rndJsonId(10);
        publishEnvelope.tid = traceId;

        // Serialize and store the envelope for topic forward
        String serializedEnvelope = serializeEnvelope(publishEnvelope);
        // Serialize the actual message
        String serializedMessage = serializeMessageObject(messageDto);

        try {
            _matsFactory.getDefaultInitiator().initiateUnchecked(init -> init
                    .from("MatsSocketServer.publish")
                    .traceId("PublishedMessageForTopic[" + topicId + "]" + rnd(5))
                    .to(_subscriptionTerminatorId_Publish)
                    .publish(new PublishedMessageDto(topicId, serializedEnvelope, serializedMessage)));
        }
        catch (MatsMessageSendRuntimeException e) {
            // This should SERIOUSLY not happen, as the initiate-lambda does NOT interact with the database.
            throw new AssertionError("MatsMessageSendException should not occur, since the publish's"
                    + " InitiateLambda does not interact with external datasource.", e);
        }
    }

    @Override
    public SortedMap<String, MatsSocketEndpoint<?, ?, ?>> getMatsSocketEndpoints() {
        return new TreeMap<>(_matsSocketEndpointsByMatsSocketEndpointId);
    }

    @Override
    public List<MatsSocketSessionDto> getMatsSocketSessions(boolean onlyActive, String userId, String appName,
            String appVersionAtOrAbove) throws DataStoreException {
        try {
            return _clusterStoreAndForward.getSessions(onlyActive, userId, appName, appVersionAtOrAbove);
        }
        catch (DataAccessException e) {
            throw new DataStoreException("Could not get Sessions from data store.", e);
        }
    }

    @Override
    public int getMatsSocketSessionsCount(boolean onlyActive, String userId, String appName,
            String appVersionAtOrAbove) {
        try {
            return _clusterStoreAndForward.getSessionsCount(onlyActive, userId, appName, appVersionAtOrAbove);
        }
        catch (DataAccessException e) {
            throw new DataStoreException("Could not get count of Sessions from data store.", e);
        }
    }

    @Override
    public SortedMap<String, ActiveMatsSocketSessionDto> getActiveMatsSocketSessions() {
        SortedMap<String, ActiveMatsSocketSessionDto> ret = new TreeMap<>();
        for (LiveMatsSocketSession liveSession : _activeSessionsByMatsSocketSessionId.values()) {
            // "Copy it out"
            ActiveMatsSocketSessionDto activeSession = liveSession.toActiveMatsSocketSession();
            // ?: Check again that the LiveSession is still SESSION_ESTABLISHED
            if (liveSession.getState() != MatsSocketSessionState.SESSION_ESTABLISHED) {
                // -> No, it changed during copying, so then we drop this.
                continue;
            }
            // Add to result-Map
            ret.put(activeSession.getMatsSocketSessionId(), activeSession);
        }
        return ret;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map<String, LiveMatsSocketSession> getLiveMatsSocketSessions() {
        return (Map) Collections.unmodifiableMap(_activeSessionsByMatsSocketSessionId);
    }

    @Override
    public void closeSession(String matsSocketSessionId, String reason) {
        try { // finally: MDC.remove()
            MDC.putCloseable(MDC_SESSION_ID, matsSocketSessionId);
            log.info("server.closeSession(..): Got instructed to Close MatsSocketSessionId: ["
                    + matsSocketSessionId + "], reason: [" + reason + "]");
            if (matsSocketSessionId == null) {
                throw new NullPointerException("matsSocketSessionId");
            }
            // :: Check if session is still registered with CSAF
            Optional<CurrentNode> currentNode;
            try {
                // ?: Check if it actually exists?
                if (!_clusterStoreAndForward.isSessionExists(matsSocketSessionId)) {
                    // -> No, it does not exist (already closed), so ignore this request.
                    log.info(" \\- Session [" + matsSocketSessionId
                            + "] does not exist in CSAF, ignoring server.closeSession(..)");
                    return;
                }
                // Get current node it resides on
                currentNode = _clusterStoreAndForward.getCurrentRegisteredNodeForSession(matsSocketSessionId);
            }
            catch (DataAccessException e) {
                // TODO: Fix.
                throw new AssertionError("Damn.");
            }

            // ?: Did we currently have a registered session?
            if (currentNode.isPresent()) {
                // -> Yes - Send this request over to that node to kill it both locally there, and in CSAF.
                // NOTE: It might be this node that is the current node, and that's fine.
                try {
                    _matsFactory.getDefaultInitiator().initiate(msg -> msg
                            .from("MatsSocketServer.closeSession")
                            .traceId("ServerCloseSession[" + matsSocketSessionId + "]" + rnd(5))
                            .to(nodeSubscriptionTerminatorId_NodeControl_ForNode(currentNode.get().getNodename()))
                            .publish(new NodeControl_CloseSessionDto(matsSocketSessionId, reason),
                                    new NodeControlStateDto(NodeControlStateDto.CLOSE_SESSION)));
                    return;
                }
                catch (MatsBackendException | MatsMessageSendException e) {
                    log.warn("Server side Close Session, and the MatsSocket Session was still registered"
                            + " in CSAF, but we didn't manage to communicate with MQ. Will ignore this and close it"
                            + " from CSAF anyway.", e);
                    // TODO: This is dodgy..
                }
            }

            // E-> No currently registered local session, so close it directly in CSAF here.

            // :: Invoke the SessionRemovedListeners
            invokeSessionRemovedEventListeners(new SessionRemovedEventImpl(SessionRemovedEventType.CLOSE,
                    matsSocketSessionId, null, reason));

            // :: Close it from the CSAF
            closeSessionFromCsaf(matsSocketSessionId);
        }
        finally {
            MDC.remove(MDC_SESSION_ID);
        }
    }

    void closeSessionFromCsaf(String matsSocketSessionId) {
        try {
            _clusterStoreAndForward.closeSession(matsSocketSessionId);
        }
        catch (DataAccessException e) {
            log.warn("Could not close session in CSAF. This is unfortunate, as it then is technically possible to"
                    + " still reconnect to the session while this evidently was not the intention"
                    + " (only the same user can reconnect, though). However, the session timeouter"
                    + " will clean this lingering session out after some hours.", e);
        }
    }

    private CopyOnWriteArrayList<SessionEstablishedEventListener> _sessionEstablishedEventListeners = new CopyOnWriteArrayList<>();

    @Override
    public void addSessionEstablishedEventListener(SessionEstablishedEventListener listener) {
        _sessionEstablishedEventListeners.add(listener);
    }

    static class SessionEstablishedEventImpl implements SessionEstablishedEvent {

        private final SessionEstablishedEventType _type;
        private final LiveMatsSocketSession _session;

        public SessionEstablishedEventImpl(SessionEstablishedEventType type, LiveMatsSocketSession session) {
            _type = type;
            _session = session;
        }

        @Override
        public SessionEstablishedEventType getType() {
            return _type;
        }

        @Override
        public LiveMatsSocketSession getMatsSocketSession() {
            return _session;
        }
    }

    void invokeSessionEstablishedEventListeners(SessionEstablishedEvent event) {
        _sessionEstablishedEventListeners.forEach(listener -> {
            try {
                listener.sessionEstablished(event);
            }
            catch (RuntimeException re) {
                log.error("SessionEstablishedEvent listener [" + listener + "] raised a ["
                        + re.getClass().getSimpleName() + "] when invoked.", re);
            }
        });
    }

    private CopyOnWriteArrayList<SessionRemovedEventListener> _sessionRemovedEventListeners = new CopyOnWriteArrayList<>();

    @Override
    public void addSessionRemovedEventListener(SessionRemovedEventListener listener) {
        _sessionRemovedEventListeners.add(listener);
    }

    static class SessionRemovedEventImpl implements SessionRemovedEvent {

        private final SessionRemovedEventType _type;
        private final String _matsSocketSessionId;
        private final Integer _closeCode;
        private final String _reason;

        public SessionRemovedEventImpl(
                SessionRemovedEventType type, String matsSocketSessionId, Integer closeCode, String reason) {
            _type = type;
            _matsSocketSessionId = matsSocketSessionId;
            _closeCode = closeCode;
            _reason = reason;
        }

        @Override
        public SessionRemovedEventType getType() {
            return _type;
        }

        @Override
        public String getMatsSocketSessionId() {
            return _matsSocketSessionId;
        }

        @Override
        public Optional<Integer> getCloseCode() {
            return Optional.ofNullable(_closeCode);
        }

        @Override
        public String getReason() {
            return _reason;
        }
    }

    void invokeSessionRemovedEventListeners(SessionRemovedEvent event) {
        _sessionRemovedEventListeners.forEach(listener -> {
            try {
                listener.sessionRemoved(event);
            }
            catch (RuntimeException re) {
                log.error("SessionRemovedEvent listener [" + listener + "] raised a ["
                        + re.getClass().getSimpleName() + "] when invoked.", re);
            }
        });
    }

    private CopyOnWriteArrayList<MessageEventListener> _messageEventListeners = new CopyOnWriteArrayList<>();

    @Override
    public void addMessageEventListener(MessageEventListener listener) {
        _messageEventListeners.add(listener);
    }

    static class MessageEventImpl implements MessageEvent {
        private final LiveMatsSocketSession _liveMatsSocketSession;
        private final List<MatsSocketEnvelopeWithMetaDto> _envelopes;

        public MessageEventImpl(LiveMatsSocketSession liveMatsSocketSession,
                List<MatsSocketEnvelopeWithMetaDto> envelopes) {
            _liveMatsSocketSession = liveMatsSocketSession;
            _envelopes = envelopes;
        }

        @Override
        public LiveMatsSocketSession getMatsSocketSession() {
            return _liveMatsSocketSession;
        }

        @Override
        public List<MatsSocketEnvelopeWithMetaDto> getEnvelopes() {
            return _envelopes;
        }
    }

    void invokeMessageEventListeners(LiveMatsSocketSession liveMatsSocketSession,
            List<MatsSocketEnvelopeWithMetaDto> envelopes) {
        if (!_messageEventListeners.isEmpty()) {
            MessageEventImpl messageEvent = new MessageEventImpl(liveMatsSocketSession, envelopes);
            _messageEventListeners.forEach(listener -> {
                try {
                    listener.messagesProcessed(messageEvent);
                }
                catch (RuntimeException re) {
                    log.error("MessageEvent listener [" + listener + "] raised a [" + re.getClass().getSimpleName()
                            + "] when invoked.", re);
                }
            });
        }
    }

    private void mats_publish(ProcessContext<Void> processContext, Void state, PublishedMessageDto msg) {
        ConcurrentHashMap<String, MatsSocketSessionAndMessageHandler> subs = _topicSubscriptions.get(msg.topicId);
        // ?: Was there any subscriptions?
        if (subs == null) {
            // -> No-one to publish to - ignore.
            return;
        }
        // E-> Yes, there are subscriptions.
        subs.values().forEach(session -> session.publishToTopic(msg.topicId, msg.env, msg.msg));

    }

    void closeWebSocketFor(String matsSocketSessionId, CurrentNode currentNode) {
        try {
            _matsFactory.getDefaultInitiator().initiate(msg -> msg
                    .from("MatsSocketServer.closeWebSocketFor")
                    .traceId("ServerCloseWebSocket[" + matsSocketSessionId + "]" + rnd(5))
                    .to(nodeSubscriptionTerminatorId_NodeControl_ForNode(currentNode.getNodename()))
                    .publish(new NodeControl_CloseWebSocketDto(matsSocketSessionId, currentNode.getConnectionId()),
                            new NodeControlStateDto(NodeControlStateDto.CLOSE_WEBSOCKET)));
        }
        catch (MatsBackendException | MatsMessageSendException e) {
            log.warn("Trying to close existing WebSocket due to reconnect for MatsSocketSessionId, but we"
                    + " didn't manage to communicate with the MQ.", e);
        }
    }

    void registerLocalMatsSocketSession(MatsSocketSessionAndMessageHandler session) {
        _activeSessionsByMatsSocketSessionId.put(session.getMatsSocketSessionId(), session);
    }

    Optional<MatsSocketSessionAndMessageHandler> getRegisteredLocalMatsSocketSession(String matsSocketSessionId) {
        return Optional.ofNullable(_activeSessionsByMatsSocketSessionId.get(matsSocketSessionId));
    }

    void deregisterLocalMatsSocketSession(String matsSocketSessionId, String connectionId) {
        // Concurrent-hack of removing a key if we have gotten the right connectionId.
        _activeSessionsByMatsSocketSessionId.computeIfPresent(matsSocketSessionId, (ignored, session) -> {
            if (session.getConnectionId().equals(connectionId)) {
                return null;
            }
            else {
                return session;
            }
        });
    }

    @Override
    public void stop(int gracefulShutdownMillis) {
        log.info("Asked to shut down MatsSocketServer [" + serverId() + "], containing ["
                + _activeSessionsByMatsSocketSessionId.size() + "] active sessions.");

        // Hinder further WebSockets connecting to us.
        _stopped = true;

        // :: Try to let currently processing messages finish up first.

        // Shut down incoming "information bearing messages" processing
        _incomingSrrMsgHandler.shutdown(gracefulShutdownMillis);

        // Shut down outbox forwarder subsystem.
        _webSocketOutboxForwarder.shutdown(gracefulShutdownMillis);

        // :: Close all MatsSocketSessions, and thus WebSockets

        // Deregister all MatsSocketSession from us, with SERVICE_RESTART, which asks them to reconnect
        ArrayList<MatsSocketSessionAndMessageHandler> sessions = new ArrayList<>(
                _activeSessionsByMatsSocketSessionId.values());
        sessions.forEach(session -> {
            session.deregisterSessionAndCloseWebSocket(MatsSocketCloseCodes.SERVICE_RESTART,
                    "From Server: Server instance is going down, please reconnect.");
        });

        // :: Shut down rest of subsystems

        // Shut down outgoing acks subsystem.
        _webSocketOutgoingAcks.shutdown(gracefulShutdownMillis);

        // Shut down Liveliness Updater and Timeouter subsystem.
        _casfUpdateAndTimeouter.shutdown(gracefulShutdownMillis);
    }

    Optional<MatsSocketEndpointRegistration<?, ?, ?>> getMatsSocketEndpointRegistration(String endpointId) {
        if (endpointId == null) {
            throw new NullPointerException("endpointId");
        }
        MatsSocketEndpointRegistration<?, ?, ?> registration = _matsSocketEndpointsByMatsSocketEndpointId.get(
                endpointId);
        return Optional.ofNullable(registration);
    }

    /**
     * Shall be one instance per socket (i.e. from the docs: "..there will be precisely one endpoint instance per active
     * client connection"), thus there should be 1:1 correlation between this instance and the single Session object for
     * the same cardinality (per client:server connection).
     */
    static class MatsWebSocketInstance extends Endpoint {
        private final DefaultMatsSocketServer _matsSocketServer;
        private final SessionAuthenticator _sessionAuthenticator;
        private final HandshakeRequestResponse _handshakeRequestResponse;

        // Will be set when onOpen is invoked
        private String _connectionId;
        private MatsSocketSessionAndMessageHandler _matsSocketSessionAndMessageHandler;

        public MatsWebSocketInstance(DefaultMatsSocketServer matsSocketServer,
                SessionAuthenticator sessionAuthenticator,
                HandshakeRequestResponse handshakeRequestResponse) {
            log.info("Created MatsWebSocketEndpointInstance: " + id(this));
            _matsSocketServer = matsSocketServer;
            _sessionAuthenticator = sessionAuthenticator;
            _handshakeRequestResponse = handshakeRequestResponse;
        }

        private boolean _isTimeoutException;

        public DefaultMatsSocketServer getMatsSocketServer() {
            return _matsSocketServer;
        }

        @Override
        public void onOpen(Session session, EndpointConfig config) {
            log.info("WebSocket @OnOpen, WebSocket SessionId:" + session.getId()
                    + ", WebSocket Session:" + id(session) + ", this:" + id(this));

            // Notice: On a particular server, the session.getId() is already unique. On Jetty: integer sequence.
            _connectionId = session.getId() + "_" + rnd(6);

            // ?: If we are going down, then immediately close it.
            if (_matsSocketServer._stopped) {
                closeWebSocket(session, MatsSocketCloseCodes.SERVICE_RESTART,
                        "This server is going down, perform a (re)connect to another instance.");
                return;
            }

            // We do not (yet) handle binary messages, so limit that pretty hard.
            session.setMaxBinaryMessageBufferSize(1024);
            // Set low limits for the HELLO message, 20KiB should be plenty even for quite large Oauth2 bearer tokens.
            session.setMaxTextMessageBufferSize(20 * 1024);
            // Set low time to say HELLO after the connect. (The default clients say it immediately on "onopen".)
            session.setMaxIdleTimeout(5000);

            try {
                boolean ok = _sessionAuthenticator.onOpen(session, (ServerEndpointConfig) config);
                log.info("webSocket.onOpen(..): Asked SessionAuthenticator.onOpen(..), returned: "
                        + (ok ? "OK" : "NOT OK!"));
                if (!ok) {
                    closeWebSocket(session, MatsSocketCloseCodes.VIOLATED_POLICY, "SessionAuthenticator did not want"
                            + " this session to proceed");
                    return;
                }
            }
            catch (Throwable t) {
                log.error("webSocket.onOpen(..): Got throwable when invoking SessionAuthenticator.onOpen(..)."
                        + " Closing WebSocket.", t);
                closeWebSocket(session, MatsSocketCloseCodes.VIOLATED_POLICY, "SessionAuthenticator did not want this"
                        + " session to proceed");
                return;
            }

            // :: Try to get Remote Address
            String remoteAddr = RemoteAddressContainerHacks.attemptGetRemoteAddress(session);

            // :: Create the MatsSocketSession - which also is the WebSocket MessageHandler.
            _matsSocketSessionAndMessageHandler = new MatsSocketSessionAndMessageHandler(_matsSocketServer, session,
                    _connectionId, _handshakeRequestResponse._handshakeRequest, _sessionAuthenticator, remoteAddr);

            // :: Register it as the MessageHandler
            session.addMessageHandler(_matsSocketSessionAndMessageHandler);
        }

        @Override
        public void onError(Session session, Throwable thr) {
            try { // finally: MDC.clear()
                if (_matsSocketSessionAndMessageHandler != null) {
                    _matsSocketSessionAndMessageHandler.setMDC();
                }

                // Deduce if this is a Server side timeout
                // Note: This is modelled after Jetty. If different with other JSR 356 implementations, please expand.
                _isTimeoutException = (thr.getCause() instanceof TimeoutException
                        || ((thr.getMessage() != null) && thr.getMessage().toLowerCase().contains("timeout expired")));

                // ?: Is it a timeout situation?
                if (_isTimeoutException) {
                    // -> Yes, timeout. This is handled, and does not constitute a "close session", client might
                    // just have lost connection and want's to reconnect soon. Just log info.
                    log.info("WebSocket @OnError: WebSocket server timed out the connection. MatsSocket SessionId: ["
                            + (_matsSocketSessionAndMessageHandler == null
                                    ? "no MatsSocketSession"
                                    : _matsSocketSessionAndMessageHandler.getMatsSocketSessionId())
                            + "], WebSocket SessionId:" + session.getId() + ", this:" + id(this));
                }
                else {
                    // -> No, not timeout. So this is some kind of unexpected situation, typically raised from the
                    // MastSocket implementation on MessageHandler. Log warn. This will close the session.
                    log.warn("WebSocket @OnError, MatsSocket SessionId: ["
                            + (_matsSocketSessionAndMessageHandler == null
                                    ? "no MatsSocketSession"
                                    : _matsSocketSessionAndMessageHandler.getMatsSocketSessionId())
                            + "], WebSocket SessionId:" + session.getId() + ", this:" + id(this),
                            new Exception("MatsSocketServer's webSocket.onError(..) handler", thr));
                }
            }
            finally {
                MDC.clear();
            }
        }

        @Override
        public void onClose(Session session, CloseReason closeReason) {
            try { // finally: MDC.clear()
                if (_matsSocketSessionAndMessageHandler != null) {
                    _matsSocketSessionAndMessageHandler.setMDC();
                }
                log.info("WebSocket @OnClose, code:[" + MatsSocketCloseCodes.getCloseCode(closeReason.getCloseCode()
                        .getCode()) + "] (timeout:[" + _isTimeoutException + "]), reason:[" + closeReason
                                .getReasonPhrase()
                        + "], MatsSocket SessionId: [" + (_matsSocketSessionAndMessageHandler == null
                                ? "no MatsSocketSession"
                                : _matsSocketSessionAndMessageHandler.getMatsSocketSessionId())
                        + "], ConnectionId:" + _connectionId + ", this:" + id(this));

                // ?: Have we gotten MatsSocketSession yet?
                // (Just in case "onOpen" has not been invoked yet. Can it happen?!).
                if (_matsSocketSessionAndMessageHandler != null) {
                    // -> Yes, so either close session, or just deregister us from local and CSAF
                    // Is this a GOING_AWAY that is NOT from the server side? (Jetty gives this on timeout)
                    boolean goingAwayFromClientSide = (MatsSocketCloseCodes.GOING_AWAY.getCode() == closeReason
                            .getCloseCode().getCode()) && (!_isTimeoutException);
                    // ?: Did the client or Server want to actually Close Session?
                    // NOTE: Need to check by the 'code' integers, since no real enum (CloseCode is an interface).
                    if ((MatsSocketCloseCodes.UNEXPECTED_CONDITION.getCode() == closeReason.getCloseCode().getCode())
                            || (MatsSocketCloseCodes.PROTOCOL_ERROR.getCode() == closeReason.getCloseCode().getCode())
                            || (MatsSocketCloseCodes.VIOLATED_POLICY.getCode() == closeReason.getCloseCode().getCode())
                            || (MatsSocketCloseCodes.CLOSE_SESSION.getCode() == closeReason.getCloseCode().getCode())
                            || (MatsSocketCloseCodes.SESSION_LOST.getCode() == closeReason.getCloseCode().getCode())
                            || goingAwayFromClientSide) {
                        // -> Yes, this was a one of the actual-close CloseCodes, or a "GOING AWAY" that was NOT
                        // initiated from server side, which means that we should actually close this session
                        log.info("Explicitly Closed MatsSocketSession due to CloseCode ["
                                + MatsSocketCloseCodes.getCloseCode(closeReason.getCloseCode().getCode())
                                + "] (timeout:[" + _isTimeoutException + "]), actually closing (terminating) it.");
                        // Close MatsSocketSession
                        _matsSocketSessionAndMessageHandler.closeSession(closeReason.getCloseCode().getCode(),
                                closeReason.getReasonPhrase());
                    }
                    else {
                        // -> No, this was a broken connection, or something else like an explicit disconnect w/o close
                        log.info("Got a non-closing CloseCode [" + MatsSocketCloseCodes.getCloseCode(closeReason
                                .getCloseCode().getCode()) + "] (timeout:[" + _isTimeoutException
                                + "]), assuming that Client might want to reconnect - deregistering"
                                + " MatsSocketSession from CSAF.");
                        // Deregister MatsSocketSession
                        _matsSocketSessionAndMessageHandler.deregisterSession(closeReason.getCloseCode().getCode(),
                                closeReason.getReasonPhrase());
                    }
                }
            }
            finally {
                MDC.clear();
            }
        }
    }

    static void closeWebSocket(Session webSocketSession, CloseCode closeCode, String reasonPhrase) {
        log.info("Closing WebSocket SessionId [" + webSocketSession.getId() + "]: code: [" + closeCode
                + "(" + closeCode.getCode() + ")], reason:[" + reasonPhrase + "]");
        try {
            while (reasonPhrase != null && reasonPhrase.getBytes(StandardCharsets.UTF_8).length > 123) {
                reasonPhrase = reasonPhrase.substring(0, reasonPhrase.length() - 1);
            }
            webSocketSession.close(new CloseReason(closeCode, reasonPhrase));
        }
        catch (IOException e) {
            log.warn("Got Exception when trying to close WebSocket SessionId [" + webSocketSession.getId()
                    + "], ignoring.", e);
        }
    }

    /**
     * Registrations of MatsSocketEndpoint - these are the definitions of which targets a MatsSocket client can send
     * messages to, i.e. SEND or REQUEST.
     *
     * @param <I>
     *            incoming MatsSocket DTO
     * @param <MR>
     *            reply Mats DTO
     * @param <R>
     *            reply MatsSocket DTO
     */
    static class MatsSocketEndpointRegistration<I, MR, R> implements MatsSocketEndpoint<I, MR, R> {
        private final String _matsSocketEndpointId;
        private final Class<I> _incomingClass;
        private final Class<MR> _matsReplyClass;
        private final Class<R> _replyClass;
        private final IncomingAuthorizationAndAdapter<I, MR, R> _incomingAuthEval;
        private final ReplyAdapter<I, MR, R> _replyAdapter;

        private final DebugStackTrace _registrationPoint;

        public MatsSocketEndpointRegistration(String matsSocketEndpointId,
                Class<I> incomingClass, Class<MR> matsReplyClass, Class<R> replyClass,
                IncomingAuthorizationAndAdapter<I, MR, R> incomingAuthEval, ReplyAdapter<I, MR, R> replyAdapter) {
            _matsSocketEndpointId = matsSocketEndpointId;
            _incomingClass = incomingClass;
            _matsReplyClass = matsReplyClass;
            _replyClass = replyClass;
            _incomingAuthEval = incomingAuthEval;
            _replyAdapter = replyAdapter;

            _registrationPoint = new DebugStackTrace("registration of MatsSocketEndpoint [" + matsSocketEndpointId
                    + "]");
        }

        @Override
        public String getMatsSocketEndpointId() {
            return _matsSocketEndpointId;
        }

        @Override
        public Class<I> getIncomingClass() {
            return _incomingClass;
        }

        @Override
        public Class<R> getReplyClass() {
            return _replyClass;
        }

        @Override
        public Class<MR> getMatsReplyClass() {
            return _matsReplyClass;
        }

        IncomingAuthorizationAndAdapter<I, MR, R> getIncomingAuthEval() {
            return _incomingAuthEval;
        }

        @Override
        public String toString() {
            return "MatsSocketEndpoint{Id='" + _matsSocketEndpointId + '\'' +
                    ", incoming=" + _incomingClass.getSimpleName() +
                    ", matsReply=" + _matsReplyClass.getSimpleName() +
                    ", reply=" + _replyClass.getSimpleName() + '}';
        }
    }

    private static class NodeControlStateDto {
        static String NEW_MESSAGE_FOR_SESSION = "NewMsg";
        static String CLOSE_SESSION = "CloseSess";
        static String CLOSE_WEBSOCKET = "CloseWS";
        String t;

        public NodeControlStateDto() {
            /* for Jackson */
        }

        private NodeControlStateDto(String type) {
            this.t = type;
        }
    }

    private static class NodeControl_CloseSessionDto {
        String sid;
        String reason;

        public NodeControl_CloseSessionDto() {
            /* for Jackson */
        }

        public NodeControl_CloseSessionDto(String sessionId, String reason) {
            this.sid = sessionId;
            this.reason = reason;
        }
    }

    private static class NodeControl_CloseWebSocketDto {
        String sid;
        String cid;

        public NodeControl_CloseWebSocketDto() {
            /* for Jackson */
        }

        public NodeControl_CloseWebSocketDto(String sessionId, String connectionId) {
            this.sid = sessionId;
            this.cid = connectionId;
        }
    }

    private static class NodeControl_NewMessageForSessionDto {
        String sid;

        public NodeControl_NewMessageForSessionDto() {
            /* for Jackson */
        }

        public NodeControl_NewMessageForSessionDto(String sessionId) {
            this.sid = sessionId;
        }
    }

    private void mats_nodeControl(ProcessContext<Void> processContext,
            NodeControlStateDto state, MatsObject incomingMsg) {
        if (NodeControlStateDto.NEW_MESSAGE_FOR_SESSION.equals(state.t)) {
            NodeControl_NewMessageForSessionDto dto = incomingMsg.toClass(NodeControl_NewMessageForSessionDto.class);
            nodeControl_notifyNewMessage(dto.sid);
        }
        if (NodeControlStateDto.CLOSE_SESSION.equals(state.t)) {
            NodeControl_CloseSessionDto dto = incomingMsg.toClass(NodeControl_CloseSessionDto.class);
            nodeControl_closeSession(dto.sid, dto.reason);
        }
        if (NodeControlStateDto.CLOSE_WEBSOCKET.equals(state.t)) {
            NodeControl_CloseWebSocketDto dto = incomingMsg.toClass(NodeControl_CloseWebSocketDto.class);
            nodeControl_closeWebSocket(dto.sid, dto.cid);
        }
    }

    private void nodeControl_closeSession(String matsSocketSessionId, String reason) {
        // Find local session
        Optional<MatsSocketSessionAndMessageHandler> localSession = getRegisteredLocalMatsSocketSession(
                matsSocketSessionId);
        // ?: Was the local session actually present on this node (as somebody thought it was)?
        if (localSession.isPresent()) {
            // -> Yes, local session present, so close it
            localSession.get().closeSessionAndWebSocket(MatsSocketCloseCodes.CLOSE_SESSION,
                    "Server Side: " + reason);
        }
        else {
            // -> No, it was not present here after all (somebody thought that it was).
            // We'll have to invoked SessionRemovedEvent listeners about CLOSE (it is our responsibility).
            invokeSessionRemovedEventListeners(new SessionRemovedEventImpl(SessionRemovedEventType.CLOSE,
                    matsSocketSessionId, null, reason));
            // Close it from the CSAF
            closeSessionFromCsaf(matsSocketSessionId);
        }
    }

    private void nodeControl_closeWebSocket(String matsSocketSessionId, String connectionId) {
        // Find local session
        Optional<MatsSocketSessionAndMessageHandler> localMatsSocketSession = getRegisteredLocalMatsSocketSession(
                matsSocketSessionId);
        // ?: Do we have this session, and is it the right ConnectionId
        if (localMatsSocketSession.isPresent()
                && connectionId.equals(localMatsSocketSession.get().getConnectionId())) {
            // -> Yes, so close it.
            localMatsSocketSession.get().deregisterSessionAndCloseWebSocket(MatsSocketCloseCodes.DISCONNECT,
                    "Cannot have two MatsSockets with the same MatsSocketSessionId - closing the previous"
                            + " (from remote node)");
        }
    }

    private void nodeControl_notifyNewMessage(String matsSocketSessionId) {
        // Get the local /Live/ MatsSocketSession
        Optional<MatsSocketSessionAndMessageHandler> localMatsSocketSession = getRegisteredLocalMatsSocketSession(
                matsSocketSessionId);
        // ?: If this Session does not exist at this node, we cannot deliver.
        if (!localMatsSocketSession.isPresent()) {
            // -> No, the MatsSocketSession is not here!
            // Someone must have found that we had it, but this must have asynchronously have been deregistered.
            // Do forward of notification - it will handle if we're the node being registered.
            newMessageOnWrongNode_NotifyCorrectHome(matsSocketSessionId);
        }
        else {
            // -> Yes, local session is here!
            // Get the Forwarder to send any new messages over the WebSocket.
            _webSocketOutboxForwarder.newMessagesInCsafNotify(localMatsSocketSession.get());
        }
    }

    static class ReplyHandleStateDto {
        private final String sid;
        private final String cmid;
        private final String ms_eid;

        private final Integer resd; // Resolved (Requested & Allowed) DebugOptions - CAN BE NULL

        private final long cmrts; // Client Message Received Timestamp (Server timestamp)
        private final String cmrnn; // Received Nodeanme

        private final long mmsts; // Mats Message Sent Timestamp (when the message was sent onto Mats MQ fabric, Server
                                  // timestamp)

        private ReplyHandleStateDto() {
            /* no-args constructor for Jackson */
            sid = null;
            cmid = null;
            ms_eid = null;

            resd = null;

            cmrts = 0;
            cmrnn = null;

            mmsts = 0;
        }

        ReplyHandleStateDto(String matsSocketSessionId, String matsSocketEndpointId,
                String clientMessageId, Integer resolvedDebugFlags,
                long clientMessageReceivedTimestamp, String clientMessageReceivedNodeName,
                long matsMessageSentTimestamp) {
            sid = matsSocketSessionId;
            cmid = clientMessageId;
            ms_eid = matsSocketEndpointId;

            resd = resolvedDebugFlags;

            cmrts = clientMessageReceivedTimestamp;
            cmrnn = clientMessageReceivedNodeName; // Also where MatsMessage is Sent.

            mmsts = matsMessageSentTimestamp;
        }
    }

    private static class MatsSocketEndpointReplyContextImpl<I, MR, R> implements
            MatsSocketEndpointReplyContext<I, MR, R> {
        private final MatsSocketEndpoint<I, MR, R> _matsSocketEndpoint;
        private final DetachedProcessContext _detachedProcessContext;
        private final MR _matsReplyMessage;

        public MatsSocketEndpointReplyContextImpl(
                MatsSocketEndpoint<I, MR, R> matsSocketEndpoint,
                DetachedProcessContext detachedProcessContext, MR matsReplyMessage) {
            _matsSocketEndpoint = matsSocketEndpoint;
            _detachedProcessContext = detachedProcessContext;
            _matsReplyMessage = matsReplyMessage;
        }

        private R _matsSocketReplyMessage;
        private IncomingResolution _handled = IncomingResolution.NO_ACTION;

        @Override
        public MatsSocketEndpoint<I, MR, R> getMatsSocketEndpoint() {
            return _matsSocketEndpoint;
        }

        @Override
        public DetachedProcessContext getMatsContext() {
            return _detachedProcessContext;
        }

        @Override
        public MR getMatsReplyMessage() {
            return _matsReplyMessage;
        }

        @Override
        public void resolve(R matsSocketResolveMessage) {
            if (_handled != IncomingResolution.NO_ACTION) {
                throw new IllegalStateException("Already handled.");
            }
            _matsSocketReplyMessage = matsSocketResolveMessage;
            _handled = IncomingResolution.RESOLVE;
        }

        @Override
        public void reject(R matsSocketRejectMessage) {
            if (_handled != IncomingResolution.NO_ACTION) {
                throw new IllegalStateException("Already handled.");
            }
            _matsSocketReplyMessage = matsSocketRejectMessage;
            _handled = IncomingResolution.REJECT;
        }
    }

    private void mats_replyHandler(ProcessContext<Void> processContext,
            ReplyHandleStateDto state, MatsObject matsObject) {
        long matsMessageReplyReceivedTimestamp = System.currentTimeMillis();

        /*
         * NOTICE!! Wrt. MDC: This is a Mats Stage Lambda. The MDC is cleared by Mats after a message is processed.
         * There is therefore no need for try-finally, AND it is just good that the stuff we set here is present for any
         * log lines within Mats afterwards (i.e. commit of database, JMS etc - if this fails).
         */
        // NOTICE: traceId is already set by Mats (this is a Stage lambda).
        MDC.put(MDC_SESSION_ID, state.sid);
        MDC.put(MDC_CMID, state.cmid);

        // Find the MatsSocketEndpoint for this reply
        Optional<MatsSocketEndpointRegistration<?, ?, ?>> regO = getMatsSocketEndpointRegistration(state.ms_eid);
        if (!regO.isPresent()) {
            throw new AssertionError("The MatsSocketEndpoint has disappeared since this message was"
                    + " initiated. This can only happen if the server has been redeployed between the request and"
                    + " its reply, where the new codebase that does not have this in MatsSocketEndpoint anymore,"
                    + " or that the nodes that constitute MatsSocketServer instance [" + _instanceName + "] runs"
                    + " different codebases.");
        }
        MatsSocketEndpointRegistration<?, ?, ?> registration = regO.get();

        Object matsReply = registration._matsReplyClass == MatsObject.class
                ? matsObject
                : matsObject.toClass(registration._matsReplyClass);

        MatsSocketEnvelopeWithMetaDto replyEnvelope = new MatsSocketEnvelopeWithMetaDto();
        Object msReply;
        ReplyAdapter<?, ?, ?> replyAdapter = registration._replyAdapter;
        if (replyAdapter != null) {
            try {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                MatsSocketEndpointReplyContextImpl<?, ?, ?> replyContext = new MatsSocketEndpointReplyContextImpl(
                        registration, processContext, matsReply);
                invokeAdaptReply(matsReply, replyAdapter, replyContext);

                switch (replyContext._handled) {
                    case NO_ACTION:
                        // -> The user did not invoke neither .resolve() nor .reject().
                        log.warn("adaptReply(..) evidently ignored the Mats message, settling with REJECT.");
                        replyEnvelope.t = REJECT;
                        replyEnvelope.desc = "ReplyAdapter didn't decide what action, thus REJECT.";
                        msReply = null;
                        break;
                    case RESOLVE:
                    case REJECT:
                        // -> The user settled with .resolve() or .reject()
                        MessageType settled = replyContext._handled == IncomingResolution.RESOLVE
                                ? RESOLVE
                                : REJECT;
                        if (log.isDebugEnabled())
                            log.debug("adaptReply(..) settled the reply with [" + settled + "]");
                        replyEnvelope.t = settled;
                        msReply = replyContext._matsSocketReplyMessage;
                        break;
                    default:
                        throw new AssertionError("Unhandled enum value [" + replyContext._handled + "]");
                }
            }
            catch (RuntimeException rte) {
                log.warn("adaptReply(..)  raised [" + rte.getClass().getSimpleName() + "], settling with REJECT",
                        rte);
                replyEnvelope.t = REJECT;
                // TODO: DEBUG: If debug enabled for authenticated user, set description to full stacktrace.
                replyEnvelope.desc = rte.getMessage();
                msReply = null;
            }
        }
        else if (registration._matsReplyClass == registration._replyClass) {
            // -> Return same class
            replyEnvelope.t = RESOLVE;
            msReply = matsReply;
            log.debug("No ReplyAdapter, so replying with RESOLVE.");
        }
        else {
            throw new AssertionError("No adapter present, but the class from Mats ["
                    + registration._matsReplyClass.getName() + "] != the expected reply from MatsSocketEndpoint ["
                    + registration._replyClass.getName() + "].");
        }

        // MDC: Set the decided message type.
        MDC.put(MDC_MESSAGE_TYPE, replyEnvelope.t.toString());

        // Create Envelope
        replyEnvelope.cmid = state.cmid;
        replyEnvelope.tid = processContext.getTraceId(); // TODO: Chop off last ":xyz", as that is added serverside.

        EnumSet<DebugOption> debugOptions = DebugOption.enumSetOf(state.resd);

        if (!debugOptions.isEmpty()) {
            // Create DebugDto - must do this "eagerly", as we do not know what the client actually wants.
            DebugDto debug = new DebugDto();
            // As long as there is ANY DebugOptions, we store the resolved debug options in the DebugDto
            debug.resd = state.resd;
            // :: Timestamps
            if (debugOptions.contains(DebugOption.TIMESTAMPS)) {
                debug.cmrts = state.cmrts;
                debug.mmsts = state.mmsts;
                debug.mmrrts = matsMessageReplyReceivedTimestamp;
            }
            // :: Node names
            if (debugOptions.contains(DebugOption.NODES)) {
                debug.cmrnn = state.cmrnn; // The receiving nodename
                debug.mmrrnn = getMyNodename(); // The Mats-receiving nodename (this processing)
            }

            replyEnvelope.debug = debug;
        }

        // Serialize the actual message
        String serializedMessage = serializeMessageObject(msReply);

        Optional<CurrentNode> currentNode = storeMessageInCsafOutbox_WithSmidRetry(replyEnvelope, state.sid,
                processContext.getTraceId(), state.cmrts, serializedMessage, null);

        // ?: Check if WE have the session locally
        Optional<MatsSocketSessionAndMessageHandler> localMatsSocketSession = getRegisteredLocalMatsSocketSession(
                state.sid);
        // ?: Did we have the targeted MatsSocketSession locally?
        if (!localMatsSocketSession.isPresent()) {
            // -> We did NOT have it locally - do remote ping.
            pingRemoteNodeAfterMessageStored(state.sid, currentNode, "MatsSocketServer.reply");
            return;
        }
        // E-> Yes, evidently we have it locally! Do local forward.

        // NOTICE!!! WE MUST use the Mats-trick of doing this AFTER commit to db, since we've put things in DB.
        processContext.doAfterCommit(() -> _webSocketOutboxForwarder
                .newMessagesInCsafNotify(localMatsSocketSession.get()));
    }

    private Optional<CurrentNode> storeMessageInCsafOutbox_WithSmidRetry(MatsSocketEnvelopeWithMetaDto replyEnvelope,
            String sessionId, String traceId, Long clientMessageReceivedTimestamp, String serializedMessage,
            byte[] messageBinary) {
        Optional<CurrentNode> currentNode;
        // Generate the SMID
        String serverMessageId = serverMessageId();
        // Stick it on the MDC
        MDC.put(MDC_SMID, serverMessageId);
        // Set it on the envelope
        replyEnvelope.smid = serverMessageId;

        // Now serialize and store the envelope for forward ("StoreAndForward")
        String serializedEnvelope = serializeEnvelope(replyEnvelope);

        try {
            currentNode = _clusterStoreAndForward.storeMessageInOutbox(
                    sessionId, serverMessageId, replyEnvelope.cmid, traceId, replyEnvelope.t,
                    clientMessageReceivedTimestamp, serializedEnvelope, serializedMessage, messageBinary);
            return currentNode;
        }
        catch (DataAccessException e) {
            // TODO: Fix
            throw new AssertionError("Damn", e);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void invokeAdaptReply(Object matsReply, ReplyAdapter replyAdapter,
            MatsSocketEndpointReplyContextImpl replyContext) {
        replyAdapter.adaptReply(replyContext, matsReply);
    }

    private void pingLocalOrRemoteNodeAfterMessageStored(String sessionId, Optional<CurrentNode> currentNode,
            String from) {
        // ?: Check if WE have the session locally
        Optional<MatsSocketSessionAndMessageHandler> localMatsSocketSession = getRegisteredLocalMatsSocketSession(
                sessionId);
        if (localMatsSocketSession.isPresent()) {
            // -> Yes, evidently we have it! Do local forward.
            _webSocketOutboxForwarder.newMessagesInCsafNotify(localMatsSocketSession.get());
            return;
        }
        // E-> We did not have it locally - do remote ping.
        pingRemoteNodeAfterMessageStored(sessionId, currentNode, from);
    }

    private void pingRemoteNodeAfterMessageStored(String sessionId, Optional<CurrentNode> currentNode, String from) {
        // ?: If we do have a nodename, ping it about new message
        if (currentNode.isPresent()) {
            // -> Yes we got a nodename, ping it.
            try {
                _matsFactory.getDefaultInitiator().initiate(init -> init
                        .from(from)
                        .traceId("NewMessageForSession[" + sessionId + "]" + rnd(5))
                        .to(nodeSubscriptionTerminatorId_NodeControl_ForNode(currentNode.get().getNodename()))
                        .publish(new NodeControl_NewMessageForSessionDto(sessionId),
                                new NodeControlStateDto(NodeControlStateDto.NEW_MESSAGE_FOR_SESSION)));
            }
            catch (MatsBackendException | MatsMessageSendException e) {
                log.warn("Got [" + e.getClass().getSimpleName()
                        + "] when trying to send Mats-ping to current node holding"
                        + " the websocket [" + currentNode.get().getNodename()
                        + "], ignoring, hoping for self-healer to figure it out", e);
            }
        }
    }

    void newMessageOnWrongNode_NotifyCorrectHome(String sessionId) {
        Optional<CurrentNode> currentNode;
        try {
            // Find which node the Session is supposed to live on.
            currentNode = _clusterStoreAndForward.getCurrentRegisteredNodeForSession(sessionId);

            // ?: Did we get a node?
            if (!currentNode.isPresent()) {
                // -> No, so nothing to do - MatsSocket will get messages when he reconnect.
                log.info("MatsSocketSession [" + sessionId + "] is not present on any node. Ignoring,"
                        + " hoping that client will come back and get his messages later.");
                return;
            }

            // ?: Was the node registered in CSAF /this/ node?
            if (currentNode.get().getNodename().equalsIgnoreCase(getMyNodename())) {
                // -> Oops, yes.
                // Find the local session.
                Optional<MatsSocketSessionAndMessageHandler> localSession = getRegisteredLocalMatsSocketSession(
                        sessionId);
                // ?: Do we have this session locally?!
                if (!localSession.isPresent()) {
                    // -> No, we do NOT have this session locally!
                    // NOTICE: This could e.g. happen if DB down when trying to deregister the MatsSocketSession.
                    log.info("MatsSocketSession [" + sessionId + "] is said by CSAF to live on this node, but we do"
                            + " not have it. Tell the CSAF this (deregister), and ignore, hoping that client will come"
                            + " back and get his messages later.");
                    /*
                     * Fix this wrongness: Tell CSAF that what he has registered wrt. SessionId+ConnectionId combo is
                     * WRONG, so deregister that. Any concurrently newly registered SessionId would have a different
                     * ConnectionId, so such asynchronous races are handled.
                     *
                     * Race is as such: Fetching "CurrentNode" from CSAF, which returns SessionId:ConnectionId "A:1",
                     * which should be present on this node - but which is /not/ on this node. At the same time, Session
                     * "A" registers on this (or a different) node, gets SessionId:ConnectionId "A:2". However, at
                     * /this/ particular point /this/ thread evaluate that Session "A" is (was) not here (think
                     * multi-node, multi-CPU and preemptive multitasking), therefore we deregister what _CSAF_ told us:
                     * "A:1". But then that "1" vs. "2" saves us: "A:1" is not in CSAF anymore, as it currently holds
                     * "A:2" for Session "A", so Session "A" will not deregister in CSAF.
                     */
                    _clusterStoreAndForward.deregisterSessionFromThisNode(sessionId,
                            currentNode.get().getConnectionId());
                    // Sorry, no can do.
                    return;
                }
                // E-> Yes, we had it locally: We'll do a standard Mats notify of new messages, which then should
                // come back to nodeControl_notifyNewMessage(..), and hopefully things will resolve then.
            }
        }
        catch (DataAccessException e) {
            log.warn("Got '" + e.getClass().getSimpleName() + "' when trying to find node home for"
                    + " MatsSocketSession [" + sessionId + "] using '"
                    + _clusterStoreAndForward.getClass().getSimpleName() + "'. Bailing out, hoping for"
                    + " self-healer process to figure it out.", e);
            return;
        }

        // Send message to current home for MatsSocketSession
        // NOTE: it /might/ be us if massive async, but that'll eventually be resolved: Check
        // nodeControl_notifyNewMessage(..), which will either do local forward (if this node), or send us back to this
        // method (if "wrong" node).
        pingLocalOrRemoteNodeAfterMessageStored(sessionId, currentNode, "MatsSocketServer.newMessageOnWrongNode");
    }

    String serializeEnvelope(MatsSocketEnvelopeDto msReplyEnvelope) {
        if (msReplyEnvelope.t == null) {
            throw new IllegalStateException("Type ('t') cannot be null.");
        }
        try {
            return _envelopeObjectWriter.writeValueAsString(msReplyEnvelope);
        }
        catch (JsonProcessingException e) {
            throw new AssertionError("Huh, couldn't serialize envelope?!");
        }
    }

    String serializeMessageObject(Object message) {
        try {
            return _jackson.writeValueAsString(message);
        }
        catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize message of type [" + message.getClass().getName()
                    + "].");
        }
    }

    /**
     * A-Z, a-z, 0-9, which is 62 chars.
     */
    public static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    /**
     * All chars from 20-7f, except:
     * <ul>
     * <li>Space (32, 0x20) - since spaces are always annoying to use in Ids</li>
     * <li>" (34, 0x22) - since this is the start and end symbol of a String</li>
     * <li>\ (92 0x5c) - since this is the escape char</li>
     * <li>DEL (127, 0x7f) - since this is a control char</li>
     * </ul>
     * This is 92 chars.
     */
    public static final String ALPHABET_JSON_ID;
    static {
        StringBuilder buf = new StringBuilder();
        // Just to make the point explicit..:
        for (int c = 0x20; c <= 0x7f; c++) {
            if (c == 0x20 || c == 0x22 || c == 0x5c || c == 0x7f) {
                continue;
            }
            buf.append((char) c);
        }
        ALPHABET_JSON_ID = buf.toString();
    }

    /**
     * The alphabet is A-Z, a-z, 0-9, which is 62 chars.
     *
     * @param length
     *            the desired length of the returned random string.
     * @return a random string of the specified length.
     */
    static String rnd(int length) {
        StringBuilder buf = new StringBuilder(length);
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        for (int i = 0; i < length; i++)
            buf.append(ALPHABET.charAt(tlr.nextInt(ALPHABET.length())));
        return buf.toString();
    }

    /**
     * All visible and JSON non-quoted ASCII chars, i.e. from 0x20-0x7f, except 0x20, 0x22, 0x5c and 0x7f, which is 92
     * chars.
     *
     * @param length
     *            the desired length of the returned random string.
     * @return a random string of the specified length.
     */
    static String rndJsonId(int length) {
        StringBuilder buf = new StringBuilder(length);
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        for (int i = 0; i < length; i++)
            buf.append(ALPHABET_JSON_ID.charAt(tlr.nextInt(ALPHABET_JSON_ID.length())));
        return buf.toString();
    }

    /**
     * Returns a string of 4 chars from a 92-char alphabet, consisting of all the visible and JSON-non-quoted ASCII
     * chars. 92^4 = 71.639.296. Notice that if we end up duplicating on the server side, this can be caught. The ONLY
     * time span where this MUST be "globally unique", is after ACK, when we delete the id on server side (and another
     * message conceivably can get the Id again) and then send ACK2, before the client receives the ACK2 and deletes the
     * Id from its inbox. If a message from the server managed to pick the same Id in <i>that particular timespan</i>,
     * the client would refuse it as a double delivery.
     *
     * @return a 4-char invocation of {@link #rndJsonId(int)}.
     */
    static String serverMessageId() {
        return rndJsonId(8);
    }

    static String id(Object x) {
        return x.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(x));
    }

    static String escape(String string) {
        // TODO: Implement HTML escaping (No messages from us should not go through JSONifying already).
        return string;
    }
}
