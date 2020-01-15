package com.stolsvik.mats.websocket.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

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

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.stolsvik.mats.MatsEndpoint.DetachedProcessContext;
import com.stolsvik.mats.MatsEndpoint.MatsObject;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsInitiator.MatsBackendException;
import com.stolsvik.mats.MatsInitiator.MatsMessageSendException;
import com.stolsvik.mats.websocket.AuthenticationPlugin;
import com.stolsvik.mats.websocket.AuthenticationPlugin.SessionAuthenticator;
import com.stolsvik.mats.websocket.ClusterStoreAndForward;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.CurrentNode;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.DataAccessException;
import com.stolsvik.mats.websocket.MatsSocketServer;

/**
 * @author Endre Stølsvik 2019-11-28 12:17 - http://stolsvik.com/, endre@stolsvik.com
 */
public class DefaultMatsSocketServer implements MatsSocketServer {
    private static final Logger log = LoggerFactory.getLogger(DefaultMatsSocketServer.class);

    private static final String MATS_PREFIX = "MatsSocket";

    private static final String MATS_POSTFIX_REPLY_HANDLER = "replyHandler";

    private static final String MATS_MIDFIX_NEWMESSAGE = "notifyNewMessage";

    private static final String MATS_MIDFIX_NODECONTROL = "nodeControl";

    private static final JavaType TYPE_LIST_OF_MSG = TypeFactory.defaultInstance().constructType(
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
    public static MatsSocketServer createMatsSocketServer(ServerContainer serverContainer,
            MatsFactory matsFactory,
            ClusterStoreAndForward clusterStoreAndForward,
            AuthenticationPlugin authenticationPlugin,
            String websocketPath) {
        String serverName = matsFactory.getFactoryConfig().getAppName();
        return createMatsSocketServer(serverContainer, matsFactory, clusterStoreAndForward, authenticationPlugin,
                serverName, websocketPath);
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
    public static MatsSocketServer createMatsSocketServer(ServerContainer serverContainer,
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

            ThreadLocal<HandshakeRequestResponse> _threadLocal = new ThreadLocal<>();

            @Override
            public boolean checkOrigin(String originHeaderValue) {
                log.info("WebSocket requested, checkOrigin: originHeaderValue: " + originHeaderValue);
                return super.checkOrigin(originHeaderValue);
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
                log.info(" \\- modifyHandshake, keeping the HandshakeRequest and HandshakeResponse for auth.");
                _threadLocal.set(new HandshakeRequestResponse(request, response));
                super.modifyHandshake(sec, request, response);
            }

            @Override
            @SuppressWarnings("unchecked") // The cast to (T) is not dodgy.
            public <T> T getEndpointInstance(Class<T> endpointClass) {
                if (endpointClass != MatsWebSocketInstance.class) {
                    throw new AssertionError("Cannot create Endpoints of type [" + endpointClass.getName()
                            + "]");
                }
                log.info(" \\- Instantiating a MatsSocketEndpoint!");
                HandshakeRequestResponse handshakeRequestResponse = _threadLocal.get();
                _threadLocal.remove();
                return (T) new MatsWebSocketInstance(matsSocketServer, handshakeRequestResponse);
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
    private final ClusterStoreAndForward _clusterStoreAndForward;
    private final ObjectMapper _jackson;
    private final ObjectReader _envelopeListObjectReader;
    private final String _terminatorId_ReplyHandler;
    private final String _terminatorId_NewMessage_NodePrefix;
    private final String _terminatorId_NodeControl_NodePrefix;
    private final MessageToWebSocketForwarder _messageToWebSocketForwarder;
    private final AuthenticationPlugin _authenticationPlugin;

    // In-line init
    private final ConcurrentHashMap<String, MatsSocketEndpointRegistration<?, ?, ?, ?>> _matsSocketEndpointsByMatsSocketEndpointId = new ConcurrentHashMap<>();
    private final Map<String, MatsSocketSession> _activeSessionsByMatsSocketSessionId_x = new LinkedHashMap<>();

    private DefaultMatsSocketServer(MatsFactory matsFactory, ClusterStoreAndForward clusterStoreAndForward,
            String instanceName, AuthenticationPlugin authenticationPlugin) {
        _matsFactory = matsFactory;
        _clusterStoreAndForward = clusterStoreAndForward;
        _jackson = jacksonMapper();
        _authenticationPlugin = authenticationPlugin;
        _envelopeListObjectReader = _jackson.readerFor(TYPE_LIST_OF_MSG);
        // TODO: "Escape" the instanceName.
        _terminatorId_ReplyHandler = MATS_PREFIX + '.' + instanceName + '.' + MATS_POSTFIX_REPLY_HANDLER;
        // TODO: "Escape" the instanceName.
        _terminatorId_NewMessage_NodePrefix = MATS_PREFIX + '.' + instanceName + '.' + MATS_MIDFIX_NEWMESSAGE;
        // TODO: "Escape" the instanceName.
        _terminatorId_NodeControl_NodePrefix = MATS_PREFIX + '.' + instanceName + '.' + MATS_MIDFIX_NODECONTROL;

        int corePoolSize = Math.max(5, matsFactory.getFactoryConfig().getConcurrency() * 4);
        int maximumPoolSize = Math.max(100, matsFactory.getFactoryConfig().getConcurrency() * 20);
        _messageToWebSocketForwarder = new MessageToWebSocketForwarder(this,
                clusterStoreAndForward, corePoolSize, maximumPoolSize);

        // Register our Reply-handler (common on all nodes). Note that the reply often comes in on wrong note: Forward.
        matsFactory.terminator(_terminatorId_ReplyHandler,
                ReplyHandleStateDto.class, MatsObject.class, this::mats_replyHandler);

        // Register node control endpoint (node-specific)
        matsFactory.terminator(terminatorId_NodeControl_ForNode(getMyNodename()),
                NodeControlStateDto.class, MatsObject.class, this::mats_nodeControl);

        // Register our Forward to WebSocket handler (node-specific).
        matsFactory.subscriptionTerminator(terminatorId_NotifyNewMessage_ForNode(getMyNodename()),
                Void.class, String.class, this::mats_notifyNewMessage);
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

    ObjectReader getEnvelopeListObjectReader() {
        return _envelopeListObjectReader;
    }

    MessageToWebSocketForwarder getMessageToWebSocketForwarder() {
        return _messageToWebSocketForwarder;
    }

    String getReplyTerminatorId() {
        return _terminatorId_ReplyHandler;
    }

    AuthenticationPlugin getAuthenticationPlugin() {
        return _authenticationPlugin;
    }

    private String terminatorId_NotifyNewMessage_ForNode(String nodename) {
        // TODO: "Escape" the nodename (just in case)
        return _terminatorId_NewMessage_NodePrefix + '.' + nodename;
    }

    private String terminatorId_NodeControl_ForNode(String nodename) {
        // TODO: "Escape" the nodename (just in case)
        return _terminatorId_NodeControl_NodePrefix + '.' + nodename;
    }

    @Override
    public <I, MI, MR, R> MatsSocketEndpoint<I, MI, MR, R> matsSocketEndpoint(String matsSocketEndpointId,
            Class<I> msIncomingClass, Class<MI> matsIncomingClass, Class<MR> matsReplyClass, Class<R> msReplyClass,
            IncomingAuthorizationAndAdapter<I, MI, R> incomingAuthEval) {
        MatsSocketEndpointRegistration<I, MI, MR, R> matsSocketRegistration = new MatsSocketEndpointRegistration<>(
                matsSocketEndpointId, msIncomingClass, matsIncomingClass, matsReplyClass, msReplyClass,
                incomingAuthEval);
        MatsSocketEndpointRegistration<?, ?, ?, ?> existing = _matsSocketEndpointsByMatsSocketEndpointId.putIfAbsent(
                matsSocketEndpointId, matsSocketRegistration);
        // Assert that there was no existing mapping
        if (existing != null) {
            // -> There was existing mapping - shall not happen.
            throw new IllegalStateException("Cannot register a MatsSocket onto an EndpointId which already is"
                    + " taken, existing: [" + existing + "].");
        }
        return matsSocketRegistration;
    }

    @Override
    public void closeSession(String matsSocketSessionId) {
        log.info("Got 'out-of-band' request to Close Session ["+matsSocketSessionId+"].");
        if (matsSocketSessionId == null) {
            throw new NullPointerException("matsSocketSessionId");
        }
        // :: Check if session is still registered with CSAF
        try {
            Optional<CurrentNode> currentRegisteredNodeForSession = _clusterStoreAndForward
                    .getCurrentRegisteredNodeForSession(matsSocketSessionId);
            // ?: Did we currently have a registered session?!
            if (currentRegisteredNodeForSession.isPresent()) {
                // -> Yes - Send over to that node to kill it both locally there, and in CSAF.
                try {
                    _matsFactory.getDefaultInitiator().initiate(msg -> msg
                            .from(MATS_PREFIX + ".outOfBandSessionClose")
                            .to(terminatorId_NodeControl_ForNode(currentRegisteredNodeForSession.get().getNodename()))
                            .send(new Control_CloseSessionDto(matsSocketSessionId), new NodeControlStateDto(
                                    NodeControlStateDto.CLOSE_SESSION)));
                    return;
                }
                catch (MatsBackendException | MatsMessageSendException e) {
                    log.warn("'Out of band' Close Session, and the MatsSocket Session was still registered in CSAF,"
                            + " but we didn't manage to communicate with MQ. Will ignore this and close it from CSAF"
                            + " anyway.", e);
                }
            }
        }
        catch (DataAccessException e) {
            // TODO: Fix.
            throw new AssertionError("Damn.");
        }

        // E-> No currently registered local session, so close it directly in CSAF here.

        // :: Close it from the CSAF
        try {
            _clusterStoreAndForward.closeSession(matsSocketSessionId);
        }
        catch (DataAccessException e) {
            // TODO: Fix.
            throw new AssertionError("Damn.");
        }
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
    public void stop(int gracefulShutdownMillis) {
        log.info("Asked to shut down MatsSocketServer [" + id(this)
                + "], containing [" + getCurrentLocalRegisteredMatsSocketSessions() + "] sessions.");

        // Hinder further WebSockets connecting to us.
        _stopped = true;

        // Shutdown forwarder thread
        _messageToWebSocketForwarder.shutdown();

        getCurrentLocalRegisteredMatsSocketSessions().forEach(s -> {
            // Close WebSocket
            closeWebSocket(s.getWebSocketSession(), MatsSocketCloseCodes.SERVICE_RESTART,
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

    /**
     * Shall be one instance per socket (i.e. from the docs: "..there will be precisely one endpoint instance per active
     * client connection"), thus there should be 1:1 correlation between this instance and the single Session object for
     * the same cardinality (per client:server connection).
     */
    static class MatsWebSocketInstance extends Endpoint {
        private final DefaultMatsSocketServer _matsSocketServer;
        private final HandshakeRequestResponse _handshakeRequestResponse;

        // Will be set when onOpen is invoked
        private MatsSocketSession _matsSocketSession;

        public MatsWebSocketInstance(DefaultMatsSocketServer matsSocketServer,
                HandshakeRequestResponse handshakeRequestResponse) {
            log.info("Created MatsWebSocketEndpointInstance: " + id(this));
            _matsSocketServer = matsSocketServer;
            _handshakeRequestResponse = handshakeRequestResponse;
        }

        public DefaultMatsSocketServer getMatsSocketServer() {
            return _matsSocketServer;
        }

        @Override
        public void onOpen(Session session, EndpointConfig config) {
            log.info("WebSocket opened, WebSocket SessionId:" + session.getId() + ", endpointConfig:" + config
                    + ", endpointInstance:" + id(this) + ", session:" + id(session));

            // ?: If we are going down, then immediately close it.
            if (_matsSocketServer._stopped) {
                DefaultMatsSocketServer.closeWebSocket(session, MatsSocketCloseCodes.SERVICE_RESTART,
                        "This server is going down, perform a (re)connect to another instance.");
                return;
            }

            // We do not (yet) handle binary messages, so limit that pretty hard.
            session.setMaxBinaryMessageBufferSize(1024);
            // Set low limits for the HELLO message, 20KiB should be plenty even for quite large Oauth2 bearer tokens.
            session.setMaxTextMessageBufferSize(20 * 1024);
            // Set low time to say HELLO after the connect. (The default clients say it immediately on "onopen".)
            session.setMaxIdleTimeout(2500);

            SessionAuthenticator sessionAuthenticator = _matsSocketServer.getAuthenticationPlugin()
                    .newSessionAuthenticator();
            try {
                sessionAuthenticator.evaluateHandshakeRequest(_handshakeRequestResponse._handshakeRequest, session);
            }
            catch (Throwable t) {
                log.error("Got throwable when SessionAuthenticator.evaluateHandshakeRequest(..). Closing WebSocket.",
                        t);
                DefaultMatsSocketServer.closeWebSocket(session, MatsSocketCloseCodes.VIOLATED_POLICY,
                        "Authentication failed upon evaluating Handshake");
                return;
            }

            _matsSocketSession = new MatsSocketSession(_matsSocketServer, session,
                    _handshakeRequestResponse._handshakeRequest, sessionAuthenticator);
            session.addMessageHandler(_matsSocketSession);
        }

        @Override
        public void onError(Session session, Throwable thr) {
            log.info("WebSocket @OnError, MatsSocket SessionId: ["
                    + (_matsSocketSession == null ? "no MatsSocketSession" : _matsSocketSession.getId())
                    + "], WebSocket SessionId:" + session.getId() + ", this:" + id(this), thr);
        }

        @Override
        public void onClose(Session session, CloseReason closeReason) {
            log.info("WebSocket @OnClose, MatsSocket SessionId: ["
                    + (_matsSocketSession == null ? "no MatsSocketSession" : _matsSocketSession.getId())
                    + "], WebSocket SessionId:" + session.getId()
                    + ", code: [" + closeReason.getCloseCode()
                    + "], reason: [" + closeReason.getReasonPhrase() + "], this:" + id(this));
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

    static void closeWebSocket(Session webSocketSession, CloseCode closeCode, String reasonPhrase) {
        log.info("Closing WebSocket SessionId [" + webSocketSession.getId() + "]: code: [" + closeCode
                + "], reason:[" + reasonPhrase + "]");
        try {
            webSocketSession.close(new CloseReason(closeCode, reasonPhrase));
        }
        catch (IOException e) {
            log.warn("Got Exception when trying to close WebSocket Session [" + webSocketSession
                    + "], ignoring.", e);
        }
    }

    /**
     * Registrations of MatsSocketEndpoint - these are the definitions of which targets a MatsSocket client can send
     * messages to, i.e. SEND or REQUEST.
     *
     * @param <I>
     *            incoming MatsSocket DTO
     * @param <MI>
     *            incoming Mats DTO
     * @param <MR>
     *            reply Mats DTO
     * @param <R>
     *            reply MatsSocket DTO
     */
    static class MatsSocketEndpointRegistration<I, MI, MR, R> implements MatsSocketEndpoint<I, MI, MR, R> {
        private final String _matsSocketEndpointId;
        private final Class<I> _msIncomingClass;
        private final Class<MI> _matsIncomingClass;
        private final Class<MR> _matsReplyClass;
        private final Class<R> _msReplyClass;
        private final IncomingAuthorizationAndAdapter<I, MI, R> _incomingAuthEval;

        public MatsSocketEndpointRegistration(String matsSocketEndpointId, Class<I> msIncomingClass,
                Class<MI> matsIncomingClass, Class<MR> matsReplyClass, Class<R> msReplyClass,
                IncomingAuthorizationAndAdapter<I, MI, R> incomingAuthEval) {
            _matsSocketEndpointId = matsSocketEndpointId;
            _msIncomingClass = msIncomingClass;
            _matsIncomingClass = matsIncomingClass;
            _matsReplyClass = matsReplyClass;
            _msReplyClass = msReplyClass;
            _incomingAuthEval = incomingAuthEval;
        }

        private volatile ReplyAdapter<MR, R> _replyAdapter;

        String getMatsSocketEndpointId() {
            return _matsSocketEndpointId;
        }

        public Class<I> getMsIncomingClass() {
            return _msIncomingClass;
        }

        IncomingAuthorizationAndAdapter<I, MI, R> getIncomingAuthEval() {
            return _incomingAuthEval;
        }

        @Override
        public void replyAdapter(ReplyAdapter<MR, R> replyAdapter) {
            _replyAdapter = replyAdapter;
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

    protected static ObjectMapper jacksonMapper() {
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

    private static class NodeControlStateDto {
        static String CLOSE_SESSION = "Close session";
        String type;

        public NodeControlStateDto() {
            /* for Jackson */
        }

        private NodeControlStateDto(String type) {
            this.type = type;
        }
    }

    private static class Control_CloseSessionDto {
        String sessionId;

        public Control_CloseSessionDto() {
            /* for Jackson */
        }

        public Control_CloseSessionDto(String sessionId) {
            this.sessionId = sessionId;
        }
    }

    private void mats_nodeControl(ProcessContext<Void> processContext,
            NodeControlStateDto state, MatsObject incomingMsg) {
        if (NodeControlStateDto.CLOSE_SESSION.equals(state.type)) {
            Control_CloseSessionDto closeSessionDto = incomingMsg.toClass(Control_CloseSessionDto.class);
            node_closeSession(closeSessionDto.sessionId);
        }
    }

    private void node_closeSession(String matsSocketSessionId) {
        // Find local session
        Optional<MatsSocketSession> localMatsSocketSession = getRegisteredLocalMatsSocketSession(matsSocketSessionId);
        // ?: Do we have this session?
        if (localMatsSocketSession.isPresent()) {
            // -> Yes, so close it.
            MatsSocketSession matsSocketSession = localMatsSocketSession.get();
            closeWebSocket(matsSocketSession.getWebSocketSession(), MatsSocketCloseCodes.FORCED_SESSION_CLOSE,
                    "Was asked out-of-band to close session, but it was still connected");
        }

        // :: Close it from the CSAF
        try {
            _clusterStoreAndForward.closeSession(matsSocketSessionId);
        }
        catch (DataAccessException e) {
            // TODO: Fix.
            throw new AssertionError("Damn.");
        }
    }

    static class ReplyHandleStateDto {
        private final String sid;
        private final String cid;
        private final String cmseq;
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
            cmseq = null;
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
            cmseq = messageSequence;
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

    private void mats_replyHandler(ProcessContext<Void> processContext,
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
        msReplyEnvelope.cmseq = state.cmseq;
        msReplyEnvelope.tid = processContext.getTraceId(); // TODO: Chop off last ":xyz", as that is added serverside.
        msReplyEnvelope.cmcts = state.cmcts;
        msReplyEnvelope.cmrts = state.cmrts;
        msReplyEnvelope.mmsts = state.mmsts;
        msReplyEnvelope.mmrrts = matsMessageReplyReceivedTimestamp;
        msReplyEnvelope.mscts = REPLACE_VALUE_TIMESTAMP; // Reply Message to Client Timestamp (not yet determined)
        msReplyEnvelope.cmrnn = state.recnn; // The receiving nodename
        msReplyEnvelope.mmrrnn = getMyNodename(); // The Mats-receiving nodename (this processing)
        msReplyEnvelope.mscnn = REPLACE_VALUE_REPLY_NODENAME; // The replying nodename (not yet determined)
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
                        .to(terminatorId_NotifyNewMessage_ForNode(nodeNameHoldingWebSocket.get().getNodename()))
                        .publish(state.sid);
            });
        }
    }

    private void mats_notifyNewMessage(ProcessContext<Void> processContext,
            Void state, String matsSocketSessionId) {
        Optional<MatsSocketSession> localMatsSocketSession = getRegisteredLocalMatsSocketSession(matsSocketSessionId);
        // ?: If this Session does not exist at this node, we cannot deliver.
        if (!localMatsSocketSession.isPresent()) {
            // -> Someone must have found that we had it, but this must have asynchronously have been deregistered.
            // Do forward of notification - it will handle if we're the node being registered.
            notifyHomeNodeIfAnyAboutNewMessage(matsSocketSessionId);
        }
        else {
            // ----- We have determined that MatsSocketSession has home here
            _messageToWebSocketForwarder.notifyMessageFor(localMatsSocketSession.get());
        }
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
                    .to(terminatorId_NotifyNewMessage_ForNode(currentNode.get().getNodename()))
                    .publish(matsSocketSessionId);
        });
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
