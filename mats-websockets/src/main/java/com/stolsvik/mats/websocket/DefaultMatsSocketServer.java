package com.stolsvik.mats.websocket;

import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

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

/**
 * @author Endre Stølsvik 2019-11-28 12:17 - http://stolsvik.com/, endre@stolsvik.com
 */
public class DefaultMatsSocketServer implements MatsSocketServer {
    private static final Logger log = LoggerFactory.getLogger(DefaultMatsSocketServer.class);

    private static final String REPLY_TERMINATOR_ID_PREFIX = "MatsSockets.replyHandler.";

    public static MatsSocketServer createMatsSocketServer(ServerContainer serverContainer, MatsFactory matsFactory) {
        // TODO: "Escape" the AppName.
        String replyTerminatorId = REPLY_TERMINATOR_ID_PREFIX + matsFactory.getFactoryConfig().getAppName();
        ObjectMapper jackson = jacksonMapper();
        DefaultMatsSocketServer matsSocketServer = new DefaultMatsSocketServer(matsFactory, jackson, replyTerminatorId);

        log.info("Registering MatsSockets' sole WebSocket endpoint.");
        Configurator configurator = new Configurator() {
            @Override
            @SuppressWarnings("unchecked") // The cast to (T) is not dodgy.
            public <T> T getEndpointInstance(Class<T> endpointClass) {
                if (endpointClass != MatsWebSocketInstance.class) {
                    throw new AssertionError("Cannot create Endpoints of type [" + endpointClass.getName()
                            + "]");
                }
                // Generate a MatsSocketSessionId
                String matsSocketSessionId = randomId();
                log.info("Instantiating a MatsSocketEndpoint!");
                return (T) new MatsWebSocketInstance(matsSocketServer, jackson, matsSocketSessionId);
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

    private final ConcurrentHashMap<String, MatsSocketEndpointRegistration<?, ?, ?, ?>> _matsSockets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MatsSocketSession> _activeSessionByMatsSocketSessionId = new ConcurrentHashMap<>();

    private final MatsFactory _matsFactory;
    private final ObjectMapper _jackson;
    private final String _replyTerminatorId;

    private DefaultMatsSocketServer(MatsFactory matsFactory, ObjectMapper jackson, String replyTerminatorId) {
        _matsFactory = matsFactory;
        _jackson = jackson;
        _replyTerminatorId = replyTerminatorId;

        // Register our Reply-handler (common on all nodes - need forwarding to correct node)
        // TODO: FORWARDING BETWEEN NODES!!
        matsFactory.terminator(replyTerminatorId, ReplyHandleStateDto.class, MatsObject.class,
                this::processReply);
    }

    private volatile Function<String, Principal> _authorizationToPrincipalFunction;

    @Override
    public <I, MI, MR, R> MatsSocketEndpoint<I, MI, MR, R> matsSocketEndpoint(String matsSocketEndpointId,
            Class<I> msIncomingClass, Class<MI> matsIncomingClass, Class<MR> matsReplyClass, Class<R> msReplyClass,
            MatsSocketEndpointIncomingAuthEval<I, MI, R> incomingAuthEval) {
        MatsSocketEndpointRegistration<I, MI, MR, R> matsSocketRegistration = new MatsSocketEndpointRegistration<>(
                matsSocketEndpointId, msIncomingClass, matsIncomingClass, matsReplyClass, msReplyClass,
                incomingAuthEval);
        MatsSocketEndpointRegistration existing = _matsSockets.putIfAbsent(matsSocketEndpointId,
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
                + "], containing [" + _activeSessionByMatsSocketSessionId.size() + "] sessions.");
        _activeSessionByMatsSocketSessionId.values().forEach(s -> {
            s.close(CloseCodes.SERVICE_RESTART,
                    "From Server: Server instance is going down, please reconnect.");
        });
    }

    private MatsSocketEndpointRegistration getMatsSocketRegistration(String eid) {
        MatsSocketEndpointRegistration matsSocketRegistration = _matsSockets.get(eid);
        log.info("MatsSocketRegistration for [" + eid + "]: " + matsSocketRegistration);
        if (matsSocketRegistration == null) {
            // TODO / SECURITY: Better handling AND JSON/HTML ENCODING!!
            throw new IllegalArgumentException("Cannot find MatsSocketRegistration for [" + eid + "]");
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

    private void processReply(ProcessContext<Void> processContext, ReplyHandleStateDto state,
            MatsObject incomingMsg) {
        long matsMessageReplyReceivedTimestamp = System.currentTimeMillis();
        // TODO: THIS ASSUMES WE'RE THE ONE HOLDING THIS SESSION!
        MatsSocketSession matsSocketSession = _activeSessionByMatsSocketSessionId.get(state.sid);
        // TODO: TOTALLY not do this!
        if (matsSocketSession == null) {
            log.error("Dropping message on floor for MatsSocketSessionId [" + state.sid
                    + "]: No Session!");
            return;
        }

        // Find the MatsSocketEndpoint for this reply
        MatsSocketEndpointRegistration registration = getMatsSocketRegistration(state.ms_eid);

        Object matsReply = incomingMsg.toClass(registration._matsReplyClass);

        Object msReply;
        if (registration._replyAdapter != null) {
            // TODO: Handle Principal.
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
        msReplyEnvelope.cmcts = state.cmcts;
        msReplyEnvelope.cmrts = state.cmrts;
        msReplyEnvelope.mmsts = state.mmsts;
        msReplyEnvelope.mmrts = matsMessageReplyReceivedTimestamp;
        msReplyEnvelope.rmcts = System.currentTimeMillis();
        msReplyEnvelope.msg = msReply;

        Session session = matsSocketSession._session;
        sendMessage(session, msReplyEnvelope);

    }

    private void sendMessage(Session session, MatsSocketEnvelopeDto msReplyEnvelope) {
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
        String msReplyEnvelopeJson;
        try {
            msReplyEnvelopeJson = _jackson.writeValueAsString(msReplyEnvelope);
        }
        catch (JsonProcessingException e) {
            throw new AssertionError("Huh, couldn't serialize message?!");
        }

        // TODO: Need to think this Async through wrt. to "finishing" the stored message from the storage.
        // TODO: Also, need to consider what to do wrt. timeouts that cannot be transmitted.
        log.info("Sending reply [" + msReplyEnvelopeJson + "]");
        session.getAsyncRemote().sendText(msReplyEnvelopeJson);
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

        public MatsWebSocketInstance(DefaultMatsSocketServer matsSocketServer,
                ObjectMapper jackson, String matsSocketSessionId) {
            log.info("Created MatsWebSocketEndpointInstance: " + id(this));
            _matsSocketServer = matsSocketServer;
            _jackson = jackson;
        }

        @Override
        public void onOpen(Session session, EndpointConfig config) {
            log.info("WebSocket opened, session:" + session.getId() + ", endpointConfig:" + config
                    + ", endpointInstance:" + id(this) + ", session:" + id(session));
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
            // Remove from Local view. (NOTICE: A pure "CLOSE" shall not kill sessionId by itself)
            if (_matsSocketSession != null) {
                _matsSocketServer._activeSessionByMatsSocketSessionId.remove(_matsSocketSession._matsSocketSessionId);
            }
            // TODO: NOTIFY THE FORWARDING MECHANISM THAT WE NO LONGER HAVE THIS MatsSocketSession
        }
    }

    private static class MatsSocketSession implements Whole<String> {
        private static final JavaType LIST_OF_MSG_TYPE = TypeFactory.defaultInstance().constructType(
                new TypeReference<List<MatsSocketEnvelopeDto>>() {
                });

        private final MatsWebSocketInstance _matsWebSocketInstance;
        private final Session _session;

        // Derived
        private final DefaultMatsSocketServer _matsSocketServer;

        // Set
        private String _matsSocketSessionId;
        private String _authorization;
        private Principal _principal;

        public MatsSocketSession(
                MatsWebSocketInstance matsWebSocketInstance, Session session) {
            _matsWebSocketInstance = matsWebSocketInstance;
            _session = session;

            // Derived
            _matsSocketServer = _matsWebSocketInstance._matsSocketServer;
        }

        @Override
        public void onMessage(String message) {
            long clientMessageReceivedTimestamp = System.currentTimeMillis();
            log.info("WebSocket received message:" + message + ", session:" + _session.getId() + ", this:" + id(this));

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
                        MatsSocketSession existingSession = _matsSocketServer._activeSessionByMatsSocketSessionId.get(
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
                            if (existingSession._session.isOpen()) {
                                try {
                                    existingSession._session.close(new CloseReason(CloseCodes.PROTOCOL_ERROR,
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
                        _matsSocketSessionId = randomId();
                    }

                    // Add Session to our active-map
                    // TODO: NOTIFY THE FORWARDING MECHANISM THAT WE NOW HAVE THIS MatsSocketSession
                    _matsSocketServer._activeSessionByMatsSocketSessionId.put(_matsSocketSessionId, this);

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
                    _matsSocketServer.sendMessage(_session, replyEnvelope);
                    continue;
                }

                if ("PING".equals(envelope.t)) {
                    // TODO: HANDLE PING
                    continue;
                }

                if ("CLOSE_SESSION".equals(envelope.t)) {
                    // TODO: DEREGISTER SESSION, NOTIFY FORWARDING MECHANISM
                    _matsSocketServer._activeSessionByMatsSocketSessionId.remove(_matsSocketSessionId);
                    close(CloseCodes.NORMAL_CLOSURE, "From Server: Client said CLOSE_SESSION (" +
                            escape(envelope.desc) + "): Deleting session, closing WebSocket.");
                    continue;
                }

                // ?: We do not accept other messages before authentication
                if (_matsSocketSessionId == null) {
                    // TODO: Handle error
                    throw new AssertionError("Introduce yourself with HELLO. You can also do PING.");
                }

                // ----- We are authenticated.

                // TODO: Handle that ANY message can contain Authorization header!!

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
                        _matsSocketServer.sendMessage(_session, replyEnvelope);
                    }

                    continue;
                }
            }
        }

        private void close(CloseCode closeCode, String reasonPhrase) {
            log.info("Shutting down WebSocket Session [" + _session + "]");
            try {
                _session.close(new CloseReason(closeCode, reasonPhrase));
            }
            catch (IOException e) {
                log.warn("Got Exception when trying to close WebSocket Session [" + _session
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

    private static class Message {
        String msg; // Just to get the entire JSON right here.
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

    private static ObjectMapper jacksonMapper() {
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
            // Trick must be to just consume from the START_OBJECT to the /corresponding/ END_OBJECT.
            return p.readValueAsTree().toString();
        }
    }

    private static String randomId() {
        // Roughly 126 bits of randomness.
        long long1 = Math.abs(ThreadLocalRandom.current().nextLong());
        long long2 = Math.abs(ThreadLocalRandom.current().nextLong());
        return Long.toString(long1, 36) + "_" + Long.toString(long2, 36);
    }

    private static String id(Object x) {
        return x.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(x));
    }

    private static String escape(String string) {
        // TODO: Implement HTML escaping (No messages from us should not go through JSONifying already).
        return string;
    }
}
