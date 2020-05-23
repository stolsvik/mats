package com.stolsvik.mats.websocket.impl;

import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.ACK;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.ACK2;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.AUTH;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.HELLO;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.NACK;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.PING;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.PONG;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.REAUTH;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.REJECT;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.REQUEST;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.RESOLVE;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.RETRY;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.SEND;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.SUB;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.SUB_LOST;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.SUB_NO_AUTH;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.SUB_OK;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.UNSUB;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.WELCOME;

import java.io.IOException;
import java.security.Principal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import javax.websocket.MessageHandler.Whole;
import javax.websocket.RemoteEndpoint.Basic;
import javax.websocket.Session;
import javax.websocket.server.HandshakeRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.stolsvik.mats.websocket.AuthenticationPlugin.AuthenticationResult;
import com.stolsvik.mats.websocket.AuthenticationPlugin.DebugOption;
import com.stolsvik.mats.websocket.AuthenticationPlugin.SessionAuthenticator;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.CurrentNode;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.DataAccessException;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.WrongUserException;
import com.stolsvik.mats.websocket.MatsSocketServer.ActiveMatsSocketSessionDto;
import com.stolsvik.mats.websocket.MatsSocketServer.LiveMatsSocketSession;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketCloseCodes;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto.Direction;
import com.stolsvik.mats.websocket.MatsSocketServer.SessionEstablishedEvent.SessionEstablishedEventType;
import com.stolsvik.mats.websocket.MatsSocketServer.SessionRemovedEvent.SessionRemovedEventType;
import com.stolsvik.mats.websocket.impl.AuthenticationContextImpl.AuthenticationResult_Authenticated;
import com.stolsvik.mats.websocket.impl.AuthenticationContextImpl.AuthenticationResult_InvalidAuthentication;
import com.stolsvik.mats.websocket.impl.AuthenticationContextImpl.AuthenticationResult_StillValid;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer.SessionEstablishedEventImpl;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer.SessionRemovedEventImpl;

/**
 * Effectively the MatsSocketSession, this is the MatsSocket "onMessage" handler.
 *
 * @author Endre Stølsvik 2019-11-28 12:17 - http://stolsvik.com/, endre@stolsvik.com
 */
class MatsSocketSessionAndMessageHandler implements Whole<String>, MatsSocketStatics, LiveMatsSocketSession {
    private static final Logger log = LoggerFactory.getLogger(MatsSocketSessionAndMessageHandler.class);

    // ===== Set in constructor

    // :: From params
    private final DefaultMatsSocketServer _matsSocketServer;
    private final Session _webSocketSession;
    private final String _connectionId;
    // SYNC upon accessing any methods: itself.
    private final SessionAuthenticator _sessionAuthenticator;

    // :: Derived in constructor
    private final Basic _webSocketBasicRemote;
    private final AuthenticationContextImpl _authenticationContext;
    private final ObjectReader _envelopeObjectReader;
    private final ObjectWriter _envelopeObjectWriter;
    private final ObjectReader _envelopeListObjectReader;
    private final ObjectWriter _envelopeListObjectWriter;

    // ===== Set later, updated later

    // Set upon HELLO. Set and read by WebSocket threads - and read by introspection.
    // NOTICE: The MatsSocketSession is not registered with the DefaultMatsSocketServer until HELLO, thus all are set
    // when registered.
    private volatile String _matsSocketSessionId;
    private volatile String _clientLibAndVersions;
    private volatile String _appName;
    private volatile String _appVersion;

    // Set upon Authorization processed.
    // SYNC: All Auth-fields are only modified while holding sync on _sessionAuthenticator, read by WebSocket threads
    // .. - and read by introspection.
    // NOTICE: The MatsSocketSession is not registered with the DefaultMatsSocketServer until HELLO, thus all are set
    // when registered.
    private volatile String _authorization; // nulled upon close
    private volatile Principal _principal; // nulled upon close
    private volatile String _userId; // NEVER nulled

    private volatile String _remoteAddr; // Set by auth, read by introspection
    private volatile String _originatingRemoteAddr; // Set by auth, read by introspection.

    private EnumSet<DebugOption> _authAllowedDebugOptions;
    // CONCURRENCY: Set by WebSocket threads, read by Forwarder.
    private volatile EnumSet<DebugOption> _currentResolvedServerToClientDebugOptions = EnumSet.noneOf(
            DebugOption.class);

    // CONCURRENCY: Set to System.currentTimeMillis() each time (re)evaluated OK by WebSocket threads,
    // .. read by Forwarder - and read by introspection.
    private AtomicLong _lastAuthenticatedTimestamp = new AtomicLong();
    // CONCURRENCY: Set true by Forwarder and false by WebSocket threads (w/ sync on _sessionAuthenticator), read
    // by Forwarder (relying on volatile)
    private volatile boolean _holdOutgoingMessages;

    // CONCURRENCY: Set and read by WebSocket threads, read by Forwarder - and read by introspection.
    private volatile MatsSocketSessionState _state = MatsSocketSessionState.NO_SESSION;

    // CONCURRENCY: Set and read by WebSocket threads, read by OnClose (which probably also is a WebSocket thread).
    private CopyOnWriteArrayList<String> _subscribedTopics = new CopyOnWriteArrayList<>();

    // CONCURRENCY: Set by handleHello, read by introspection
    private volatile long _createdTimestamp;
    private volatile long _sessionEstablishedTimestamp;

    // CONCURRENCY: Set by ping-handling, read by introspection
    private AtomicLong _lastClientPingTimestamp = new AtomicLong();
    // CONCURRENCY: Set by handleSendOrRequestOrReply(..) and Forwarder, read by introspection
    private AtomicLong _lastActivityTimestamp = new AtomicLong();

    private volatile long _sessionLivelinessTimestamp = 0;

    // SYNC upon adding when processing messages, and when reading from introspection: itself.
    private final List<MatsSocketEnvelopeWithMetaDto> _matsSocketEnvelopeWithMetaDtos = new ArrayList<>();

    MatsSocketSessionAndMessageHandler(DefaultMatsSocketServer matsSocketServer, Session webSocketSession,
            String connectionId, HandshakeRequest handshakeRequest, SessionAuthenticator sessionAuthenticator,
            String remoteAddr) {
        _matsSocketServer = matsSocketServer;
        _webSocketSession = webSocketSession;
        _connectionId = connectionId;
        _sessionAuthenticator = sessionAuthenticator;
        // Might be resolved upon onOpen if we have a hack for doing it for this container.
        _remoteAddr = remoteAddr;

        // Derived
        _webSocketBasicRemote = _webSocketSession.getBasicRemote();
        _authenticationContext = new AuthenticationContextImpl(handshakeRequest, this);
        _envelopeObjectReader = _matsSocketServer.getEnvelopeObjectReader();
        _envelopeObjectWriter = _matsSocketServer.getEnvelopeObjectWriter();
        _envelopeListObjectReader = _matsSocketServer.getEnvelopeListObjectReader();
        _envelopeListObjectWriter = _matsSocketServer.getEnvelopeListObjectWriter();
    }

    /**
     * NOTE: There can <i>potentially</i> be multiple instances of {@link MatsSocketSessionAndMessageHandler} with the
     * same Id if we're caught by bad asyncness wrt. one connection dropping and the client immediately reconnecting.
     * The two {@link MatsSocketSessionAndMessageHandler}s would then hey would then have different
     * {@link #getWebSocketSession() WebSocketSessions}, i.e. differing actual connections. One of them would soon
     * realize that is was closed. <b>This Id together with {@link #getConnectionId()} is unique</b>.
     *
     * @return the MatsSocketSessionId that this {@link MatsSocketSessionAndMessageHandler} instance refers to.
     */
    @Override
    public String getMatsSocketSessionId() {
        return _matsSocketSessionId;
    }

    @Override
    public String getUserId() {
        return _userId;
    }

    @Override
    public Instant getSessionCreatedTimestamp() {
        return Instant.ofEpochMilli(_createdTimestamp);
    }

    @Override
    public Instant getSessionLivelinessTimestamp() {
        // NOTE: It is constantly "live", until either DEREGISTERED or CLOSED - in which case that field is set.
        return _sessionLivelinessTimestamp == 0 ? Instant.now() : Instant.ofEpochMilli(_sessionLivelinessTimestamp);
    }

    @Override
    public String getClientLibAndVersions() {
        return _clientLibAndVersions;
    }

    @Override
    public String getAppName() {
        return _appName;
    }

    @Override
    public String getAppVersion() {
        return _appVersion;
    }

    @Override
    public Optional<String> getNodeName() {
        return Optional.of(_matsSocketServer.getMyNodename());
    }

    @Override
    public MatsSocketSessionState getState() {
        return _state;
    }

    @Override
    public Optional<String> getAuthorization() {
        return Optional.ofNullable(_authorization);
    }

    @Override
    public Optional<String> getPrincipalName() {
        return getPrincipal().map(Principal::getName);
    }

    @Override
    public Optional<String> getRemoteAddr() {
        return Optional.ofNullable(_remoteAddr);
    }

    @Override
    public Optional<String> getOriginatingRemoteAddr() {
        return Optional.ofNullable(_originatingRemoteAddr);
    }

    @Override
    public SortedSet<String> getTopicSubscriptions() {
        return new TreeSet<>(_subscribedTopics);
    }

    @Override
    public Instant getSessionEstablishedTimestamp() {
        return Instant.ofEpochMilli(_sessionEstablishedTimestamp);
    }

    @Override
    public Instant getLastAuthenticatedTimestamp() {
        return Instant.ofEpochMilli(_lastAuthenticatedTimestamp.get());
    }

    @Override
    public Instant getLastClientPingTimestamp() {
        return Instant.ofEpochMilli(_lastClientPingTimestamp.get());
    }

    @Override
    public Instant getLastActivityTimestamp() {
        return Instant.ofEpochMilli(_lastActivityTimestamp.get());
    }

    @Override
    public List<MatsSocketEnvelopeWithMetaDto> getLastEnvelopes() {
        synchronized (_matsSocketEnvelopeWithMetaDtos) {
            return new ArrayList<>(_matsSocketEnvelopeWithMetaDtos);
        }
    }

    @Override
    public Session getWebSocketSession() {
        return _webSocketSession;
    }

    @Override
    public Optional<Principal> getPrincipal() {
        return Optional.ofNullable(_principal);
    }

    @Override
    public EnumSet<DebugOption> getAllowedDebugOptions() {
        return _authAllowedDebugOptions;
    }

    @Override
    public ActiveMatsSocketSessionDto toActiveMatsSocketSession() {
        ActiveMatsSocketSessionDto as = new ActiveMatsSocketSessionDto();
        as.id = this.getMatsSocketSessionId();
        as.uid = this.getUserId();
        as.scts = _createdTimestamp;
        as.clv = this.getClientLibAndVersions();
        as.an = this.getAppName();
        as.av = this.getAppVersion();
        as.nn = _matsSocketServer.getMyNodename();
        as.auth = _authorization;
        as.pn = _principal != null ? _principal.getName() : null;
        as.rip = getRemoteAddr().orElse(null);
        as.ocrip = getOriginatingRemoteAddr().orElse(null);
        as.subs = getTopicSubscriptions();
        as.sets = _sessionEstablishedTimestamp;
        as.lauthts = _lastAuthenticatedTimestamp.get();
        as.lcpts = _lastClientPingTimestamp.get();
        as.lactts = _lastActivityTimestamp.get();
        as.msgs = getLastEnvelopes();
        return as;
    }

    public void registerActivityTimestamp(long timestamp) {
        _lastActivityTimestamp.set(timestamp);
    }

    private final Object _webSocketSendSyncObject = new Object();

    void webSocketSendText(String text) throws IOException {
        synchronized (_webSocketSendSyncObject) {
            if (!_state.isHandlesMessages()) {
                log.warn("When about to send message, the WebSocket 'BasicRemote' instance was gone,"
                        + " MatsSocketSessionId [" + _matsSocketSessionId + "], connectionId:[" + _connectionId + "]"
                        + " - probably async close, ignoring. Message:\n" + text);
                return;
            }
            _webSocketBasicRemote.sendText(text);
        }
    }

    /**
     * NOTE: Read JavaDoc of {@link #getMatsSocketSessionId} to understand why this Id is of interest.
     */
    String getConnectionId() {
        return _connectionId;
    }

    /**
     * NOTE: Read JavaDoc of {@link #getMatsSocketSessionId} to understand why this Id is of interest.
     *
     * @return a unique id for this particular {@link MatsSocketSessionAndMessageHandler} - used by the message
     *         forwarders to uniquely identify the MatsSocketSession + WebSocket connection.
     */
    String getInstanceId() {
        return getMatsSocketSessionId() + getConnectionId();
    }

    boolean isSessionEstablished() {
        return _state == MatsSocketSessionState.SESSION_ESTABLISHED;
    }

    boolean isWebSocketSessionOpen() {
        // Volatile, so read it out once
        Session webSocketSession = _webSocketSession;
        // .. then access it twice.
        return webSocketSession != null && webSocketSession.isOpen();
    }

    /**
     * @return the {@link DebugOption}s requested by client intersected with what is allowed for this user ("resolved"),
     *         for Server-initiated messages (Server-to-Client SEND and REQUEST).
     */
    public EnumSet<DebugOption> getCurrentResolvedServerToClientDebugOptions() {
        return _currentResolvedServerToClientDebugOptions;
    }

    // Handling of "REAUTH" - only from WebSocket threads
    private List<MatsSocketEnvelopeWithMetaDto> _heldEnvelopesWaitingForReauth = new ArrayList<>();
    private boolean _askedClientForReauth = false;
    private int _numberOfInformationBearingIncomingWhileWaitingForReauth = 0;

    void setMDC() {
        if (_matsSocketSessionId != null) {
            MDC.put(MDC_SESSION_ID, _matsSocketSessionId);
        }
        if (_principal != null) {
            MDC.put(MDC_PRINCIPAL_NAME, _principal.getName());
        }
        if (_userId != null) {
            MDC.put(MDC_USER_ID, _userId);
        }
        if ((_appName != null) && (_appVersion != null)) {
            MDC.put(MDC_CLIENT_APP_NAME_AND_VERSION, _appName + ";" + _appVersion);
        }
    }

    void recordEnvelopes(List<MatsSocketEnvelopeWithMetaDto> envelopes, long timestamp, Direction direction) {
        // Enrich the "WithMeta" part some more, and handle the DirectJson -> String conversion.
        envelopes.forEach(envelope -> {
            envelope.dir = direction;
            envelope.ts = timestamp;
            envelope.nn = _matsSocketServer.getMyNodename();

            // "Copy out" the JSON directly as a String if this is an outgoing message.
            if (envelope.msg instanceof DirectJson) {
                envelope.msg = ((DirectJson) envelope.msg).getJson();
            }
            // Assert my code: Incoming messages should have envelope.msg == String, outgoing == DirectJson.
            else if ((envelope.msg != null) && !(envelope.msg instanceof String)) {
                log.error("THIS IS AN ERROR! If the envelope.msg field is set, it should be a String or DirectJson,"
                        + " not [" + envelope.msg.getClass().getName() + "].",
                        new RuntimeException("Debug Stacktrace!"));
            }
        });
        // Store the envelopes in the "last few" list.
        synchronized (_matsSocketEnvelopeWithMetaDtos) {
            _matsSocketEnvelopeWithMetaDtos.addAll(envelopes);
            while (_matsSocketEnvelopeWithMetaDtos.size() > MAX_NUMBER_OF_RECORDED_ENVELOPES_PER_SESSION) {
                _matsSocketEnvelopeWithMetaDtos.remove(0);
            }
        }
        // Invoke any MessageEvent listeners.
        _matsSocketServer.invokeMessageEventListeners(this, envelopes);
    }

    @Override
    public void onMessage(String message) {
        try { // try-finally: MDC.clear();
            setMDC();
            long receivedTimestamp = System.currentTimeMillis();

            // ?: Do we accept messages?
            if (!_state.isHandlesMessages()) {
                // -> No, so ignore message.
                log.info("WebSocket @OnMessage: WebSocket received message on MatsSocketSession with"
                        + " non-message-accepting state [" + _state + "], MatsSocketSessionId ["
                        + _matsSocketSessionId + "], connectionId:[" + _connectionId + "], size:[" + message.length()
                        + " cp] this:" + DefaultMatsSocketServer.id(this) + "], ignoring, msg: " + message);
                return;
            }

            // E-> Not closed, process message (containing MatsSocket envelope(s)).

            log.info("WebSocket @OnMessage: WebSocket received message for MatsSocketSessionId [" + _matsSocketSessionId
                    + "], connectionId:[" + _connectionId + "], size:[" + message.length()
                    + " cp] this:" + DefaultMatsSocketServer.id(this));

            // :: Parse the message into MatsSocket envelopes
            List<MatsSocketEnvelopeWithMetaDto> envelopes;
            try {
                envelopes = _envelopeListObjectReader.readValue(message);
            }
            catch (IOException e) {
                log.error("Could not parse WebSocket message into MatsSocket envelope(s).", e);
                closeSessionAndWebSocketWithProtocolError("Could not parse message into MatsSocket envelope(s)");
                return;
            }

            if (log.isDebugEnabled()) log.debug("Messages: " + envelopes);

            // :: 0. Look for request-debug in any of the messages

            // NOTE! This is what will be used for Server initiated messages. For Client initiated, each message itself
            // chooses which debug options it wants for the full processing of itself (incl. Reply).
            for (MatsSocketEnvelopeWithMetaDto envelope : envelopes) {
                // ?: Pick out any "Request Debug" flags
                if (envelope.rd != null) {
                    // -> Yes, there was an "Request Debug" sent along with this message
                    EnumSet<DebugOption> requestedDebugOptions = DebugOption.enumSetOf(envelope.rd);
                    // Intersect this with allowed DebugOptions
                    requestedDebugOptions.retainAll(_authAllowedDebugOptions);
                    // Store the result as new Server-to-Client DebugOptions
                    _currentResolvedServerToClientDebugOptions = requestedDebugOptions;
                    // NOTE: We do not "skip out" with 'continue', as we want to use the latest in the pipeline if any.
                }
            }

            // :: 1a. Look for Authorization header in any of the messages

            // NOTE! Authorization header can come with ANY message!
            String newAuthorization = null;
            for (MatsSocketEnvelopeWithMetaDto envelope : envelopes) {
                // ?: Pick out any Authorization header, i.e. the auth-string - it can come in any message.
                if (envelope.auth != null) {
                    // -> Yes, there was an authorization header sent along with this message
                    newAuthorization = envelope.auth;
                    log.info("Found authorization header in message of type [" + envelope.t + "]");
                    // Notice: No 'break', as we want to go through all messages and find the latest auth header.
                }
            }
            // :: 1b. Remove any *specific* AUTH message (it ONLY contains the 'auth' property, handled above).
            // NOTE: the 'auth' property can come on ANY message, but AUTH is a special message to send 'auth' with.
            envelopes.removeIf(envelope -> {
                boolean remove = envelope.t == AUTH;
                if (remove) {
                    recordEnvelopes(Collections.singletonList(envelope), receivedTimestamp, Direction.C2S);
                }
                return remove;
            });

            // :: 2a. Handle PINGs (send PONG asap).

            for (Iterator<MatsSocketEnvelopeWithMetaDto> it = envelopes.iterator(); it.hasNext();) {
                MatsSocketEnvelopeWithMetaDto envelope = it.next();
                // ?: Is this message a PING?
                if (envelope.t == PING) {
                    // -> Yes, so handle it.
                    // Record start of handling
                    long nanosStart = System.nanoTime();
                    // Record received Envelope
                    recordEnvelopes(Collections.singletonList(envelope), receivedTimestamp, Direction.C2S);

                    // Remove it from the pipeline
                    it.remove();
                    // Assert that we've had HELLO already processed
                    // NOTICE! We will handle PINGs without valid Authorization, but only if we've already established
                    // Session, as checked by seeing if we've processed HELLO
                    if (!isSessionEstablished()) {
                        closeSessionAndWebSocketWithProtocolError(
                                "Cannot process PING before HELLO and session established");
                        return;
                    }
                    // :: Update PING timestamp
                    _lastClientPingTimestamp.set(receivedTimestamp);

                    // :: Create PONG message
                    MatsSocketEnvelopeWithMetaDto replyEnvelope = new MatsSocketEnvelopeWithMetaDto();
                    replyEnvelope.t = PONG;
                    replyEnvelope.x = envelope.x;

                    // Pack the PONG over to client ASAP.
                    // TODO: Consider doing this async, probably with MessageToWebSocketForwarder
                    List<MatsSocketEnvelopeWithMetaDto> replySingleton = Collections.singletonList(
                            replyEnvelope);
                    try {
                        String json = _envelopeListObjectWriter.writeValueAsString(replySingleton);
                        webSocketSendText(json);
                    }
                    catch (JsonProcessingException e) {
                        throw new AssertionError("Huh, couldn't serialize message?!", e);
                    }
                    catch (IOException e) {
                        // TODO: Handle!
                        throw new AssertionError("Hot damn.", e);
                    }
                    // Record sent Envelope
                    replyEnvelope.rttm = msSince(nanosStart);
                    recordEnvelopes(Collections.singletonList(replyEnvelope), System.currentTimeMillis(),
                            Direction.S2C);
                }
            }

            // :: 2b. Handle PONGs

            for (Iterator<MatsSocketEnvelopeWithMetaDto> it = envelopes.iterator(); it.hasNext();) {
                MatsSocketEnvelopeWithMetaDto envelope = it.next();
                // ?: Is this message a PING?
                if (envelope.t == PONG) {
                    // -> Yes, so handle it.
                    // Remove it from the pipeline
                    it.remove();
                    // Record received Envelope.
                    recordEnvelopes(Collections.singletonList(envelope), receivedTimestamp, Direction.C2S);
                    // Assert that we've had HELLO already processed
                    // NOTICE! We will handle PONGs without valid Authorization, but only if we've already established
                    // Session, as checked by seeing if we've processed HELLO
                    if (!isSessionEstablished()) {
                        closeSessionAndWebSocketWithProtocolError(
                                "Cannot process PONG before HELLO and session established");
                        return;
                    }

                    // TODO: Handle PONGs (well, first the server actually needs to send PINGs..!)
                }
            }

            // :: 3a. Handle ACKs and NACKs - i.e. Client tells us that it has received an information bearing message.

            // ACK or NACK from Client denotes that we can delete it from outbox on our side.
            // .. we respond with ACK2 to these
            List<String> clientAckIds = null;
            for (Iterator<MatsSocketEnvelopeWithMetaDto> it = envelopes.iterator(); it.hasNext();) {
                MatsSocketEnvelopeWithMetaDto envelope = it.next();

                // ?: Is this a ACK or NACK for a message from us?
                if ((envelope.t == ACK) || (envelope.t == NACK)) {
                    // -> Yes - remove it, we're handling it now.
                    it.remove();
                    // Record received Envelope
                    recordEnvelopes(Collections.singletonList(envelope), receivedTimestamp, Direction.C2S);
                    // Assert correctness
                    if ((envelope.smid == null) && (envelope.ids == null)) {
                        closeSessionAndWebSocketWithProtocolError("Received " + envelope.t
                                + " message with missing both 'smid' and 'ids'.");
                        return;
                    }
                    if ((envelope.smid != null) && (envelope.ids != null)) {
                        closeSessionAndWebSocketWithProtocolError("Received " + envelope.t
                                + " message with both 'smid' and 'ids' - only set one!");
                        return;
                    }
                    // Assert that we've had HELLO already processed
                    // NOTICE! We will handle ACK/NACKs without valid Authorization, but only if we've already
                    // established Session, as checked by seeing if we've processed HELLO
                    if (!isSessionEstablished()) {
                        closeSessionAndWebSocketWithProtocolError(
                                "Cannot process " + envelope.t + " before HELLO and session established");
                        return;
                    }
                    if (clientAckIds == null) {
                        clientAckIds = new ArrayList<>();
                    }
                    if (envelope.smid != null) {
                        clientAckIds.add(envelope.smid);
                    }
                    if (envelope.ids != null) {
                        clientAckIds.addAll(envelope.ids);
                    }
                }
            }
            // .. now actually act on the ACK and NACKs (delete from outbox, then Reply with ACK2)
            // TODO: Make this a bit more nifty, putting such Ids on a queue of sorts, finishing async
            if (clientAckIds != null) {
                long nanosStart = System.nanoTime();
                log.debug("Got ACK/NACK for messages " + clientAckIds + ".");
                try {
                    _matsSocketServer.getClusterStoreAndForward().outboxMessagesComplete(_matsSocketSessionId,
                            clientAckIds);
                }
                catch (DataAccessException e) {
                    // TODO: Make self-healer thingy.
                    log.warn("Got problems when trying to mark messages as complete. Ignoring, hoping for miracles.");
                }

                // Send "ACK2", i.e. "I've now deleted these Ids from my outbox".
                if (clientAckIds.isEmpty()) {
                    // This should not happen, the client should not send an empty list.
                    closeSessionAndWebSocketWithProtocolError("An empty list of ackids was sent");
                    return;
                }
                _matsSocketServer.getWebSocketOutgoingAcks().sendAck2s(_matsSocketSessionId, clientAckIds);

                // Record sent Envelope
            }

            // :: 3b. Handle ACK2's - which is that the Client has received an ACK or NACK from us.

            // ACK2 from the Client is message to Server that the "client has deleted an information-bearing message
            // from his outbox", denoting that we can delete it from inbox on our side
            List<String> clientAck2Ids = null;
            for (Iterator<MatsSocketEnvelopeWithMetaDto> it = envelopes.iterator(); it.hasNext();) {
                MatsSocketEnvelopeWithMetaDto envelope = it.next();
                // ?: Is this a ACK2 for a ACK/NACK from us?
                if (envelope.t == ACK2) {
                    it.remove();
                    // Record received Envelope
                    recordEnvelopes(Collections.singletonList(envelope), receivedTimestamp, Direction.C2S);
                    // Assert that we've had HELLO already processed
                    // NOTICE! We will handle ACK2s without valid Authorization, but only if we've already established
                    // Session, as checked by seeing if we've processed HELLO
                    if (!isSessionEstablished()) {
                        closeSessionAndWebSocketWithProtocolError(
                                "Cannot process ACK2 before HELLO and session established");
                        return;
                    }
                    if ((envelope.cmid == null) && (envelope.ids == null)) {
                        closeSessionAndWebSocketWithProtocolError("Received ACK2 envelope with missing 'cmid'"
                                + " or 'ids'.");
                        return;
                    }
                    if (clientAck2Ids == null) {
                        clientAck2Ids = new ArrayList<>();
                    }
                    if (envelope.cmid != null) {
                        clientAck2Ids.add(envelope.cmid);
                    }
                    if (envelope.ids != null) {
                        clientAck2Ids.addAll(envelope.ids);
                    }
                }
            }
            // .. now actually act on the ACK2s
            // (delete from our inbox - we do not need it anymore to guard for double deliveries)
            // TODO: Make this a bit more nifty, putting such Ids on a queue of sorts, finishing async
            if (clientAck2Ids != null) {
                log.debug("Got ACK2 for messages " + clientAck2Ids + ".");
                try {
                    _matsSocketServer.getClusterStoreAndForward().deleteMessageIdsFromInbox(_matsSocketSessionId,
                            clientAck2Ids);
                }
                catch (DataAccessException e) {
                    // TODO: Make self-healer thingy.
                    log.warn("Got problems when trying to delete Messages from Inbox."
                            + " Ignoring, not that big of a deal.");
                }
            }

            // :: 4. Evaluate whether we should go further.

            // ?: Do we have any more messages in pipeline, any held messages, or have we gotten new Authorization?
            if (envelopes.isEmpty() && _heldEnvelopesWaitingForReauth.isEmpty() && (newAuthorization == null)) {
                // -> No messages, no held, and no new auth. Thus, all messages that were here was control messages.
                // Return, without considering valid existing authentication. Again: It is allowed to send control
                // messages (in particular PING), and thus keep connection open, without having current valid
                // authorization. The rationale here is that otherwise we'd have to /continuously/ ask an
                // OAuth/OIDC/token server about new tokens, which probably would keep the authentication session open.
                // With the present logic, the only thing you can do without authorization, is keeping the connection
                // actually open. Once you try to send any information-bearing messages, or SUB/UNSUB, authentication
                // check will immediately kick in - which either will allow you to pass, or ask for REAUTH.
                return;
            }

            // ----- We have messages in pipeline that needs Authentication, OR we have new Authorization value.

            // === AUTHENTICATION! On every pipeline of messages, we re-evaluate authentication

            // :: 5. Evaluate Authentication by Authorization header - must be done before HELLO handling
            AuthenticationHandlingResult authenticationHandlingResult = doAuthentication(newAuthorization);

            // ?: Was the AuthenticationHandlingResult == BAD, indicating that initial auth went bad?
            if (authenticationHandlingResult == AuthenticationHandlingResult.BAD) {
                // -> Yes, BAD - and then doAuthentication() has already closed session and websocket and the lot.
                return;
            }

            // :: 6. Handle "Hold of messages" if REAUTH

            // ?: Was the AuthenticationHandlingResult == REAUTH, indicating that the token was expired?
            if (authenticationHandlingResult == AuthenticationHandlingResult.REAUTH) {
                // -> Yes, We are not allowed to process further messages until better auth present.
                // NOTE: There shall only be information-bearing messages left in the pipeline now.
                // NOTE: Also, it cannot be HELLO, as initial auth fail would have given BAD, not REAUTH.

                // Keep count of how many information bearing messages we've gotten without replying to any.
                _numberOfInformationBearingIncomingWhileWaitingForReauth += envelopes.size();

                // Make reply envelope list
                List<MatsSocketEnvelopeWithMetaDto> replyEnvelopes = new ArrayList<>();

                // ?: Have we already asked for REAUTH?
                if (!_askedClientForReauth) {
                    // -> No, not asked for REAUTH, so do it now.
                    MatsSocketEnvelopeWithMetaDto reauthEnvelope = new MatsSocketEnvelopeWithMetaDto();
                    reauthEnvelope.t = REAUTH;
                    // Record the S2C REAUTH Envelope
                    recordEnvelopes(Collections.singletonList(reauthEnvelope), System.currentTimeMillis(),
                            Direction.S2C);
                    replyEnvelopes.add(reauthEnvelope);
                    // We've now asked Client for REAUTH, so don't do it again until he has given us new.
                    _askedClientForReauth = true;
                }
                else {
                    // ?: Is the number of info-bearing messages processed without forward motion too high?
                    if (_numberOfInformationBearingIncomingWhileWaitingForReauth > 500) {
                        // -> Yes, so close it off.
                        /*
                         * What has likely happened here is that we've sent REAUTH, but the Client screwed this up, and
                         * has not given us any new auth, but keeps doing RETRY when we ask for it. This will go on
                         * forever - and without new AUTH, we will just keep answering RETRY. So, if this number of
                         * information bearing messages has gone way overboard, then shut things down. PS: The counter
                         * is reset after a processing round.
                         */
                        closeSessionAndWebSocketWithPolicyViolation("Too many information bearing messages."
                                + " Server has requested REAUTH, no answer.");
                        return;
                    }
                }

                // :: We will "hold" messages while waiting for REAUTH
                /*
                 * The reason for this holding stuff is not only out of love for the bandwidth, but also because if the
                 * reason for the AUTH being old when we evaluated it was that the message sent over was massive over a
                 * slow line, replying "REAUTH" and "RETRY" to the massive message could potentially lead to the same
                 * happening right away again. So instead, we hold the big message, ask for REAUTH, and then process it
                 * when the auth comes in.
                 */
                // :: First hold all SUB and UNSUB, as we cannot reply "RETRY" to these
                for (Iterator<MatsSocketEnvelopeWithMetaDto> it = envelopes.iterator(); it.hasNext();) {
                    MatsSocketEnvelopeWithMetaDto envelope = it.next();
                    if ((envelope.t == SUB) || (envelope.t == UNSUB)) {
                        // ?: Is the topicId too long?
                        if (envelope.eid.length() > MAX_LENGTH_OF_TOPIC_NAME) {
                            closeSessionAndWebSocketWithPolicyViolation("TopicId length > " + MAX_LENGTH_OF_TOPIC_NAME);
                        }
                        _heldEnvelopesWaitingForReauth.add(envelope);
                        // :: A DOS-preventive measure:
                        // ?: Do we have WAY too many held messages
                        // (this can only happen due to SUB & UNSUB, because we stop at 100 below)
                        if (_heldEnvelopesWaitingForReauth.size() > 3000) {
                            // -> Yes, and this is a ridiculous amount of SUBs and UNSUBs
                            closeSessionAndWebSocketWithPolicyViolation("Way too many held messages.");
                        }
                        // Now remove it from incoming list (since we hold it!)
                        it.remove();
                    }
                }
                // :: There should only be information bearing messages left.
                // :: Assert that we have 'cmid' on all the remaining messages, as they shall all be information bearing
                // messages from the Client, and thus contain 'cmid'.
                for (MatsSocketEnvelopeWithMetaDto envelope : envelopes) {
                    if (envelope.cmid == null) {
                        closeSessionAndWebSocketWithProtocolError("Missing 'cmid' on message of type ["
                                + envelope.t + "]");
                        return;
                    }
                }
                // :: Now hold all messages until we pass some rather arbitrary size limits
                for (Iterator<MatsSocketEnvelopeWithMetaDto> it = envelopes.iterator(); it.hasNext();) {
                    // :: Do some DOS-preventive measures:
                    // ?: Do we have more than some limit of held messages?
                    if (_heldEnvelopesWaitingForReauth.size() > MAX_NUMBER_OF_HELD_ENVELOPES_PER_SESSION) {
                        // -> Yes, over number-limit, so then we reply "RETRY" to the rest.
                        break;
                    }
                    // ?: Is the size of current held messages more than some limit?
                    int currentSizeOfHeld = _heldEnvelopesWaitingForReauth.stream()
                            .mapToInt(e -> e.msg instanceof String ? ((String) e.msg).length() : 0)
                            .sum();
                    if (currentSizeOfHeld > MAX_SIZE_OF_HELD_ENVELOPE_MSGS) {
                        // -> Yes, over size-limit, so then we reply "RETRY" to the rest.
                        /*
                         * NOTE! This will lead to at least one message being held, since if under limit, we add
                         * unconditionally, and the overrun will be caught in the next looping (this one single is
                         * however also limited by the WebSocket per-message limit set up in HELLO). Therefore, if the
                         * Client has a dozen giga messages, where each of them ends up in this bad situation where the
                         * auth is expired before we get to evaluate it, each time at least one more message should be
                         * processed.
                         */
                        break;
                    }

                    // E-> There IS room for this message to be held - so hold it!
                    _heldEnvelopesWaitingForReauth.add(it.next());
                    // Now remove it from incoming list (since we hold it!) - remaining will get RETRY, then done.
                    it.remove();
                }

                // Any remaining incoming message will get a RETRY back to Client
                for (MatsSocketEnvelopeWithMetaDto envelopeToRetry : envelopes) {
                    MatsSocketEnvelopeWithMetaDto retryReplyEnvelope = new MatsSocketEnvelopeWithMetaDto();
                    retryReplyEnvelope.t = RETRY;
                    retryReplyEnvelope.cmid = envelopeToRetry.cmid;
                    replyEnvelopes.add(retryReplyEnvelope);
                }

                try {
                    String json = _envelopeListObjectWriter.writeValueAsString(replyEnvelopes);
                    webSocketSendText(json);
                }
                catch (JsonProcessingException e) {
                    throw new AssertionError("Huh, couldn't serialize message?!", e);
                }
                catch (IOException e) {
                    // TODO: Handle!
                    // TODO: At least store last messageSequenceId that we had ASAP. Maybe do it async?!
                    throw new AssertionError("Hot damn.", e);
                }
                // We're finished handling all Envelopes in incoming WebSocket Message that was blocked by REAUTH
                return;
            }

            // ---- Not BAD nor REAUTH AuthenticationHandlingResult

            // ?: .. okay, thu MUST then be OK Auth Result - just assert this
            if (authenticationHandlingResult != AuthenticationHandlingResult.OK) {
                log.error("Unknown AuthenticationHandlingResult [" + authenticationHandlingResult
                        + "], what on earth is this?!");
                closeSessionAndWebSocketWithProtocolError("Internal Server error.");
                return;
            }

            // :: 7. Are we authenticated? (I.e. Authorization Header must sent along in the very first pipeline..)

            if (_authorization == null) {
                log.error("We have not got Authorization header!");
                closeSessionAndWebSocketWithPolicyViolation("Missing Authorization header");
                return;
            }
            if (_principal == null) {
                log.error("We have not got Principal!");
                closeSessionAndWebSocketWithPolicyViolation("Missing Principal");
                return;
            }
            if (_userId == null) {
                log.error("We have not got UserId!");
                closeSessionAndWebSocketWithPolicyViolation("Missing UserId");
                return;
            }

            // :: 8. look for a HELLO message

            // (should be first/alone, but we will reply to it immediately even if part of pipeline).
            for (Iterator<MatsSocketEnvelopeWithMetaDto> it = envelopes.iterator(); it.hasNext();) {
                MatsSocketEnvelopeWithMetaDto envelope = it.next();
                if (envelope.t == HELLO) {
                    try { // try-finally: MDC.remove(..)
                        MDC.put(MDC_MESSAGE_TYPE, envelope.t.name());
                        // Remove this HELLO envelope from pipeline
                        it.remove();
                        // ?: Have we processed HELLO before for this WebSocket connection, i.e. already ACTIVE?
                        if (isSessionEstablished()) {
                            // -> Yes, and this is not according to protocol.
                            closeSessionAndWebSocketWithPolicyViolation("Shall only receive HELLO once per MatsSocket"
                                    + " WebSocket connection.");
                            return;
                        }
                        // E-> First time we see HELLO
                        // :: Handle the HELLO
                        boolean handleHelloOk = handleHello(receivedTimestamp, envelope);
                        // ?: Did the HELLO go OK?
                        if (!handleHelloOk) {
                            // -> No, not OK - handleHello(..) has already closed session and websocket and the lot.
                            return;
                        }
                    }
                    finally {
                        MDC.remove(MDC_MESSAGE_TYPE);
                    }
                    break;
                }
            }

            // :: 9. Assert state: All present: SessionId, Authorization, Principal and UserId.

            if ((_matsSocketSessionId == null)
                    || (_authorization == null)
                    || (_principal == null)
                    || (_userId == null)) {
                closeSessionAndWebSocketWithPolicyViolation("Illegal state at checkpoint.");
                return;
            }

            if (!isSessionEstablished()) {
                closeSessionAndWebSocketWithPolicyViolation("SessionState != ACTIVE.");
                return;
            }

            // MDC: Just ensure that all MDC stuff is set now after HELLO
            setMDC();

            // :: 10. Now go through and handle all the rest of the messages: information bearing, or SUB/UNSUB

            // First drain the held messages into the now-being-processed list
            if (!_heldEnvelopesWaitingForReauth.isEmpty()) {
                // Add the held ones in front..
                List<MatsSocketEnvelopeWithMetaDto> newList = new ArrayList<>(_heldEnvelopesWaitingForReauth);
                // .. now clear the held-list
                _heldEnvelopesWaitingForReauth.clear();
                // .. then add the existing envelopes.
                newList.addAll(envelopes);
                // .. finally use this instead of the one we had.
                envelopes = newList;
            }

            // .. then go through all incoming Envelopes (both held, and from this pipeline).
            List<MatsSocketEnvelopeWithMetaDto> replyEnvelopes = new ArrayList<>();
            for (MatsSocketEnvelopeWithMetaDto envelope : envelopes) {
                try { // try-finally: MDC.remove..
                    MDC.put(MDC_MESSAGE_TYPE, envelope.t.name());
                    if (envelope.tid != null) {
                        MDC.put(MDC_TRACE_ID, envelope.tid);
                    }

                    // :: These are the "information bearing messages" client-to-server, which we need to put in inbox
                    // so that we can catch double-deliveries of same message.

                    // ?: Is this a incoming SEND or REQUEST, or Reply RESOLVE or REJECT to us?
                    if ((envelope.t == SEND) || (envelope.t == REQUEST)
                            || (envelope.t == RESOLVE) || (envelope.t == REJECT)) {
                        boolean handledOk = handleSendOrRequestOrReply(receivedTimestamp,
                                envelope);
                        // ?: Was this OK?
                        if (!handledOk) {
                            // -> No, badness ensued - handling has already closed session and websocket and the lot.
                            return;
                        }
                        // The message is handled, so go to next message.
                        continue;
                    }

                    // ?: Is this a SUB?
                    else if (envelope.t == SUB) {
                        handleSub(receivedTimestamp, replyEnvelopes, envelope);
                        // The message is handled, so go to next message.
                        continue;
                    }
                    // ?: Is this an UNSUB?
                    else if (envelope.t == UNSUB) {
                        handleUnsub(envelope);
                        // The message is handled, so go to next message.
                        continue;
                    }

                    // :: Unknown message..

                    else {
                        // -> Unknown message: We're not very lenient her - CLOSE SESSION AND CONNECTION!
                        log.error("Got an unknown message type [" + envelope.t
                                + "] from client. Answering by closing connection with PROTOCOL_ERROR.");
                        closeSessionAndWebSocketWithProtocolError("Received unknown message type.");
                        return;
                    }
                }
                finally {
                    MDC.remove(MDC_MESSAGE_TYPE);
                    MDC.remove(MDC_TRACE_ID);
                }
            }

            // ----- All messages handled.

            // Now we have forward progress, so reset this to 0.
            _numberOfInformationBearingIncomingWhileWaitingForReauth = 0;

            // Record these incoming Envelopes
            recordEnvelopes(envelopes, receivedTimestamp, Direction.C2S);

            // :: 10. Send any replies

            if (replyEnvelopes.size() > 0) {
                // TODO: Use the MessageToWebSocketForwarder for this.
                try {
                    String json = _envelopeListObjectWriter.writeValueAsString(replyEnvelopes);
                    webSocketSendText(json);
                }
                catch (JsonProcessingException e) {
                    throw new AssertionError("Huh, couldn't serialize message?!", e);
                }
                catch (IOException e) {
                    // TODO: Handle!
                    throw new AssertionError("Hot damn.", e);
                }

                // Record these replied Envelopes
                recordEnvelopes(replyEnvelopes, System.currentTimeMillis(), Direction.S2C);
            }
        }
        finally {
            MDC.clear();
        }
    }

    /**
     * Closes session:
     * <ul>
     * <li>Marks closed</li>
     * <li>Nulls out important fields, to disarm this instance.</li>
     * <li>Unsubscribe all topics.</li>
     * <li>Deregister from local node</li>
     * <li>Close Session in CSAF</li>
     * <li>FINALLY: Notifies SessionRemovedEventListeners</li>
     * </ul>
     */
    void closeSession(Integer closeCode, String reason) {
        // ?: Are we already DEREGISTERD or CLOSED?
        if (!_state.isHandlesMessages()) {
            // Already deregistered or closed.
            log.info("session.closeSession(): .. but was already in a state where not accepting messages ["
                    + _state + "].");
            return;
        }
        // Mark closed, disarm this instance (auth-null), unsubscribe all topics, local deregister
        commonDeregisterAndClose(MatsSocketSessionState.CLOSED, closeCode, reason);

        // Close CSAF session
        if (_matsSocketSessionId != null) {
            try {
                _matsSocketServer.getClusterStoreAndForward().closeSession(_matsSocketSessionId);
            }
            catch (DataAccessException e) {
                log.warn("Could not close session in CSAF. This is unfortunate, as it then is technically possible to"
                        + " still reconnect to the session while this evidently was not the intention"
                        + " (only the same user can reconnect, though). However, the session scavenger"
                        + " will clean this lingering session out after some hours.", e);
            }
            // :: Invoke the SessionRemovedEvent listeners - AFTER it is removed from MatsSocketServer and CSAF
            _matsSocketServer.invokeSessionRemovedEventListeners(new SessionRemovedEventImpl(
                    SessionRemovedEventType.CLOSE, _matsSocketSessionId, closeCode, reason));
        }
    }

    void closeSessionAndWebSocketWithProtocolError(String reason) {
        closeSessionAndWebSocket(MatsSocketCloseCodes.PROTOCOL_ERROR, reason);
    }

    void closeSessionAndWebSocketWithPolicyViolation(String reason) {
        closeSessionAndWebSocket(MatsSocketCloseCodes.VIOLATED_POLICY, reason);
    }

    void closeSessionAndWebSocket(MatsSocketCloseCodes closeCode, String reason) {
        // NOTE: FIRST deregister, so that we do not get more messages our way, THEN close WebSocket, which might lag.

        // Perform the instance close
        // Note: This might end up being invoked twice, since the above WebSocket close will invoke onClose
        // However, such double invocation is handled in close-method
        closeSession(closeCode.getCode(), reason);

        // :: Close the actual WebSocket
        DefaultMatsSocketServer.closeWebSocket(_webSocketSession, closeCode, reason);
    }

    /**
     * Deregisters session: Same as {@link #closeSession(Integer, String)}, only where it says "close", it now says
     * "deregisters".
     */
    void deregisterSession(Integer closeCode, String reason) {
        // ?: Are we already DEREGISTERD or CLOSED?
        if (!_state.isHandlesMessages()) {
            // Already deregistered or closed.
            log.info("session.deregisterSession(): .. but was already in a state where not accepting messages ["
                    + _state + "].");
            return;
        }
        // Mark closed, disarm this instance (auth-null), unsubscribe all topics, local deregister
        commonDeregisterAndClose(MatsSocketSessionState.DEREGISTERED, closeCode, reason);

        // Deregister CSAF session
        if (_matsSocketSessionId != null) {
            try {
                _matsSocketServer.getClusterStoreAndForward().deregisterSessionFromThisNode(_matsSocketSessionId,
                        _connectionId);
            }
            catch (DataAccessException e) {
                log.warn("Could not deregister MatsSocketSessionId [" + _matsSocketSessionId
                        + "] from CSAF, ignoring.", e);
            }
            // :: Invoke the SessionRemovedEvent listeners - AFTER it is removed from MatsSocketServer and CSAF
            _matsSocketServer.invokeSessionRemovedEventListeners(new SessionRemovedEventImpl(
                    SessionRemovedEventType.DEREGISTER, _matsSocketSessionId, closeCode, reason));
        }
    }

    void deregisterSessionAndCloseWebSocket(MatsSocketCloseCodes closeCode, String reason) {
        // NOTE: FIRST deregister, so that we do not get more messages our way, THEN close WebSocket, which might lag.

        // Perform the instance deregister
        // Note: This might end up being invoked twice, since the above WebSocket close will invoke onClose
        // However, such double invocation is handled in deregister-method
        deregisterSession(closeCode.getCode(), reason);

        // Close WebSocket
        DefaultMatsSocketServer.closeWebSocket(_webSocketSession, closeCode, reason);
    }

    private void commonDeregisterAndClose(MatsSocketSessionState state, Integer closeCode, String reason) {
        _state = state;

        // :: Eagerly drop authorization for session, so that this session object is ensured to be pretty useless.
        _authorization = null;
        _principal = null;
        // Note: letting SessionId and userId be, as it is needed for some other parts, incl. introspection.

        // Make note of last liveliness timestamp, for absolute correctness. No-one will ever thank me for this..!
        _sessionLivelinessTimestamp = System.currentTimeMillis();

        // Unsubscribe from all topics
        _subscribedTopics.forEach(topicId -> {
            _matsSocketServer.deregisterMatsSocketSessionFromTopic(topicId, getConnectionId());
        });

        // Local deregister
        if (_matsSocketSessionId != null) {
            _matsSocketServer.deregisterLocalMatsSocketSession(_matsSocketSessionId, _connectionId);
        }
    }

    boolean isHoldOutgoingMessages() {
        return _holdOutgoingMessages;
    }

    boolean reevaluateAuthenticationForOutgoingMessage() {
        /*
         * Any evaluation implication SessionAuthenticator, and setting of _authorization, _principal and _userId, is
         * done within sync of SessionAuthenticator.
         */
        synchronized (_sessionAuthenticator) {
            // Ask SessionAuthenticator whether he still is happy with this Principal being authenticated, or
            // supplies a new Principal
            AuthenticationResult authenticationResult;
            try {
                authenticationResult = _sessionAuthenticator.reevaluateAuthenticationForOutgoingMessage(
                        _authenticationContext, _authorization, _principal, _lastAuthenticatedTimestamp.get());
            }
            catch (RuntimeException re) {
                log.error("Got Exception when invoking SessionAuthenticator"
                        + ".reevaluateAuthenticationOutgoingMessage(..), Authorization header: " + _authorization, re);
                closeSessionAndWebSocketWithPolicyViolation(
                        "Auth reevaluateAuthenticationOutgoingMessage(..): Got Exception.");
                return false;
            }
            // Evaluate whether we're still Authenticated or StillValid, and thus good to go with sending message
            boolean okToSendOutgoingMessages = (authenticationResult instanceof AuthenticationResult_Authenticated)
                    || (authenticationResult instanceof AuthenticationResult_StillValid);

            // ?: If we're NOT ok, then we should hold outgoing messages until AUTH comes in.
            if (!okToSendOutgoingMessages) {
                _holdOutgoingMessages = true;
            }

            // NOTE! We do NOT update '_lastAuthenticatedTimestamp' here, as this is exclusively meant for the
            // 'reevaluateAuthenticationForOutgoingMessage()' method.

            return okToSendOutgoingMessages;
        }
    }

    private enum AuthenticationHandlingResult {
        /**
         * Authentication evaluation went well.
         */
        OK,

        /**
         * Initial Authentication evaluation went bad, MatsSocketSession is closed, as is WebSocket - exit out of
         * handler.
         */
        BAD,

        /**
         * Re-evaluation (NOT initial) did not work out - ask Client for new auth. Possibly hold messages until AUTH
         * comes back.
         */
        REAUTH
    }

    private AuthenticationHandlingResult doAuthentication(String newAuthorization) {
        /*
         * Any evaluation implication SessionAuthenticator, and setting of _authorization, _principal and _userId, is
         * done within sync of SessionAuthenticator.
         */
        synchronized (_sessionAuthenticator) {
            // ?: Do we have an existing Principal?
            if (_principal == null) {
                // -> No, we do not have an existing Principal -> Initial Authentication
                // ::: Ask SessionAuthenticator if it is happy with this initial Authorization header

                // Assert that we then have a new Authorization header
                if (newAuthorization == null) {
                    throw new AssertionError("We have not got Principal, and a new Authorization header is"
                            + " not provided either.");
                }
                AuthenticationResult authenticationResult;
                try {
                    authenticationResult = _sessionAuthenticator.initialAuthentication(_authenticationContext,
                            newAuthorization);
                }
                catch (RuntimeException e) {
                    // -> SessionAuthenticator threw - bad
                    log.error("Got Exception when invoking SessionAuthenticator.initialAuthentication(..),"
                            + " Authorization header: " + newAuthorization, e);
                    closeSessionAndWebSocketWithPolicyViolation("Initial Auth-eval: Got Exception");
                    return AuthenticationHandlingResult.BAD;
                }
                if (authenticationResult instanceof AuthenticationResult_Authenticated) {
                    // -> Authenticated!
                    AuthenticationResult_Authenticated res = (AuthenticationResult_Authenticated) authenticationResult;
                    goodAuthentication(newAuthorization, res._principal, res._userId, res._debugOptions,
                            "Initial Authentication");
                    // ?: Did AuthenticationPlugin set (or override) the Remote Address?
                    if (_authenticationContext._remoteAddr != null) {
                        // -> Yes it did, so set it.
                        _remoteAddr = _authenticationContext._remoteAddr;
                    }
                    // Unconditionally set the Originating Remote Address, as we make no attempt to resolve this native
                    _originatingRemoteAddr = _authenticationContext._originatingRemoteAddr;
                    return AuthenticationHandlingResult.OK;
                }
                else {
                    // -> InvalidAuthentication, null, or any other result.
                    badAuthentication(newAuthorization, authenticationResult, "Initial Auth-eval");
                    return AuthenticationHandlingResult.BAD;
                }
            }
            else {
                // -> Yes, we already have Principal -> Reevaluation of existing Authentication
                // ::: Ask SessionAuthenticator whether he still is happy with this Principal being authenticated, or
                // supplies a new Principal

                // If newAuthorization is provided, use that - otherwise use existing
                String authorizationToEvaluate = (newAuthorization != null ? newAuthorization : _authorization);

                // Assert that we actually have an Authorization header to evaluate
                if (authorizationToEvaluate == null) {
                    throw new AssertionError("We have not gotten neither an existing or a new"
                            + " Authorization Header.");
                }
                AuthenticationResult authenticationResult;
                try {
                    authenticationResult = _sessionAuthenticator.reevaluateAuthentication(_authenticationContext,
                            authorizationToEvaluate, _principal);
                }
                catch (RuntimeException e) {
                    // -> SessionAuthenticator threw - bad
                    log.error("Got Exception when invoking SessionAuthenticator.reevaluateAuthentication(..),"
                            + " Authorization header: " + authorizationToEvaluate, e);
                    closeSessionAndWebSocketWithPolicyViolation("Auth re-eval: Got Exception.");
                    return AuthenticationHandlingResult.BAD;
                }
                if (authenticationResult instanceof AuthenticationResult_Authenticated) {
                    // -> Authenticated anew
                    AuthenticationResult_Authenticated res = (AuthenticationResult_Authenticated) authenticationResult;
                    goodAuthentication(authorizationToEvaluate, res._principal, res._userId, res._debugOptions,
                            "Re-auth: New Authentication");
                    return AuthenticationHandlingResult.OK;
                }
                else if (authenticationResult instanceof AuthenticationResult_StillValid) {
                    // -> The existing authentication is still valid
                    // Do a "goodAuthentication"-invocation, just with existing info!
                    goodAuthentication(_authorization, _principal, _userId, _authAllowedDebugOptions,
                            "Still valid Authentication");
                    return AuthenticationHandlingResult.OK;
                }
                else if (authenticationResult instanceof AuthenticationResult_InvalidAuthentication) {
                    // -> The existing authentication is NOT any longer valid
                    log.info("NOT valid anymore Authentication - asking client for REAUTH. Current userId: [" + _userId
                            + "] and Principal [" + _principal + "]");
                    return AuthenticationHandlingResult.REAUTH;
                }
                else {
                    // -> InvalidAuthentication, null, or any other result.
                    badAuthentication(authorizationToEvaluate, authenticationResult, "Auth re-eval");
                    return AuthenticationHandlingResult.BAD;
                }
            }
        } // end sync _sessionAuthenticator
          // NOTE! There should NOT be a default return here!
    }

    private void goodAuthentication(String authorization, Principal principal, String userId,
            EnumSet<DebugOption> authAllowedDebugOptions, String what) {
        // Store the new values
        _authorization = authorization;
        _principal = principal;
        _userId = userId;
        _authAllowedDebugOptions = authAllowedDebugOptions;

        // Update the timestamp of when the SessionAuthenticator last time was happy with the authentication.
        _lastAuthenticatedTimestamp.set(System.currentTimeMillis());
        // We have gotten the Auth, so we do not currently have a question outstanding
        _askedClientForReauth = false;
        // ?: Are we on "hold outgoing messages"?
        if (_holdOutgoingMessages) {
            // -> Yes we are holding, so clear that and do a round of message forwarding
            _holdOutgoingMessages = false;
            // Notify forwarder.
            // NOTICE: There is by definition already HELLO processed and MatsSocketSessionId present in this situation.
            _matsSocketServer.getWebSocketOutboxForwarder().newMessagesInCsafNotify(this);
        }

        // Update MDCs before logging.
        MDC.put(MDC_PRINCIPAL_NAME, _principal.getName());
        MDC.put(MDC_USER_ID, _userId);
        log.info(what + " with UserId: [" + _userId + "] and Principal [" + _principal + "]");
    }

    private void badAuthentication(String authorizationToEvaluate, AuthenticationResult authenticationResult,
            String what) {
        log.error("SessionAuthenticator replied " + authenticationResult + ", Authorization header: "
                + authorizationToEvaluate);
        // ?: Is it NotAuthenticated?
        if (authenticationResult instanceof AuthenticationResult_InvalidAuthentication) {
            // -> Yes, explicit NotAuthenticated
            AuthenticationResult_InvalidAuthentication invalidAuthentication = (AuthenticationResult_InvalidAuthentication) authenticationResult;
            closeSessionAndWebSocketWithPolicyViolation(what + ": " + invalidAuthentication.getReason());
        }
        else {
            // -> Null or some unexpected value - no good.
            closeSessionAndWebSocketWithPolicyViolation(what + ": Failed");
        }
    }

    private boolean handleHello(long clientMessageReceivedTimestamp, MatsSocketEnvelopeWithMetaDto envelope) {
        long nanosStart = System.nanoTime();
        log.info("Handling HELLO!");
        // ?: Auth is required - should already have been processed
        if ((_principal == null) || (_authorization == null) || (_userId == null)) {
            // NOTE: This shall really never happen, as the implicit state machine should not have put us in this
            // situation. But just as an additional check.
            closeSessionAndWebSocketWithPolicyViolation("Missing authentication when evaluating HELLO message");
            return false;
        }

        _clientLibAndVersions = envelope.clv;
        if (_clientLibAndVersions == null) {
            closeSessionAndWebSocketWithProtocolError("Missing ClientLibAndVersion (clv) in HELLO envelope.");
            return false;
        }
        // NOTE: Setting MDC_CLIENT_LIB_AND_VERSIONS for the rest of this pipeline, but not for every onMessage!
        MDC.put(MDC_CLIENT_LIB_AND_VERSIONS, _clientLibAndVersions);
        String appName = envelope.an;
        if (appName == null) {
            closeSessionAndWebSocketWithProtocolError("Missing AppName (an) in HELLO envelope.");
            return false;
        }
        String appVersion = envelope.av;
        if (appVersion == null) {
            closeSessionAndWebSocketWithProtocolError("Missing AppVersion (av) in HELLO envelope.");
            return false;
        }
        _appName = appName;
        _appVersion = appVersion;
        // Ensure MDC is as current as possible
        setMDC();

        // ----- HELLO was good (and authentication is already performed, earlier in process)

        MatsSocketEnvelopeWithMetaDto replyEnvelope = new MatsSocketEnvelopeWithMetaDto();

        // ?: Do the client want to reconnecting using existing MatsSocketSessionId
        if (envelope.sid != null) {
            MDC.put(MDC_SESSION_ID, envelope.sid);
            log.info("MatsSocketSession Reconnect requested to MatsSocketSessionId [" + envelope.sid + "]");
            // -> Yes, try to find it

            // :: Local invalidation of existing session.
            Optional<MatsSocketSessionAndMessageHandler> existingSession = _matsSocketServer
                    .getRegisteredLocalMatsSocketSession(envelope.sid);
            // ?: Is there an existing local Session?
            if (existingSession.isPresent()) {
                log.info(" \\- Existing LOCAL Session found! (at this node)");
                // -> Yes, thus you can use it.
                /*
                 * NOTE: If it is open - which it "by definition" should not be - we close the *previous*. The question
                 * of whether to close this or previous: We chose previous because there might be reasons where the
                 * client feels that it has lost the connection, but the server hasn't yet found out. The client will
                 * then try to reconnect, and that is ok. So we close the existing. Since it is always the server that
                 * creates session Ids and they are large and globally unique, AND since we've already authenticated the
                 * user so things should be OK, this ID is obviously the one the client got the last time. So if he
                 * really wants to screw up his life by doing reconnects when he does not need to, then OK.
                 */
                // ?: Is the existing local session open?
                if (existingSession.get().getWebSocketSession().isOpen()) {
                    // -> Yes, open, so close.DISCONNECT existing - we will "overwrite" the current local afterwards.
                    existingSession.get().deregisterSessionAndCloseWebSocket(MatsSocketCloseCodes.DISCONNECT,
                            "Cannot have two MatsSockets with the same MatsSocketSessionId"
                                    + " - closing the previous (from local node)");
                }
                // You're allowed to use this, since the sessionId was already existing.
                _matsSocketSessionId = envelope.sid;
                replyEnvelope.desc = "reconnected - existing local";
            }
            else {
                log.info(" \\- No existing local Session found (i.e. at this node), check CSAF..");
                // -> No, no local existing session, but is there an existing session in CSAF?
                try {
                    boolean sessionExists = _matsSocketServer.getClusterStoreAndForward()
                            .isSessionExists(envelope.sid);
                    // ?: Is there a CSAF Session?
                    if (sessionExists) {
                        log.info(" \\- Existing CSAF Session found!");
                        // -> Yes, there is a CSAF Session - so client can use this session
                        _matsSocketSessionId = envelope.sid;
                        replyEnvelope.desc = "reconnected - existing in CSAF";
                        MDC.put(MDC_SESSION_ID, _matsSocketSessionId);
                        // :: Check if there is a current node where it is not yet closed
                        Optional<CurrentNode> currentNode = _matsSocketServer
                                .getClusterStoreAndForward()
                                .getCurrentRegisteredNodeForSession(_matsSocketSessionId);
                        // ?: Remote close of WebSocket if it is still open at the CurrentNode
                        currentNode.ifPresent(node -> _matsSocketServer
                                .closeWebSocketFor(_matsSocketSessionId, node));
                    }
                    else {
                        // -> There is no existing session
                        log.info(" \\- No existing CSAF Session found either, closing remote with SESSION_LOST");
                        closeSessionAndWebSocket(MatsSocketCloseCodes.SESSION_LOST,
                                "HELLO from client asked for reconnect, but given MatsSocketSessionId was gone.");
                        return false;
                    }
                }
                catch (DataAccessException e) {
                    log.warn("Asked for reconnect, but got problems with DB.", e);
                    closeSessionAndWebSocket(MatsSocketCloseCodes.UNEXPECTED_CONDITION,
                            "Asked for reconnect, but got problems with DB.");
                    return false;
                }
            }
        }

        // ?: Do we have a MatsSocketSessionId by now?
        boolean newSession = false;
        if (_matsSocketSessionId == null) {
            // -> No, so make one.
            newSession = true;
            _matsSocketSessionId = DefaultMatsSocketServer.rnd(16);
        }

        // Set state SESSION_ESTABLISHED
        _state = MatsSocketSessionState.SESSION_ESTABLISHED;

        // Record timestamp of when this session was established.
        long now = System.currentTimeMillis();
        _sessionEstablishedTimestamp = System.currentTimeMillis();
        // .. also set this as "last ping" and "last activity", as otherwise we get annoying "1970-01-01..", i.e. Epoch
        _lastClientPingTimestamp.set(now);
        _lastActivityTimestamp.set(now);

        // Ensure MDC is as current as possible
        setMDC();

        // Register Session at this node
        _matsSocketServer.registerLocalMatsSocketSession(this);
        // :: Register Session in CSAF, and reset "attempted delivery" mark.
        try {

            // :: Assert that the user do not have too many sessions going already
            int sessCount = _matsSocketServer.getClusterStoreAndForward().getSessionsCount(false, _userId, null, null);
            // ?: Check if we overflow when adding one more session
            if ((sessCount + 1) > MAX_NUMBER_OF_SESSIONS_PER_USER_ID) {
                // -> Yes, that would be too many: Error
                log.warn("Too many sessions for userId [" + _userId + "], so we will not accept this new one.");
                closeSessionAndWebSocketWithPolicyViolation("Too many sessions for resolved userId: [" + sessCount
                        + "].");
                return false;
            }

            // Register in CSAF
            _createdTimestamp = _matsSocketServer.getClusterStoreAndForward().registerSessionAtThisNode(
                    _matsSocketSessionId, _userId, _connectionId, _clientLibAndVersions, _appName, _appVersion);

            // Clear attempted delivery mark, to perform retransmission of these.
            _matsSocketServer.getClusterStoreAndForward().outboxMessagesUnmarkAttemptedDelivery(_matsSocketSessionId);

        }
        catch (WrongUserException e) {
            // -> This should never occur with the normal MatsSocket clients, so this is probably hackery going on.
            log.error("We got WrongUserException when (evidently) trying to reconnect to existing SessionId."
                    + " This sniffs of hacking.", e);
            closeSessionAndWebSocketWithPolicyViolation(
                    "UserId of existing SessionId does not match currently logged in user.");
            return false;
        }
        catch (DataAccessException e) {
            // -> We could not talk to data store, so we cannot accept sessions at this point. Sorry.
            log.warn("Could not establish session in CSAF.", e);
            closeSessionAndWebSocket(MatsSocketCloseCodes.UNEXPECTED_CONDITION,
                    "Could not establish Session information in permanent storage, sorry.");
            return false;
        }

        // ----- We're now a live MatsSocketSession!

        // Increase timeout to "prod timeout", now that client has said HELLO
        _webSocketSession.setMaxIdleTimeout(75_000);
        // Set high limit for text, as we don't want to be held back on the protocol side of things.
        _webSocketSession.setMaxTextMessageBufferSize(50 * 1024 * 1024);

        // :: Record incoming Envelope
        recordEnvelopes(Collections.singletonList(envelope), clientMessageReceivedTimestamp, Direction.C2S);

        // :: Notify SessionEstablishedEvent listeners - AFTER it is added to MatsSocketServer
        _matsSocketServer.invokeSessionEstablishedEventListeners(new SessionEstablishedEventImpl(
                (newSession ? SessionEstablishedEventType.NEW : SessionEstablishedEventType.RECONNECT),
                this));

        // :: Create reply WELCOME message

        // Stack it up with props
        replyEnvelope.t = WELCOME;
        replyEnvelope.sid = _matsSocketSessionId;
        replyEnvelope.tid = envelope.tid;

        log.info("Sending WELCOME! MatsSocketSessionId:[" + _matsSocketSessionId + "]!");

        // Pack it over to client
        // NOTICE: Since this is the first message ever for this connection, there will not be any currently-sending
        // messages the other way (i.e. server-to-client, from the MessageToWebSocketForwarder). Therefore, it is
        // perfectly OK to do this synchronously right here.
        List<MatsSocketEnvelopeWithMetaDto> replySingleton = Collections.singletonList(replyEnvelope);
        try {
            String json = _envelopeListObjectWriter.writeValueAsString(replySingleton);
            webSocketSendText(json);
        }
        catch (JsonProcessingException e) {
            throw new AssertionError("Huh, couldn't serialize message?!", e);
        }
        catch (IOException e) {
            // TODO: Handle!
            // TODO: At least store last messageSequenceId that we had ASAP. Maybe do it async?!
            throw new AssertionError("Hot damn.", e);
        }

        // :: Record outgoing Envelope
        replyEnvelope.icts = clientMessageReceivedTimestamp;
        replyEnvelope.rttm = msSince(nanosStart);
        recordEnvelopes(Collections.singletonList(replyEnvelope), System.currentTimeMillis(), Direction.S2C);

        // MessageForwarder-> There might be EXISTING messages waiting for this MatsSocketSession!
        _matsSocketServer.getWebSocketOutboxForwarder().newMessagesInCsafNotify(this);

        // This went well.
        return true;
    }

    private boolean handleSendOrRequestOrReply(long receivedTimestamp,
            MatsSocketEnvelopeWithMetaDto envelope) {
        long nanosStart = System.nanoTime();
        log.debug("  \\- " + envelope + ", msg:[" + envelope.msg + "].");

        registerActivityTimestamp(receivedTimestamp);

        // :: Assert as much as possible before sending over for handling.

        // 1. All "information bearing messages" shall have TraceId
        if (envelope.tid == null) {
            closeSessionAndWebSocketWithProtocolError("Missing 'tid' (TraceId) on " + envelope.t + ", cmid:["
                    + envelope.cmid + "].");
            return false;
        }
        // 2. All "information bearing messages" shall have sender specific message Id, for C2S this means 'cmid'.
        if (envelope.cmid == null) {
            closeSessionAndWebSocketWithProtocolError("Missing 'cmid' on " + envelope.t + ".");
            return false;
        }
        // 3. If this is a Reply to a Server-to-Client REQUEST, it shall have the sender specific message Id; 'smid'
        if (((envelope.t == RESOLVE) || (envelope.t == REJECT)) && (envelope.smid == null)) {
            closeSessionAndWebSocketWithProtocolError("Missing 'smid' on a Client Reply to a Server-to-Client"
                    + " REQUEST, for message with cmid:[" + envelope.cmid + "].");
            return false;
        }

        _matsSocketServer.getIncomingSendAndRequestAndRepliesHandler().handleSendOrRequestOrReply(this,
                receivedTimestamp, nanosStart, envelope);

        // This went OK
        return true;
    }

    private void handleSub(long receivedTimestamp, List<MatsSocketEnvelopeWithMetaDto> replyEnvelopes,
            MatsSocketEnvelopeWithMetaDto envelope) {
        long nanosStart = System.nanoTime();
        if ((envelope.eid == null) || (envelope.eid.trim().isEmpty())) {
            closeSessionAndWebSocketWithProtocolError("SUB: Topic is null or empty.");
            return;
        }
        // ?: Already subscribed to this topic?
        if (_subscribedTopics.contains(envelope.eid)) {
            // -> Yes, already subscribed - client shall locally handle multi-subscriptions to same topic: Error.
            closeSessionAndWebSocketWithProtocolError("SUB: Already subscribed to Topic ["
                    + envelope.eid + "]");
            return;
        }

        // ?: "DoS-protection", from using absurdly long topic names to deplete our memory.
        if (envelope.eid.length() > MAX_LENGTH_OF_TOPIC_NAME) {
            // -> Yes, too long topicId.
            closeSessionAndWebSocketWithPolicyViolation("TopicId length > " + MAX_LENGTH_OF_TOPIC_NAME);
        }

        // ?: "DoS-protection", from inadvertent or malicious subscription to way too many topics.
        if (_subscribedTopics.size() >= MAX_NUMBER_OF_TOPICS_PER_SESSION) {
            // -> Yes, too many subscriptions: Error.
            closeSessionAndWebSocketWithProtocolError("SUB: Subscribed to way too many topics ["
                    + _subscribedTopics.size() + "]");
            return;
        }

        // :: Handle the subscription
        MatsSocketEnvelopeWithMetaDto replyEnvelope = new MatsSocketEnvelopeWithMetaDto();
        // :: AUTHORIZE
        // ?: Is the user allowed to subscribe to this topic?
        boolean authorized;
        try {
            authorized = _sessionAuthenticator.authorizeUserForTopic(_authenticationContext, envelope.eid);
        }
        catch (RuntimeException re) {
            log.error("The SessionAuthenticator [" + _sessionAuthenticator + "] raised a [" + re
                    .getClass().getSimpleName() + "] when asked whether the user [" + _userId
                    + "], principal:[" + _principal + "] was allowed to subscribe to Topic ["
                    + envelope.eid + "]");
            closeSessionAndWebSocketWithPolicyViolation("Authorization of Topic subscription: Error.");
            return;
        }
        // ?: Were we authorized?!
        if (authorized) {
            // -> YES, authorized to subscribe to this topic!

            // Add it to MatsSocketSession's view of subscribed Topics
            _subscribedTopics.add(envelope.eid);
            // Add it to the actual Topic in the MatsSocketServer
            _matsSocketServer.registerMatsSocketSessionWithTopic(envelope.eid, this);

            // TODO: Handle replay of lost messages!!!

            // NOTE! No replay-mechanism is yet implemented, so IF the Client is reconnecting, we'll ALWAYS reply with
            // "SUB_LOST" if the client specified a specific "last message seen"

            // ?: Did client specify "last message seen" id?
            if (envelope.smid != null) {
                // -> Yes, Client has specified "last message seen" message id.
                // TODO: Could at least hold message id of *last* message sent, so that we do not reply SUB_LOST
                // completely unnecessary.
                log.warn("We got a 'smid' on a SUB message, but we have no messages to replay."
                        + " Thus answering SUB_LOST.");
                replyEnvelope.t = SUB_LOST;
            }
            else {
                // -> Client did not supply "last message seen". This was OK
                replyEnvelope.t = SUB_OK;
            }
        }
        else {
            // -> NO, NOT authorized to subscribe to this Topic!
            // We will NOT subscribe the Client to this Topic.
            replyEnvelope.t = SUB_NO_AUTH;
        }
        replyEnvelope.eid = envelope.eid;

        replyEnvelope.icts = receivedTimestamp;
        // TODO: Should really be when after sent
        replyEnvelope.rttm = msSince(nanosStart);

        replyEnvelopes.add(replyEnvelope);
    }

    private void handleUnsub(MatsSocketEnvelopeWithMetaDto envelope) {
        if ((envelope.eid == null) || (envelope.eid.trim().isEmpty())) {
            closeSessionAndWebSocketWithProtocolError("UNSUB: Topic is null or empty.");
            return;
        }
        _subscribedTopics.remove(envelope.eid);
        _matsSocketServer.deregisterMatsSocketSessionFromTopic(envelope.eid, getConnectionId());
        // Note: No reply-message
    }

    void publishToTopic(String topicId, String env, String msg) {
        long nanos_start_Deserialize = System.nanoTime();
        MatsSocketEnvelopeWithMetaDto envelope;
        try {
            envelope = _matsSocketServer.getEnvelopeObjectReader().readValue(env);
        }
        catch (JsonProcessingException e) {
            throw new AssertionError("Could not deserialize Envelope DTO.");
        }
        // Set the message onto the envelope, in "raw" mode (it is already json)
        envelope.msg = DirectJson.of(msg);
        // Handle debug
        if (envelope.debug != null) {
            /*
             * For Server-initiated messages, we do not know what the user wants wrt. debug information until now, and
             * thus the initiation always adds it. We thus need to check with the AuthenticationPlugin's resolved auth
             * vs. what the client has asked for wrt. Server-initiated.
             */
            // Find what the client requests along with what authentication allows
            EnumSet<DebugOption> debugOptions = getCurrentResolvedServerToClientDebugOptions();
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
                    envelope.debug.mscts = System.currentTimeMillis();
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
        double milliDeserializeMessage = msSince(nanos_start_Deserialize);

        registerActivityTimestamp(System.currentTimeMillis());

        long nanos_start_Serialize = System.nanoTime();
        String jsonEnvelopeList;
        try {
            jsonEnvelopeList = _envelopeListObjectWriter.writeValueAsString(Collections.singletonList(envelope));
        }
        catch (JsonProcessingException e) {
            // TODO: Fix
            throw new AssertionError("Hot damn.");
        }
        double milliSerializeMessage = msSince(nanos_start_Serialize);

        // :: Actually send message over WebSocket.
        long nanos_start_SendMessage = System.nanoTime();
        try {
            webSocketSendText(jsonEnvelopeList);
        }
        catch (IOException e) {
            // TODO: Fix
            throw new AssertionError("Hot damn.");
        }
        double millisSendMessages = msSince(nanos_start_SendMessage);
        log.debug("Forwarded a topic message [" + topicId + "], time taken to deserialize: [" + milliDeserializeMessage
                + "], serialize:[" + milliSerializeMessage + "], send:[" + millisSendMessages + "]");
    }

    @Override
    public String toString() {
        return "MatsSocketSession{id='" + getMatsSocketSessionId() + ",connId:'" + getConnectionId() + "'}";
    }

}
