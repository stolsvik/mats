package com.stolsvik.mats.websocket.impl;

import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.ACK;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.ACK2;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.AUTH;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.HELLO;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.NACK;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.PING;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.PONG;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.REJECT;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.REQUEST;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.RESOLVE;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.RETRY;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.SEND;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.WELCOME;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

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
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsInitiator.MatsBackendRuntimeException;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;
import com.stolsvik.mats.MatsInitiator.MatsMessageSendRuntimeException;
import com.stolsvik.mats.websocket.AuthenticationPlugin;
import com.stolsvik.mats.websocket.AuthenticationPlugin.AuthenticationContext;
import com.stolsvik.mats.websocket.AuthenticationPlugin.AuthenticationResult;
import com.stolsvik.mats.websocket.AuthenticationPlugin.DebugOption;
import com.stolsvik.mats.websocket.AuthenticationPlugin.SessionAuthenticator;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.ClientMessageIdAlreadyExistsException;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.CurrentNode;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.DataAccessException;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.RequestCorrelation;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.StoredMessage;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.WrongUserException;
import com.stolsvik.mats.websocket.MatsSocketServer.IncomingAuthorizationAndAdapter;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketCloseCodes;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEndpointRequestContext;
import com.stolsvik.mats.websocket.MatsSocketServer.MessageType;
import com.stolsvik.mats.websocket.impl.AuthenticationContextImpl.AuthenticationResult_Authenticated;
import com.stolsvik.mats.websocket.impl.AuthenticationContextImpl.AuthenticationResult_InvalidAuthentication;
import com.stolsvik.mats.websocket.impl.AuthenticationContextImpl.AuthenticationResult_StillValid;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer.MatsSocketEndpointRegistration;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer.ReplyHandleStateDto;
import com.stolsvik.mats.websocket.impl.MatsSocketEnvelopeDto.DebugDto;
import com.stolsvik.mats.websocket.impl.MatsSocketEnvelopeDto.DirectJsonMessage;

/**
 * Effectively the MatsSocketSession, this is the MatsSocket "onMessage" handler.
 *
 * @author Endre Stølsvik 2019-11-28 12:17 - http://stolsvik.com/, endre@stolsvik.com
 */
class MatsSocketMessageHandler implements Whole<String>, MatsSocketStatics {
    private static final Logger log = LoggerFactory.getLogger(MatsSocketMessageHandler.class);

    private final DefaultMatsSocketServer _matsSocketServer;
    private Session _webSocketSession; // Non-final to be able to null out upon close.
    private String _connectionId; // Non-final to be able to null out upon close.
    // SYNC: itself.
    private final SessionAuthenticator _sessionAuthenticator;

    // Derived
    private Basic _webSocketBasicRemote; // Non-final to be able to null out upon close.
    private final AuthenticationContext _authenticationContext;
    private final ObjectReader _envelopeObjectReader;
    private final ObjectWriter _envelopeObjectWriter;
    private final ObjectReader _envelopeListObjectReader;
    private final ObjectWriter _envelopeListObjectWriter;

    // Set
    private String _matsSocketSessionId;
    // SYNC: Auth-fields are only modified while holding sync on _sessionAuthenticator.
    private String _authorization; // nulled upon close
    private Principal _principal; // nulled upon close
    private String _userId; // nulled upon close
    private EnumSet<DebugOption> _authAllowedDebugOptions;
    private EnumSet<DebugOption> _currentResolvedServerToClientDebugOptions = EnumSet.noneOf(DebugOption.class);
    private long _lastAuthenticatedTimestamp; // set to System.currentTimeMillis() each time (re)evaluated OK.
    private volatile boolean _holdOutgoingMessages;

    private String _clientLibAndVersion;
    private String _appNameAndVersion;

    private boolean _closed; // set true upon close. When closed, won't process any more messages.
    private boolean _helloProcessedOk; // set true when HELLO processed. HELLO only accepted once.

    MatsSocketMessageHandler(DefaultMatsSocketServer matsSocketServer, Session webSocketSession, String connectionId,
            HandshakeRequest handshakeRequest, SessionAuthenticator sessionAuthenticator) {
        _matsSocketServer = matsSocketServer;
        _webSocketSession = webSocketSession;
        _connectionId = connectionId;
        _sessionAuthenticator = sessionAuthenticator;

        // Derived
        _webSocketBasicRemote = _webSocketSession.getBasicRemote();
        _authenticationContext = new AuthenticationContextImpl(handshakeRequest, _webSocketSession);
        _envelopeObjectReader = _matsSocketServer.getEnvelopeObjectReader();
        _envelopeObjectWriter = _matsSocketServer.getEnvelopeObjectWriter();
        _envelopeListObjectReader = _matsSocketServer.getEnvelopeListObjectReader();
        _envelopeListObjectWriter = _matsSocketServer.getEnvelopeListObjectWriter();
    }

    Session getWebSocketSession() {
        return _webSocketSession;
    }

    private final Object _webSocketSendSyncObject = new Object();

    void webSocketSendText(String text) throws IOException {
        synchronized (_webSocketSendSyncObject) {
            _webSocketBasicRemote.sendText(text);
        }
    }

    /**
     * NOTE: There can <i>potentially</i> be multiple instances of {@link MatsSocketMessageHandler} with the same Id if
     * we're caught by bad asyncness wrt. one connection dropping and the client immediately reconnecting. The two
     * {@link MatsSocketMessageHandler}s would then hey would then have different {@link #getWebSocketSession()
     * WebSocketSessions}, i.e. differing actual connections. One of them would soon realize that is was closed. <b>This
     * Id together with {@link #getConnectionId()} is unique</b>.
     *
     * @return the MatsSocketSessionId that this {@link MatsSocketMessageHandler} instance refers to.
     */
    String getMatsSocketSessionId() {
        return _matsSocketSessionId;
    }

    /**
     * NOTE: Read JavaDoc of {@link #getMatsSocketSessionId} to understand why this Id is of interest.
     */
    String getConnectionId() {
        return _connectionId;
    }

    /**
     * @return the {@link DebugOption}s that the {@link AuthenticationPlugin} told us was allowed for this user to
     *         request.
     */
    public EnumSet<DebugOption> getAuthAllowedDebugOptions() {
        return _authAllowedDebugOptions;
    }

    /**
     * @return the {@link DebugOption}s requested by client intersected with what is allowed for this user ("resolved"),
     *         for Server-initiated messages (Server-to-Client SEND and REQUEST).
     */
    public EnumSet<DebugOption> getCurrentResolvedServerToClientDebugOptions() {
        return _currentResolvedServerToClientDebugOptions;
    }

    @Override
    public void onMessage(String message) {
        try { // try-finally: MDC.clear();
            long clientMessageReceivedTimestamp = System.currentTimeMillis();
            if (_matsSocketSessionId != null) {
                MDC.put(MDC_SESSION_ID, _matsSocketSessionId);
                MDC.put(MDC_PRINCIPAL_NAME, _principal.getName());
                MDC.put(MDC_USER_ID, _userId);
            }
            if (_appNameAndVersion != null) {
                MDC.put(MDC_CLIENT_APP_NAME_AND_VERSION, _appNameAndVersion);
            }

            // ?: Are we closed?
            if (_closed) {
                // -> Yes, so ignore message.
                log.info("WebSocket @OnMessage: WebSocket received message for CLOSED MatsSocketSessionId ["
                        + _matsSocketSessionId + "], connectionId:[" + _connectionId + "], size:[" + message.length()
                        + " cp] this:" + DefaultMatsSocketServer.id(this) + "], ignoring, msg: " + message);
                return;
            }

            // E-> Not closed, process message (containing MatsSocket envelope(s)).

            log.info("WebSocket @OnMessage: WebSocket received message for MatsSocketSessionId [" + _matsSocketSessionId
                    + "], connectionId:[" + _connectionId + "], size:[" + message.length()
                    + " cp] this:" + DefaultMatsSocketServer.id(this));

            // :: Parse the message into MatsSocket envelopes
            List<MatsSocketEnvelopeDto> envelopes;
            try {
                envelopes = _envelopeListObjectReader.readValue(message);
            }
            catch (IOException e) {
                log.error("Could not parse WebSocket message into MatsSocket envelope(s).", e);
                closeSessionWithProtocolError("Could not parse message into MatsSocket envelope(s)");
                return;
            }

            if (log.isDebugEnabled()) log.debug("Messages: " + envelopes);
            boolean shouldNotifyAboutExistingMessages = false;

            // :: 0. Look for request-debug in any of the messages
            for (MatsSocketEnvelopeDto envelope : envelopes) {
                // ?: Pick out any "Request Debug" flags
                if (envelope.rd != null) {
                    // -> Yes, there was an authorization header sent along with this message
                    EnumSet<DebugOption> requestedDebugOptions = DebugOption.enumSetOf(envelope.rd);
                    // Intersect this with allowed DebugOptions
                    requestedDebugOptions.retainAll(_authAllowedDebugOptions);
                    // Store the result as new Server-to-Client DebugOptions
                    _currentResolvedServerToClientDebugOptions = requestedDebugOptions;
                }
            }

            // :: 1. Look for Authorization header in any of the messages
            // NOTE! Authorization header can come with ANY message!
            String newAuthorization = null;
            for (MatsSocketEnvelopeDto envelope : envelopes) {
                // ?: Pick out any Authorization header, i.e. the auth-string - it can come in any message.
                if (envelope.auth != null) {
                    // -> Yes, there was an authorization header sent along with this message
                    newAuthorization = envelope.auth;
                    log.info("Found authorization header in message of type [" + envelope.t + "]");
                    // Notify client about "new" (as in existing) messages, just in case there are any.
                    shouldNotifyAboutExistingMessages = true;
                    // Notice: No 'break', as we want to go through all messages and find the latest auth header.
                }
            }
            // :: 1b. Remove any specific AUTH message (it ONLY contains the 'auth' property, handled above).
            envelopes.removeIf(envelope -> envelope.t == AUTH);

            // :: 2a. Handle any PING messages (send PONG asap).

            for (Iterator<MatsSocketEnvelopeDto> it = envelopes.iterator(); it.hasNext();) {
                MatsSocketEnvelopeDto envelope = it.next();
                // ?: Is this message a PING?
                if (envelope.t == PING) {
                    // -> Yes, so handle it.
                    // Remove it from the pipeline
                    it.remove();
                    // Assert that we've had HELLO already processed
                    // NOTICE! We will handle PINGs without valid Authorization, but only if we've already established
                    // Session, as checked by seeing if we've processed HELLO
                    if (!_helloProcessedOk) {
                        closeSessionWithProtocolError("Cannot process PING before HELLO and session established");
                        return;
                    }
                    // :: Create PONG message
                    MatsSocketEnvelopeDto replyEnvelope = new MatsSocketEnvelopeDto();
                    replyEnvelope.t = PONG;
                    replyEnvelope.x = envelope.x;

                    // Pack the PONG over to client ASAP.
                    // TODO: Consider doing this async, probably with MessageToWebSocketForwarder
                    List<MatsSocketEnvelopeDto> replySingleton = Collections.singletonList(replyEnvelope);
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
                }
            }

            // :: 2b. Handle any PONG messages

            for (Iterator<MatsSocketEnvelopeDto> it = envelopes.iterator(); it.hasNext();) {
                MatsSocketEnvelopeDto envelope = it.next();
                // ?: Is this message a PING?
                if (envelope.t == PONG) {
                    // -> Yes, so handle it.
                    // Remove it from the pipeline
                    it.remove();
                    // Assert that we've had HELLO already processed
                    // NOTICE! We will handle PINGs without valid Authorization, but only if we've already established
                    // Session, as checked by seeing if we've processed HELLO
                    if (!_helloProcessedOk) {
                        closeSessionWithProtocolError("Cannot process PING before HELLO and session established");
                        return;
                    }

                    // TODO: Handle PONGs (well, first the server actually needs to send PINGs..!)
                }
            }

            // ?: Do we have any more messages in pipeline, or have we gotten new Authorization?
            if (envelopes.isEmpty() && (newAuthorization == null)) {
                // -> No messages, and no new auth. Thus, all messages that were here was PING.
                // Return, without considering valid existing authentication.
                // Again: It is allowed to send PINGs, and thus keep connection open, without having current
                // valid authorization. The rationale here is that otherwise we'd have to /continuously/ ask an
                // OAuth/OIDC/token server about new tokens, which probably would keep that session open. With
                // the present logic, the only thing you can do without authorization, is keeping the connection
                // actually open. Once you try to send any information-bearing messages, authentication check will
                // immediately kick in - and then you better have sent over valid auth, otherwise you're kicked off.
                return;
            }

            // AUTHENTICATION! On every pipeline of messages, we re-evaluate authentication

            // :: 3. Evaluate Authentication by Authorization header
            boolean authenticationOk = doAuthentication(newAuthorization);
            // ?: Did this go OK?
            if (!authenticationOk) {
                // -> No, not OK - doAuthentication() has already closed session and websocket and the lot.
                return;
            }

            // :: 4. Are we authenticated? (I.e. Authorization Header must sent along in the very first pipeline..)
            if (_authorization == null) {
                log.error("We have not got Authorization header!");
                closeSessionWithPolicyViolation("Missing Authorization header");
                return;
            }
            if (_principal == null) {
                log.error("We have not got Principal!");
                closeSessionWithPolicyViolation("Missing Principal");
                return;
            }
            if (_userId == null) {
                log.error("We have not got UserId!");
                closeSessionWithPolicyViolation("Missing UserId");
                return;
            }

            // :: 5. look for a HELLO message
            // (should be first/alone, but we will reply to it immediately even if part of pipeline).
            for (Iterator<MatsSocketEnvelopeDto> it = envelopes.iterator(); it.hasNext();) {
                MatsSocketEnvelopeDto envelope = it.next();
                if (envelope.t == HELLO) {
                    try { // try-finally: MDC.remove(..)
                        MDC.put(MDC_MESSAGE_TYPE, envelope.t.name());
                        // Remove this HELLO envelope from pipeline
                        it.remove();
                        // ?: Have we processed HELLO before?
                        if (_helloProcessedOk) {
                            // -> Yes, and this is not according to protocol.
                            closeSessionWithPolicyViolation(
                                    "Shall only receive HELLO once for an entire MatsSocket Session.");
                            return;
                        }
                        // E-> First time we see HELLO
                        // :: Handle the HELLO
                        boolean handleHelloOk = handleHello(clientMessageReceivedTimestamp, envelope);
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

            // :: 6. Assert state: All present: SessionId, Authorization, Principal and UserId.
            if ((_matsSocketSessionId == null)
                    || (_authorization == null)
                    || (_principal == null)
                    || (_userId == null)) {
                closeSessionWithPolicyViolation("Illegal state at checkpoint.");
                return;
            }

            List<String> clientAcks = null;
            List<String> clientAck2s = null;

            // :: 7. Now go through and handle all the rest of the messages
            List<MatsSocketEnvelopeDto> replyEnvelopes = new ArrayList<>();
            for (MatsSocketEnvelopeDto envelope : envelopes) {
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
                        boolean ok = handleSendOrRequestOrReply(clientMessageReceivedTimestamp, replyEnvelopes,
                                envelope);
                        if (!ok) {
                            return;
                        }
                        // The message is handled, so go to next message.
                        continue;
                    }

                    // :: This is the "client has received an information-bearing message"-message, denoting
                    // that we can delete it from outbox on our side.

                    // ?: Is this a ACK or NACK for a message from us?
                    else if ((envelope.t == ACK) || (envelope.t == NACK)) {
                        if (envelope.smid == null) {
                            closeSessionWithProtocolError("Received " + envelope.t + " message with missing 'smid.");
                            return;
                        }
                        if (clientAcks == null) {
                            clientAcks = new ArrayList<>();
                        }
                        clientAcks.add(envelope.smid);
                        // The message is handled, so go to next message.
                        continue;
                    }

                    // :: This is the "client has deleted an information-bearing message from his outbox"-message,
                    // denoting that we can delete it from inbox on our side

                    // ?: Is this a ACK2 for a ACK/NACK from us?
                    else if (envelope.t == ACK2) {
                        if (envelope.cmid == null) {
                            closeSessionWithProtocolError("Received ACK2 message with missing 'cmid.");
                            return;
                        }
                        if (clientAck2s == null) {
                            clientAck2s = new ArrayList<>();
                        }
                        clientAck2s.add(envelope.cmid);
                        // The message is handled, so go to next message.
                        continue;
                    }

                    // :: Unknown message..

                    else {
                        // -> Unknown message: We're not very lenient her - CLOSE SESSION AND CONNECTION!
                        log.error("Got an unknown message type [" + envelope.t
                                + "] from client. Answering by closing connection with PROTOCOL_ERROR.");
                        closeSessionWithProtocolError("Received unknown message type.");
                        return;
                    }
                }
                finally {
                    MDC.remove(MDC_MESSAGE_TYPE);
                }
            }

            // ----- All messages handled.

            // Send all replies
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
                    // TODO: At least store last messageSequenceId that we had ASAP. Maybe do it async?!
                    throw new AssertionError("Hot damn.", e);
                }
            }
            // ?: Notify about existing messages
            if (shouldNotifyAboutExistingMessages) {
                // -> Yes, so do it now.
                _matsSocketServer.getMessageToWebSocketForwarder().newMessagesInCsafNotify(this);
            }

            // ?: Did we get any RECEIVED?
            // TODO: Make this a bit more nifty, putting such Ids on a queue of sorts, finishing async
            if (clientAcks != null) {
                log.debug("Got ACK/NACK for messages " + clientAcks + ".");
                try {
                    _matsSocketServer.getClusterStoreAndForward().outboxMessagesComplete(_matsSocketSessionId,
                            clientAcks);
                }
                catch (DataAccessException e) {
                    // TODO: Make self-healer thingy.
                    log.warn("Got problems when trying to mark messages as complete. Ignoring, hoping for miracles.");
                }

                // Send "ACK2", i.e. "I've now deleted these Ids from my outbox".
                // TODO: Use the MessageToWebSocketForwarder for this.
                List<MatsSocketEnvelopeDto> ackAckEnvelopes = new ArrayList<>(clientAcks.size());
                for (String ackSmid : clientAcks) {
                    MatsSocketEnvelopeDto replyEnvelope = new MatsSocketEnvelopeDto();
                    replyEnvelope.t = ACK2;
                    replyEnvelope.smid = ackSmid;
                    ackAckEnvelopes.add(replyEnvelope);
                }
                try {
                    String json = _envelopeListObjectWriter.writeValueAsString(ackAckEnvelopes);
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
            }

            // ?: Did we get any ACKACK?
            // TODO: Make this a bit more nifty, putting such Ids on a queue of sorts, finishing async
            if (clientAck2s != null) {
                log.debug("Got ACK2 for messages " + clientAck2s + ".");
                try {
                    _matsSocketServer.getClusterStoreAndForward().deleteMessageIdsFromInbox(_matsSocketSessionId,
                            clientAck2s);
                }
                catch (DataAccessException e) {
                    // TODO: Make self-healer thingy.
                    log.warn("Got problems when trying to mark messages as complete. Ignoring, hoping for miracles.");
                }
            }
        }
        finally {
            MDC.clear();
        }
    }

    void closeSessionWithProtocolError(String reason) {
        closeSessionAndWebSocket(MatsSocketCloseCodes.PROTOCOL_ERROR, reason);
    }

    void closeSessionWithPolicyViolation(String reason) {
        closeSessionAndWebSocket(MatsSocketCloseCodes.VIOLATED_POLICY, reason);
    }

    void closeSessionAndWebSocket(MatsSocketCloseCodes closeCode, String reason) {
        // We're closed
        _closed = true;

        // :: Get copy of the WebSocket Session, before nulling it out
        Session webSocketSession = _webSocketSession;

        // :: Eagerly drop all authorization for session, so that this session object is ensured to be utterly useless.
        _authorization = null;
        _principal = null;
        _userId = null;
        _lastAuthenticatedTimestamp = -1;
        // :: Also nulling out our references to the WebSocket, to ensure that it is impossible to send anything more
        _webSocketSession = null;
        _webSocketBasicRemote = null;

        // :: Deregister locally and Close MatsSocket Session in CSAF
        if (_matsSocketSessionId != null) {
            // Local deregister of live connection
            _matsSocketServer.deregisterLocalMatsSocketSession(_matsSocketSessionId, _connectionId);

            try {
                // CSAF close session
                _matsSocketServer.getClusterStoreAndForward().closeSession(_matsSocketSessionId);
            }
            catch (DataAccessException e) {
                log.warn("Could not close session in CSAF. This is unfortunate, as it then is technically possible to"
                        + " still reconnect to the session while this evidently was not the intention"
                        + " (only the same user can reconnect, though). However, the session scavenger"
                        + " will clean this lingering session out after some hours.", e);
            }
        }

        // :: Close the actual WebSocket
        DefaultMatsSocketServer.closeWebSocket(webSocketSession, closeCode, reason);
    }

    void closeWebSocketAndDeregisterSession(MatsSocketCloseCodes closeCode, String reason) {
        // Close WebSocket
        DefaultMatsSocketServer.closeWebSocket(_webSocketSession, closeCode, reason);
        // Local deregister
        _matsSocketServer.deregisterLocalMatsSocketSession(_matsSocketSessionId, _connectionId);
        // CSAF deregister
        try {
            _matsSocketServer.getClusterStoreAndForward().deregisterSessionFromThisNode(_matsSocketSessionId,
                    _connectionId);
        }
        catch (DataAccessException e) {
            log.warn("Could not deregister MatsSocketSessionId [" + _matsSocketSessionId
                    + "] from CSAF, ignoring.", e);
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
                        _authenticationContext, _authorization, _principal, _lastAuthenticatedTimestamp);
            }
            catch (RuntimeException re) {
                log.error("Got Exception when invoking SessionAuthenticator"
                        + ".reevaluateAuthenticationOutgoingMessage(..), Authorization header: " + _authorization, re);
                closeSessionWithPolicyViolation("Auth reevaluateAuthenticationOutgoingMessage(..): Got Exception.");
                return false;
            }
            // Return whether we're still Authenticated or StillValid, and thus good to go with sending message
            boolean okToSendOutgoingMessages = (authenticationResult instanceof AuthenticationResult_Authenticated)
                    || (authenticationResult instanceof AuthenticationResult_StillValid);

            // ?: If we're not OK, then we should hold outgoing messages for now
            if (!okToSendOutgoingMessages) {
                _holdOutgoingMessages = true;
            }
            return okToSendOutgoingMessages;
        }
    }

    private boolean doAuthentication(String newAuthorization) {
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
                    closeSessionWithPolicyViolation("Initial Auth-eval: Got Exception");
                    return false;
                }
                if (authenticationResult instanceof AuthenticationResult_Authenticated) {
                    // -> Authenticated!
                    goodAuthentication(newAuthorization,
                            (AuthenticationResult_Authenticated) authenticationResult, "Authenticated");
                    return true;
                }
                else {
                    // -> InvalidAuthentication, null, or any other result.
                    badAuthentication(newAuthorization, authenticationResult, "Initial Auth-eval");
                    return false;
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
                    closeSessionWithPolicyViolation("Auth re-eval: Got Exception.");
                    return false;
                }
                if (authenticationResult instanceof AuthenticationResult_Authenticated) {
                    // -> Authenticated anew
                    goodAuthentication(authorizationToEvaluate,
                            (AuthenticationResult_Authenticated) authenticationResult, "New authentication");
                    return true;
                }
                else if (authenticationResult instanceof AuthenticationResult_StillValid) {
                    // -> The existing authentication is still valid
                    log.debug("Still valid authentication with UserId: [" + _userId + "] and Principal [" + _principal
                            + "]");
                    // Update Authorization, in case it changed (strange that it didn't return "Authenticated" instead?)
                    _authorization = authorizationToEvaluate;
                    // Update lastAuthenticatedMillis, since it was still happy with it.
                    _lastAuthenticatedTimestamp = System.currentTimeMillis();
                    return true;
                }
                else {
                    // -> InvalidAuthentication, null, or any other result.
                    badAuthentication(authorizationToEvaluate, authenticationResult, "Auth re-eval");
                    return false;
                }
            }
        } // end sync _sessionAuthenticator
          // NOTE! There should NOT be a default return here!
    }

    private void goodAuthentication(String authorization,
            AuthenticationResult_Authenticated authenticationResult,
            String what) {
        // Store the new values
        _authorization = authorization;
        _principal = authenticationResult._principal;
        _userId = authenticationResult._userId;
        _authAllowedDebugOptions = authenticationResult._debugOptions;
        // We can now send outgoing messages
        _holdOutgoingMessages = false;
        // Update the timestamp of when the SessionAuthenticator last time was happy with the authentication.
        _lastAuthenticatedTimestamp = System.currentTimeMillis();

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
            closeSessionWithPolicyViolation(what + ": " + invalidAuthentication.getReason());
        }
        else {
            // -> Null or some unexpected value - no good.
            closeSessionWithPolicyViolation(what + ": Failed");
        }
    }

    private boolean handleHello(long clientMessageReceivedTimestamp, MatsSocketEnvelopeDto envelope) {
        log.info("Handling HELLO!");
        // ?: Auth is required - should already have been processed
        if ((_principal == null) || (_authorization == null) || (_userId == null)) {
            // NOTE: This shall really never happen, as the implicit state machine should not have put us in this
            // situation. But just as an additional check.
            closeSessionWithPolicyViolation("Missing authentication when evaluating HELLO message");
            return false;
        }
        // We've received hello.
        _helloProcessedOk = true;

        _clientLibAndVersion = envelope.clv;
        if (_clientLibAndVersion == null) {
            closeSessionWithProtocolError("Missing ClientLibAndVersion (clv) in HELLO envelope.");
            return false;
        }
        MDC.put(MDC_CLIENT_LIB_AND_VERSIONS, _clientLibAndVersion);
        String appName = envelope.an;
        if (appName == null) {
            closeSessionWithProtocolError("Missing AppName (an) in HELLO envelope.");
            return false;
        }
        String appVersion = envelope.av;
        if (appVersion == null) {
            closeSessionWithProtocolError("Missing AppVersion (av) in HELLO envelope.");
            return false;
        }
        _appNameAndVersion = appName + ";" + appVersion;

        // ----- HELLO was good (and authentication is already performed, earlier in process)

        MatsSocketEnvelopeDto replyEnvelope = new MatsSocketEnvelopeDto();

        // ?: Do the client want to reconnecting using existing MatsSocketSessionId
        if (envelope.sid != null) {
            log.info("MatsSocketSession Reconnect requested to MatsSocketSessionId [" + envelope.sid + "]");
            // -> Yes, try to find it

            // :: Local invalidation of existing session.
            Optional<MatsSocketMessageHandler> existingSession = _matsSocketServer
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
                // ?: If the existing is open, then deregister it from us, and DISCONNECT it.
                if (existingSession.get().getWebSocketSession().isOpen()) {
                    existingSession.get().closeWebSocketAndDeregisterSession(MatsSocketCloseCodes.DISCONNECT,
                            "Cannot have two MatsSockets with the same MatsSocketSessionId"
                                    + " - closing the previous (from local node)");
                }
                // You're allowed to use this, since the sessionId was already existing.
                _matsSocketSessionId = envelope.sid;
                replyEnvelope.desc = "reconnected - existing local";
                MDC.put(MDC_SESSION_ID, _matsSocketSessionId);
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
        if (_matsSocketSessionId == null) {
            // -> No, so make one.
            _matsSocketSessionId = DefaultMatsSocketServer.rnd(16);
        }
        MDC.put(MDC_SESSION_ID, _matsSocketSessionId);

        // Register Session at this node
        _matsSocketServer.registerLocalMatsSocketSession(this);
        // Register Session in CSAF
        try {
            _matsSocketServer.getClusterStoreAndForward().registerSessionAtThisNode(_matsSocketSessionId, _userId,
                    _connectionId);
        }
        catch (WrongUserException e) {
            // -> This should never occur with the normal MatsSocket clients, so this is probably hackery going on.
            log.error("We got WrongUserException when (evidently) trying to reconnect to existing SessionId."
                    + " This sniffs of hacking.", e);
            closeSessionWithPolicyViolation("UserId of existing SessionId does not match currently logged in user.");
            return false;
        }
        catch (DataAccessException e) {
            // -> We could not talk to data store, so we cannot accept sessions at this point. Sorry.
            log.warn("Could not establish session in CSAF.", e);
            closeSessionAndWebSocket(MatsSocketCloseCodes.UNEXPECTED_CONDITION,
                    "Could not establish Session information in permanent storage, sorry.");
            return false;
        }

        // ----- We're now a live MatsSocketSession

        // Increase timeout to "prod timeout", now that client has said HELLO
        // TODO: Increase timeout, e.g. 75 seconds.
        _webSocketSession.setMaxIdleTimeout(30_000);
        // Set high limit for text, as we, don't want to be held back on the protocol side of things.
        _webSocketSession.setMaxTextMessageBufferSize(50 * 1024 * 1024);

        // :: Create reply WELCOME message

        // Stack it up with props
        replyEnvelope.t = WELCOME;
        replyEnvelope.sid = _matsSocketSessionId;
        replyEnvelope.tid = envelope.tid;

        log.info("Sending WELCOME! MatsSocketSessionId:[" + _matsSocketSessionId + "]!");

        // Pack it over to client
        // NOTICE: Since this is the first message ever, there will not be any currently-sending messages the other
        // way (i.e. server-to-client, from the MessageToWebSocketForwarder). Therefore, it is perfectly OK to
        // do this synchronously right here.
        List<MatsSocketEnvelopeDto> replySingleton = Collections.singletonList(replyEnvelope);
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
        return true;
    }

    private boolean handleSendOrRequestOrReply(long clientMessageReceivedTimestamp,
            List<MatsSocketEnvelopeDto> replyEnvelopes, MatsSocketEnvelopeDto envelope) {
        MessageType type = envelope.t;

        log.info("  \\- " + envelope + ", msg:[" + envelope.msg + "].");

        // :: Assert some props.
        if (envelope.cmid == null) {
            closeSessionWithProtocolError("Missing 'cmid' on " + envelope.t + ".");
            return false;
        }
        if (envelope.tid == null) {
            closeSessionWithProtocolError("Missing 'tid' (TraceId) on " + envelope.t + ", cmid:[" + envelope.cmid
                    + "].");
            return false;
        }
        if (((type == RESOLVE) || (type == REJECT)) && (envelope.smid == null)) {
            closeSessionWithProtocolError("Missing 'smid' on a REPLY from Client, cmid:[" + envelope.cmid + "].");
            return false;
        }

        MatsSocketEnvelopeDto[] handledEnvelope = new MatsSocketEnvelopeDto[] { new MatsSocketEnvelopeDto() };
        handledEnvelope[0].cmid = envelope.cmid; // Client MessageId.

        // :: Perform the entire handleIncoming(..) inside Mats initiate-lambda
        RequestCorrelation[] _correlationInfo_LambdaHack = new RequestCorrelation[1];
        try {
            _matsSocketServer.getMatsFactory().getDefaultInitiator().initiateUnchecked(init -> {

                String targetEndpointId;

                String correlationString = null;
                byte[] correlationBinary = null;
                // ?: Is this a Reply (RESOLVE or REJECT)?
                if ((type == RESOLVE) || (type == REJECT)) {
                    // -> Yes, Reply (RESOLVE or REJECT), so we'll get-and-delete the Correlation information.
                    // Find the CorrelationInformation - or NOT, if this is a duplicate delivery.
                    try {
                        Optional<RequestCorrelation> correlationInfoO = _matsSocketServer.getClusterStoreAndForward()
                                .getAndDeleteRequestCorrelation(_matsSocketSessionId, envelope.smid);
                        // ?: Did we have CorrelationInformation?
                        if (!correlationInfoO.isPresent()) {
                            // -> NO, no CorrelationInformation present, so this is a dupe
                            // Double delivery: Simply say "yes, yes, good, good" to client, as we have already
                            // processed this one.
                            log.info("We have evidently got a double-delivery for ClientMessageId [" + envelope.cmid
                                    + "] of type [" + envelope.t + "], fixing by ACK it again"
                                    + " (it's already processed).");
                            handledEnvelope[0].t = ACK;
                            handledEnvelope[0].desc = "dupe " + envelope.t;
                            // return from lambda
                            return;
                        }
                        // E-> YES, we had CorrelationInfo!
                        RequestCorrelation correlationInfo = correlationInfoO.get();
                        // Store it for half-assed attempt at un-fucking the situation if we get "VERY BAD!"-situation.
                        _correlationInfo_LambdaHack[0] = correlationInfo;
                        log.info("Incoming REPLY for Server-to-Client Request for smid[" + envelope.smid
                                + "], time since request: [" + (System.currentTimeMillis() - correlationInfo
                                        .getRequestTimestamp()) + " ms].");
                        correlationString = correlationInfo.getCorrelationString();
                        correlationBinary = correlationInfo.getCorrelationBinary();
                        // With a reply to a message initiated on the Server, the targetEID is in the correlation
                        targetEndpointId = correlationInfo.getReplyTerminatorId();
                    }
                    catch (DataAccessException e) {
                        throw new DatabaseRuntimeException(
                                "Got problems trying to get Correlation information for REPLY for smid:["
                                        + envelope.smid + "].", e);
                    }
                }
                else {
                    // -> No, not a Reply back to us, so this is a REQUEST or SEND:

                    // With a message initiated on the client, the targetEndpointId is embedded in the message
                    targetEndpointId = envelope.eid;

                    // Store the ClientMessageId in the Inbox to catch double deliveries.
                    /*
                     * This shall throw ClientMessageIdAlreadyExistsException if we've already processed this before.
                     *
                     * Notice: It MIGHT be that the SQLIntegrityConstraintViolationException (or similar) is not raised
                     * until commit due to races, albeit this seems rather far-fetched considering that there shall not
                     * be any concurrent handling of this particular MatsSocketSessionId. Anyway, failure on commit will
                     * lead to the Mats initiation to throw MatsBackendRuntimeException, which is caught further down,
                     * and the client shall then end up with redelivery. When redelivered, the other message should
                     * already be in place, and we should get the unique constraint violation right here.
                     */
                    try {
                        _matsSocketServer.getClusterStoreAndForward().storeMessageIdInInbox(_matsSocketSessionId,
                                envelope.cmid);
                    }
                    catch (DataAccessException e) {
                        // Throw out of the lambda, letting Mats do its rollback-thing.
                        // Notice: This will raise a MatsBackendRuntimeException, which is handled down below.
                        throw new DatabaseRuntimeException(e);
                    }
                    catch (ClientMessageIdAlreadyExistsException e) {
                        // -> Already have this in the inbox, so this is a dupe
                        // Double delivery: Fetch the answer we said last time, and just answer that!

                        // :: Fetch previous answer if present and answer that, otherwise answer default; ACK.
                        try {
                            StoredMessage messageFromInbox = _matsSocketServer.getClusterStoreAndForward()
                                    .getMessageFromInbox(_matsSocketSessionId, envelope.cmid);
                            // ?: Did we have a serialized message here?
                            if (messageFromInbox.getEnvelope() != null) {
                                // -> Yes, we had the JSON from last processing stored!
                                log.info("We had an envelope from last time! " + messageFromInbox.getMessageText());
                                MatsSocketEnvelopeDto lastTimeEnvelope = _envelopeObjectReader.readValue(
                                        messageFromInbox.getEnvelope());
                                // Doctor the deserialized envelope (by magic JSON-holder DirectJsonMessage)
                                // (The 'msg' field is currently a proper JSON String, we want it directly as-is)
                                lastTimeEnvelope.msg = DirectJsonMessage.of((String) lastTimeEnvelope.msg);
                                // REPLACE the existing handledEnvelope - use "magic" to NOT re-serialize the JSON.
                                handledEnvelope[0] = lastTimeEnvelope;
                                handledEnvelope[0].desc = "dupe " + envelope.t + " stored";
                                log.info("We have evidently got a double-delivery for ClientMessageId [" + envelope.cmid
                                        + "] of type [" + envelope.t + "] - we had it stored, so just replying the"
                                        + " previous answer again.");
                            }
                            else {
                                // -> We did NOT have a previous JSON stored, which means that it was the default: ACK
                                handledEnvelope[0].t = MessageType.ACK;
                                handledEnvelope[0].desc = "dupe " + envelope.t + " ACK";
                                log.info("We have evidently got a double-delivery for ClientMessageId [" + envelope.cmid
                                        + "] of type [" + envelope.t + "] - it was NOT stored, thus it was an ACK.");
                            }
                            // We're done here - return from Mats-initiate-lambda
                            return;
                        }
                        catch (DataAccessException ex) {
                            // TODO: Do something with these exceptions - more types?
                            throw new DatabaseRuntimeException(ex);
                        }
                        catch (JsonProcessingException ex) {
                            // TODO: Fix
                            throw new AssertionError("Hot damn!", ex);
                        }
                    }
                }

                // Go get the Endpoint registration.
                Optional<MatsSocketEndpointRegistration<?, ?, ?, ?>> registrationO = _matsSocketServer
                        .getMatsSocketEndpointRegistration(targetEndpointId);
                // ?: Check if we found the endpoint
                if (!registrationO.isPresent()) {
                    // -> No, unknown MatsSocket EndpointId.
                    handledEnvelope[0].t = NACK;
                    handledEnvelope[0].desc = "An incoming " + envelope.t
                            + " envelope targeted a non-existing MatsSocketEndpoint";
                    log.warn("Unknown MatsSocketEndpointId for incoming envelope " + envelope);
                    // We're done here - return from Mats-initiate-lambda
                    return;
                }
                MatsSocketEndpointRegistration<?, ?, ?, ?> registration = registrationO.get();
                IncomingAuthorizationAndAdapter incomingAuthEval = registration.getIncomingAuthEval();

                // Deserialize the message with the info from the registration
                Object msg = deserialize((String) envelope.msg, registration.getMsIncomingClass());

                // :: Actually invoke the IncomingAuthorizationAndAdapter.handleIncoming(..)
                // Create the Context
                MatsSocketEndpointRequestContextImpl<?, ?, ?> requestContext = new MatsSocketEndpointRequestContextImpl(
                        _matsSocketServer, registration, _matsSocketSessionId, init, envelope,
                        clientMessageReceivedTimestamp, _authorization, _principal, _userId, _authAllowedDebugOptions,
                        type, correlationString, correlationBinary, msg);

                // Invoke the incoming handler
                incomingAuthEval.handleIncoming(requestContext, _principal, msg);

                // :: Based on the situation in the RequestContext, we return ACK/NACK/RETRY/RESOLVE/REJECT
                switch (requestContext._handled) {
                    case IGNORED:
                        // ?: Is this a REQUEST?
                        if (type == REQUEST) {
                            // -> Yes, REQUEST, so then it is not allowed to Ignore it.
                            handledEnvelope[0].t = NACK;
                            handledEnvelope[0].desc = "An incoming REQUEST envelope was ignored by the MatsSocket incoming handler.";
                            log.warn("handleIncoming(..) ignored an incoming REQUEST, i.e. not answered at all."
                                    + " Replying with [RECEIVED:NACK] to reject the outstanding request promise");
                        }
                        else {
                            // -> No, not REQUEST, i.e. either SEND, RESOLVE or REJECT, and then Ignore is OK.
                            handledEnvelope[0].t = ACK;
                            log.info("handleIncoming(..) evidently ignored the incoming SEND envelope. Responding"
                                    + " [RECEIVED:ACK], since that is OK.");
                        }
                        break;
                    case DENIED:
                        handledEnvelope[0].t = NACK;
                        log.info("handleIncoming(..) denied the incoming message. Replying with"
                                + " [RECEIVED:NACK]");
                        break;
                    case SETTLED_RESOLVE:
                    case SETTLED_REJECT:
                        // -> Yes, the handleIncoming insta-settled the incoming message, so we insta-reply
                        // NOTICE: We thus elide the "RECEIVED", as the client will handle the missing RECEIVED
                        handledEnvelope[0].t = requestContext._handled == Processed.SETTLED_RESOLVE
                                ? RESOLVE
                                : REJECT;
                        // Add standard Reply message properties, since this is no longer just an ACK/NACK
                        handledEnvelope[0].tid = envelope.tid; // TraceId

                        // Handle DebugOptions
                        EnumSet<DebugOption> debugOptions = DebugOption.enumSetOf(envelope.rd);
                        debugOptions.retainAll(_authAllowedDebugOptions);
                        if (!debugOptions.isEmpty()) {
                            DebugDto debug = new DebugDto();
                            if (debugOptions.contains(DebugOption.TIMINGS)) {
                                debug.cmrts = clientMessageReceivedTimestamp;
                                debug.mscts = System.currentTimeMillis();
                            }
                            if (debugOptions.contains(DebugOption.NODES)) {
                                debug.cmrnn = _matsSocketServer.getMyNodename();
                                debug.mscnn = _matsSocketServer.getMyNodename();
                            }
                            handledEnvelope[0].debug = debug;
                        }

                        handledEnvelope[0].msg = requestContext._matsSocketReplyMessage;
                        log.info("handleIncoming(..) insta-settled the incoming message with"
                                + " [" + envelope.t + "]");
                        break;
                    case FORWARDED:
                        handledEnvelope[0].t = ACK;
                        log.info("handleIncoming(..) forwarded the incoming message. Replying with"
                                + " [" + envelope.t + "]");
                        break;
                }

                // NOW, if we got anything else than an ACK out of this, we must store the reply in the inbox
                if (handledEnvelope[0].t != MessageType.ACK) {
                    log.debug("Got handledEnvelope of type [" + handledEnvelope[0].t + "], so storing it.");
                    try {
                        String envelopeJson = _envelopeObjectWriter.writeValueAsString(handledEnvelope[0]);
                        _matsSocketServer.getClusterStoreAndForward().updateMessageInInbox(_matsSocketSessionId,
                                envelope.cmid, envelopeJson, null);
                    }
                    catch (JsonProcessingException e) {
                        // TODO: Handle
                        throw new AssertionError("Hot damn!");
                    }
                    catch (DataAccessException e) {
                        throw new DatabaseRuntimeException(e);
                    }
                }
            });
        }
        catch (DatabaseRuntimeException e) {
            // Problems adding the ClientMessageId to outbox. Ask client to RETRY.
            // TODO: This log line is wrong.
            log.warn("Got problems storing incoming ClientMessageId in inbox - replying RETRY to client.",
                    e);
            handledEnvelope[0].t = RETRY;
            handledEnvelope[0].desc = e.getClass().getSimpleName() + ':' + e.getMessage();
        }
        catch (MatsBackendRuntimeException e) {
            // Evidently got problems talking to Mats backend, probably DB commit fail. Ask client to RETRY.
            log.warn("Got problems running handleIncoming(..), probably due to DB - replying RETRY to client.",
                    e);
            handledEnvelope[0].t = RETRY;
            handledEnvelope[0].desc = e.getClass().getSimpleName() + ':' + e.getMessage();
        }
        catch (MatsMessageSendRuntimeException e) {
            // Evidently got problems talking to MQ, aka "VERY BAD!". Trying to do compensating tx, then client RETRY
            log.warn("Got major problems running handleIncoming(..) due to DB committing, but MQ not committing."
                    + " Now trying compensating transaction - deleting from inbox (SEND or REQUEST) or"
                    + " re-inserting Correlation info (REPLY) - then replying RETRY to client.", e);

            /*
             * NOTICE! With "Outbox pattern" enabled on the MatsFactory, this exception shall never come. This because
             * if the sending to MQ does not work out, it will still have stored the message in the outbox, and the
             * MatsFactory will then get the message sent onto MQ at a later time, thus the need for this particular
             * exception is not needed, and will not be raised.
             */

            // :: Compensating transaction, i.e. delete that we've received the message (if SEND or REQUEST), or store
            // back the Correlation information (if REPLY), so that we can RETRY.
            // Go for a massively crude attempt to fix this if DB is down now: Retry the operation for some seconds.
            int retry = 0;
            while (true) {
                try {
                    // ?: Was this a REPLY (that wasn't a double-delivery)
                    if (_correlationInfo_LambdaHack[0] != null) {
                        // -> Yes, REPLY, so try to store back the Correlation Information since we did not handle
                        // it after all (so go for RETRY from Client).
                        RequestCorrelation c = _correlationInfo_LambdaHack[0];
                        _matsSocketServer.getClusterStoreAndForward()
                                .storeRequestCorrelation(_matsSocketSessionId, envelope.smid,
                                        c.getRequestTimestamp(), c.getReplyTerminatorId(),
                                        c.getCorrelationString(), c.getCorrelationBinary());
                    }
                    else {
                        // -> No, this was SEND or REQUEST, so try to delete the entry in the inbox since we did not
                        // handle it after all (so go for RETRY from Client).
                        _matsSocketServer.getClusterStoreAndForward()
                                .deleteMessageIdsFromInbox(_matsSocketSessionId, Collections.singleton(envelope.cmid));
                    }
                    // YES, this worked out!
                    break;
                }
                catch (DataAccessException ex) {
                    retry++;
                    if (retry >= 12) {
                        log.error("Dammit, didn't manage to recover from a MatsMessageSendRuntimeException."
                                + " Closing session and websocket with SERVER_ERROR.", ex);
                        closeSessionAndWebSocket(MatsSocketCloseCodes.UNEXPECTED_CONDITION,
                                "Server error, could not reliably recover.");
                        return false;
                    }
                    log.warn("Didn't manage to get out of a MatsMessageSendRuntimeException situation at attempt ["
                            + retry + "], will try again after sleeping half a second.", e);
                    try {
                        Thread.sleep(500);
                    }
                    catch (InterruptedException exc) {
                        log.warn("Got interrupted while chill-sleeping trying to recover from"
                                + "MatsMessageSendRuntimeException. Closing session and websocket with SERVER_ERROR.",
                                exc);
                        closeSessionAndWebSocket(MatsSocketCloseCodes.UNEXPECTED_CONDITION,
                                "Server error, could not reliably recover.");
                        return false;
                    }
                }
            }

            // ----- Compensating transaction worked, now ask client to go for retrying.

            handledEnvelope[0].t = RETRY;
            handledEnvelope[0].desc = e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        catch (Throwable t) {
            // Evidently the handleIncoming didn't handle this message. This is a NACK.
            log.warn("handleIncoming(..) raised exception, assuming that it didn't like the incoming message"
                    + " - replying RECEIVED:NACK to client.", t);
            handledEnvelope[0].t = NACK;
            handledEnvelope[0].desc = t.getClass().getSimpleName() + ": " + t.getMessage();
        }

        // Add ACK/NACK/RETRY/RESOLVE/REJECT message to "queue"
        replyEnvelopes.add(handledEnvelope[0]);

        // This went OK.
        return true;
    }

    private <T> T deserialize(String serialized, Class<T> clazz) {
        try {
            return _matsSocketServer.getJackson().readValue(serialized, clazz);
        }
        catch (JsonProcessingException e) {
            // TODO: Handle parse exceptions.
            throw new AssertionError("Damn", e);
        }
    }

    @Override
    public String toString() {
        return "MatsSocketSession{id='" + getMatsSocketSessionId() + ",connId:'" + getConnectionId() + "'}";
    }

    /**
     * Raised if a message seems to be double-delivered. Either that it already exists in the inbox, or that, for a
     * REPLY, there is no Correlation info for it.
     */
    private static class DuplicateDeliveryException extends RuntimeException {
        public DuplicateDeliveryException(Exception e) {
            super(e);
        }

        public DuplicateDeliveryException(String message) {
            super(message);
        }
    }

    /**
     * Raised if problems during handling of incoming information-bearing message in Mats stages. Leads to RETRY.
     */
    private static class DatabaseRuntimeException extends RuntimeException {
        public DatabaseRuntimeException(Exception e) {
            super(e);
        }

        public DatabaseRuntimeException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    enum Processed {
        /**
         * If none of the deny, forward or settle methods was invoked. The "state machine" starts here, and can only go
         * to one of the other values - and then it cannot be changed.
         */
        IGNORED,
        /**
         * {@link MatsSocketEndpointRequestContext#deny()} was invoked.
         */
        DENIED,
        /**
         * {@link MatsSocketEndpointRequestContext#resolve(Object)} was invoked.
         */
        SETTLED_RESOLVE,
        /**
         * {@link MatsSocketEndpointRequestContext#reject(Object)} was invoked.
         */
        SETTLED_REJECT,
        /**
         * {@link MatsSocketEndpointRequestContext#forwardCustom(Object, InitiateLambda)} or its ilk was invoked.
         */
        FORWARDED
    }

    private static class MatsSocketEndpointRequestContextImpl<I, MI, R> implements
            MatsSocketEndpointRequestContext<I, MI, R> {
        private final DefaultMatsSocketServer _matsSocketServer;
        private final MatsSocketEndpointRegistration _matsSocketEndpointRegistration;

        private final String _matsSocketSessionId;

        private final MatsInitiate _matsInitiate;

        private final MatsSocketEnvelopeDto _envelope;
        private final long _clientMessageReceivedTimestamp;

        private final String _authorization;
        private final Principal _principal;
        private final String _userId;
        private final EnumSet<DebugOption> _allowedDebugOptions;

        private final String _correlationString;
        private final byte[] _correlationBinary;
        private final I _incomingMessage;

        private final MessageType _messageType;

        public MatsSocketEndpointRequestContextImpl(DefaultMatsSocketServer matsSocketServer,
                MatsSocketEndpointRegistration matsSocketEndpointRegistration, String matsSocketSessionId,
                MatsInitiate matsInitiate,
                MatsSocketEnvelopeDto envelope, long clientMessageReceivedTimestamp, String authorization,
                Principal principal, String userId, EnumSet<DebugOption> allowedDebugOptions,
                MessageType messageType,
                String correlationString, byte[] correlationBinary, I incomingMessage) {
            _matsSocketServer = matsSocketServer;
            _matsSocketEndpointRegistration = matsSocketEndpointRegistration;
            _matsSocketSessionId = matsSocketSessionId;
            _matsInitiate = matsInitiate;
            _envelope = envelope;
            _clientMessageReceivedTimestamp = clientMessageReceivedTimestamp;

            _authorization = authorization;
            _principal = principal;
            _userId = userId;
            _allowedDebugOptions = allowedDebugOptions;

            _messageType = messageType;

            _correlationString = correlationString;
            _correlationBinary = correlationBinary;
            _incomingMessage = incomingMessage;
        }

        private R _matsSocketReplyMessage;
        private Processed _handled = Processed.IGNORED;

        @Override
        public String getMatsSocketEndpointId() {
            return _envelope.eid;
        }

        @Override
        public String getAuthorizationHeader() {
            return _authorization;
        }

        @Override
        public Principal getPrincipal() {
            return _principal;
        }

        @Override
        public String getUserId() {
            return _userId;
        }

        @Override
        public EnumSet<DebugOption> getAllowedDebugOptions() {
            return _allowedDebugOptions;
        }

        @Override
        public EnumSet<DebugOption> getResolvedDebugOptions() {
            // Resolve which DebugOptions are requested and allowed
            EnumSet<DebugOption> debugOptions = DebugOption.enumSetOf(_envelope.rd);
            debugOptions.retainAll(getAllowedDebugOptions());
            return debugOptions;
        }

        @Override
        public String getMatsSocketSessionId() {
            return _matsSocketSessionId;
        }

        @Override
        public String getTraceId() {
            return _envelope.tid;
        }

        @Override
        public MessageType getMessageType() {
            return _messageType;
        }

        @Override
        public I getMatsSocketIncomingMessage() {
            return _incomingMessage;
        }

        @Override
        public String getCorrelationString() {
            return _correlationString;
        }

        @Override
        public byte[] getCorrelationBinary() {
            return _correlationBinary;
        }

        @Override
        public void deny() {
            if (_handled != Processed.IGNORED) {
                throw new IllegalStateException("Already handled.");
            }
            _handled = Processed.DENIED;
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
            if (_handled != Processed.IGNORED) {
                throw new IllegalStateException("Already handled.");
            }
            _handled = Processed.FORWARDED;

            MatsInitiate init = _matsInitiate;
            init.from("MatsSocketEndpoint." + _envelope.eid)
                    .traceId(_envelope.tid);
            // Add a small extra side-load - the MatsSocketSessionId - since it seems nice.
            init.addString("matsSocketSessionId", _matsSocketSessionId);
            // -> Is this a REQUEST?
            if (getMessageType() == REQUEST) {
                // -> Yes, this is a REQUEST, so we should forward as Mats .request(..)
                // :: Need to make state so that receiving terminator know what to do.

                EnumSet<DebugOption> resolvedDebugOptions = getResolvedDebugOptions();
                Integer debugFlags = DebugOption.flags(resolvedDebugOptions);
                // Hack to save a tiny bit of space for this that mostly will be 0
                if (debugFlags == 0) {
                    debugFlags = null;
                }

                ReplyHandleStateDto sto = new ReplyHandleStateDto(_matsSocketSessionId,
                        _matsSocketEndpointRegistration.getMatsSocketEndpointId(),
                        _envelope.cmid, debugFlags, _clientMessageReceivedTimestamp,
                        _matsSocketServer.getMyNodename(), System.currentTimeMillis());
                // Set ReplyTo parameter
                init.replyTo(_matsSocketServer.getReplyTerminatorId(), sto);
                // Invoke the customizer
                customInit.initiate(init);
                // Send the REQUEST message
                init.request(matsMessage);
            }
            else {
                // -> No, not a REQUEST (thus either SEND or REPLY): Forward as fire-and-forget style Mats .send(..)
                // Invoke the customizer
                customInit.initiate(init);
                // Send the SEND message
                init.send(matsMessage);
            }
        }

        @Override
        public void resolve(R matsSocketResolveMessage) {
            if (getMessageType() != REQUEST) {
                throw new IllegalStateException("This is not a Request, thus you cannot resolve nor reject it."
                        + " For a SEND, your options is to deny() it, forward it to Mats, or ignore it (and just return).");
            }
            if (_handled != Processed.IGNORED) {
                throw new IllegalStateException("Already handled.");
            }
            _matsSocketReplyMessage = matsSocketResolveMessage;
            _handled = Processed.SETTLED_RESOLVE;
        }

        @Override
        public void reject(R matsSocketRejectMessage) {
            if (getMessageType() != REQUEST) {
                throw new IllegalStateException("This is not a Request, thus you cannot resolve nor reject it."
                        + " For a SEND, your options is to deny() it, forward it to Mats, or ignore it (and just return).");
            }
            if (_handled != Processed.IGNORED) {
                throw new IllegalStateException("Already handled.");
            }
            _matsSocketReplyMessage = matsSocketRejectMessage;
            _handled = Processed.SETTLED_REJECT;
        }
    }
}
