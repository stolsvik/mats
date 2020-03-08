package com.stolsvik.mats.websocket.impl;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
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
import com.stolsvik.mats.websocket.AuthenticationPlugin.AuthenticationContext;
import com.stolsvik.mats.websocket.AuthenticationPlugin.AuthenticationResult;
import com.stolsvik.mats.websocket.AuthenticationPlugin.SessionAuthenticator;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.ClientMessageIdAlreadyExistsException;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.CurrentNode;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.DataAccessException;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.RequestCorrelation;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.WrongUserException;
import com.stolsvik.mats.websocket.MatsSocketServer.IncomingAuthorizationAndAdapter;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketCloseCodes;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEndpointRequestContext;
import com.stolsvik.mats.websocket.MatsSocketServer.MessageType;
import com.stolsvik.mats.websocket.impl.AuthenticationContextImpl.AuthenticationResult_Authenticated;
import com.stolsvik.mats.websocket.impl.AuthenticationContextImpl.AuthenticationResult_NotAuthenticated;
import com.stolsvik.mats.websocket.impl.AuthenticationContextImpl.AuthenticationResult_StillValid;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer.MatsSocketEndpointRegistration;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer.ReplyHandleStateDto;

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
    private final ObjectReader _envelopeListObjectReader;
    private final ObjectWriter _envelopeListObjectWriter;

    // Set
    private String _matsSocketSessionId;
    // SYNC: Auth-fields are only modified while holding sync on _sessionAuthenticator.
    private String _authorization; // nulled upon close
    private Principal _principal; // nulled upon close
    private String _userId; // nulled upon close
    private long _lastAuthenticatedMillis; // set to System.currentTimeMillis() each time (re)evaluated OK.

    private String _clientLibAndVersion;
    private String _appNameAndVersion;

    private boolean _closed; // set true upon close. When closed, won't process any more messages.
    private boolean _helloReceived; // set true when HELLO processed. HELLO only accepted once.

    MatsSocketMessageHandler(DefaultMatsSocketServer matsSocketServer, Session webSocketSession, String connectionId,
            HandshakeRequest handshakeRequest, SessionAuthenticator sessionAuthenticator) {
        _matsSocketServer = matsSocketServer;
        _webSocketSession = webSocketSession;
        _connectionId = connectionId;
        _sessionAuthenticator = sessionAuthenticator;

        // Derived
        _webSocketBasicRemote = _webSocketSession.getBasicRemote();
        _authenticationContext = new AuthenticationContextImpl(handshakeRequest, _webSocketSession);
        _envelopeListObjectReader = _matsSocketServer.getEnvelopeListObjectReader();
        _envelopeListObjectWriter = _matsSocketServer.getEnvelopeListObjectWriter();
    }

    Session getWebSocketSession() {
        return _webSocketSession;
    }

    private final Object _webSocketSendSync = new Object();

    void webSocketSendText(String text) throws IOException {
        synchronized (_webSocketSendSync) {
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
                        + _matsSocketSessionId + "], connectionId:[" + _connectionId + "], this:"
                        + DefaultMatsSocketServer.id(this) + "], ignoring, msg: " + message);
                return;
            }

            // E-> Not closed, process message (containing MatsSocket envelope(s)).

            log.info("WebSocket @OnMessage: WebSocket received message for MatsSocketSessionId [" + _matsSocketSessionId
                    + "], connectionId:[" + _connectionId + "], this:" + DefaultMatsSocketServer.id(this));

            // :: Parse the message into MatsSocket envelopes
            List<MatsSocketEnvelopeDto> envelopes;
            try {
                envelopes = _envelopeListObjectReader.readValue(message);
            }
            catch (IOException e) {
                log.error("Could not parse WebSocket message into MatsSocket envelope(s).", e);
                closeWithProtocolError("Could not parse message into MatsSocket envelope(s)");
                return;
            }

            if (log.isDebugEnabled()) log.debug("Messages: " + envelopes);
            boolean shouldNotifyAboutExistingMessages = false;

            // :: 1. Look for Authorization header in any of the messages
            // NOTE! Authorization header can come with ANY message!
            String newAuthorization = null;
            for (MatsSocketEnvelopeDto envelope : envelopes) {
                // ?: Pick out any Authorization header, i.e. the auth-string - it can come in any message.
                if (envelope.auth != null) {
                    // -> Yes, there was an authorization header sent along with this message
                    newAuthorization = envelope.auth;
                    log.info("Found authorization header in message of type [" + (envelope.st != null ? envelope.t + ':'
                            + envelope.st : envelope.t) + "]");
                    // Notify client about "new" (as in existing) messages, just in case there are any.
                    shouldNotifyAboutExistingMessages = true;
                    // Notice: No 'break', as we want to go through all messages and find the latest auth header.
                }
            }

            // AUTHENTICATION! On every pipeline of messages, we re-evaluate authentication

            // :: 2. Evaluate Authentication by Authorization header
            boolean authenticationOk = doAuthentication(newAuthorization);
            // ?: Did this go OK?
            if (!authenticationOk) {
                // -> No, not OK - doAuthentication() has already closed session and websocket and the lot.
                return;
            }

            // :: 3. Are we authenticated? (I.e. Authorization Header must sent along in the very first pipeline..)
            if (_authorization == null) {
                log.error("We have not got Authorization header!");
                closeWithPolicyViolation("Missing Authorization header");
                return;
            }
            if (_principal == null) {
                log.error("We have not got Principal!");
                closeWithPolicyViolation("Missing Principal");
                return;
            }
            if (_userId == null) {
                log.error("We have not got UserId!");
                closeWithPolicyViolation("Missing UserId");
                return;
            }

            // :: 4. look for a HELLO message
            // (should be first/alone, but we will reply to it immediately even if part of pipeline).
            for (Iterator<MatsSocketEnvelopeDto> it = envelopes.iterator(); it.hasNext();) {
                MatsSocketEnvelopeDto envelope = it.next();
                if ("HELLO".equals(envelope.t)) {
                    try { // try-finally: MDC.remove(..)
                        MDC.put(MDC_MESSAGE_TYPE, (envelope.st != null ? envelope.t + ':' + envelope.st : envelope.t));
                        // Remove this HELLO envelope from pipeline
                        it.remove();
                        // ?: Have we processed HELLO before?
                        if (_helloReceived) {
                            // -> Yes, and this is not according to protocol.
                            closeWithPolicyViolation("Shall only receive HELLO once for an entire MatsSocket Session.");
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

            // :: 5. Assert state: All present: SessionId, Authorization, Principal and UserId.
            if ((_matsSocketSessionId == null)
                    || (_authorization == null)
                    || (_principal == null)
                    || (_userId == null)) {
                closeWithPolicyViolation("Illegal state at checkpoint.");
                return;
            }

            List<String> clientHasReceived = null;
            List<String> ackAckFromClient = null;

            // :: 6. Now go through and handle all the rest of the messages
            List<MatsSocketEnvelopeDto> replyEnvelopes = new ArrayList<>();
            for (MatsSocketEnvelopeDto envelope : envelopes) {
                try { // try-finally: MDC.remove..
                    MDC.put(MDC_MESSAGE_TYPE, (envelope.st != null ? envelope.t + ':' + envelope.st : envelope.t));
                    if (envelope.tid != null) {
                        MDC.put(MDC_TRACE_ID, envelope.tid);
                    }

                    // TODO: Move ping to above auth-requirement (but below HELLO verification).

                    if ("PING".equals(envelope.t)) {
                        MatsSocketEnvelopeDto replyEnvelope = new MatsSocketEnvelopeDto();
                        replyEnvelope.t = "PONG";
                        commonPropsOnReceived(envelope, replyEnvelope, clientMessageReceivedTimestamp);

                        // Add PONG message to return-pipeline
                        // (Note: To get accurate measurements of ping time, it should be sole message - not pipelined)
                        replyEnvelopes.add(replyEnvelope);
                        // The pong is handled, so go to next message
                        continue;
                    }

                    // :: These are the "information bearing messages" client-to-server, which we need to put in inbox
                    // so that we can catch double-deliveries of same message.

                    // ?: Is this a SEND or REQUEST to us?
                    else if ("SEND".equals(envelope.t) || "REQUEST".equals(envelope.t) || "REPLY".equals(envelope.t)) {
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

                    // ?: Is this a RECEIVED for a message from us?
                    else if ("RECEIVED".equals(envelope.t)) {
                        if (envelope.smid == null) {
                            closeWithProtocolError("Received RECEIVED message with missing 'smid.");
                            return;
                        }
                        if (clientHasReceived == null) {
                            clientHasReceived = new ArrayList<>();
                        }
                        clientHasReceived.add(envelope.smid);
                        // The message is handled, so go to next message.
                        continue;
                    }

                    // :: This is the "client has deleted an information-bearing message from his outbox"-message,
                    // denoting that we can delete it from inbox on our side

                    // ?: Is this a ACKACK for a RECEIVED from us?
                    else if ("ACKACK".equals(envelope.t)) {
                        if (envelope.cmid == null) {
                            closeWithProtocolError("Received ACKACK message with missing 'cmid.");
                            return;
                        }
                        if (ackAckFromClient == null) {
                            ackAckFromClient = new ArrayList<>();
                        }
                        ackAckFromClient.add(envelope.cmid);
                        // The message is handled, so go to next message.
                        continue;
                    }

                    // :: Unknown message..

                    else {
                        // -> Not expected message
                        log.error("Got an unknown message type [" + envelope.t + (envelope.st != null ? ":"
                                + envelope.st : "") + "] from client. Answering RECEIVED:ERROR.");
                        MatsSocketEnvelopeDto replyEnvelope = new MatsSocketEnvelopeDto();
                        replyEnvelope.t = "RECEIVED";
                        replyEnvelope.st = "ERROR";
                        commonPropsOnReceived(envelope, replyEnvelope, clientMessageReceivedTimestamp);

                        // Add error message to return-pipeline
                        replyEnvelopes.add(replyEnvelope);
                        // The message is handled, so go to next message.
                        continue;
                    }
                }
                finally {
                    MDC.remove(MDC_MESSAGE_TYPE);
                }
            }

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
            if (clientHasReceived != null) {
                log.debug("Got RECEIVED for messages " + clientHasReceived + ".");
                try {
                    _matsSocketServer.getClusterStoreAndForward().outboxMessagesComplete(_matsSocketSessionId,
                            clientHasReceived);
                }
                catch (DataAccessException e) {
                    // TODO: Make self-healer thingy.
                    log.warn("Got problems when trying to mark messages as complete. Ignoring, hoping for miracles.");
                }

                // Send "ACKACK", i.e. "I've now deleted these Ids from my outbox".
                // TODO: Use the MessageToWebSocketForwarder for this.
                List<MatsSocketEnvelopeDto> ackAckEnvelopes = new ArrayList<>(clientHasReceived.size());
                for (String ackSmid : clientHasReceived) {
                    MatsSocketEnvelopeDto replyEnvelope = new MatsSocketEnvelopeDto();
                    replyEnvelope.t = "ACKACK";
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
            if (ackAckFromClient != null) {
                log.debug("Got ACKACK for messages " + ackAckFromClient + ".");
                try {
                    _matsSocketServer.getClusterStoreAndForward().deleteMessageIdsFromInbox(_matsSocketSessionId,
                            ackAckFromClient);
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

    private void commonPropsOnReceived(MatsSocketEnvelopeDto envelope, MatsSocketEnvelopeDto replyEnvelope,
            long clientMessageReceivedTimestamp) {
        replyEnvelope.cmid = envelope.cmid; // Client MessageId.
        replyEnvelope.tid = envelope.tid; // TraceId
        replyEnvelope.cid = envelope.cid; // CorrelationId

        replyEnvelope.cmcts = envelope.cmcts; // Set by client..
        replyEnvelope.cmrts = clientMessageReceivedTimestamp;
        replyEnvelope.cmrnn = _matsSocketServer.getMyNodename();
        replyEnvelope.mscts = System.currentTimeMillis();
        replyEnvelope.mscnn = _matsSocketServer.getMyNodename();
    }

    void closeWithProtocolError(String reason) {
        closeSessionAndWebSocket(MatsSocketCloseCodes.PROTOCOL_ERROR, reason);
    }

    void closeWithPolicyViolation(String reason) {
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
        _lastAuthenticatedMillis = -1;
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
                        _authenticationContext, _authorization, _principal, _lastAuthenticatedMillis);
            }
            catch (RuntimeException re) {
                log.error("Got Exception when invoking SessionAuthenticator"
                        + ".reevaluateAuthenticationOutgoingMessage(..), Authorization header: " + _authorization, re);
                closeWithPolicyViolation("Auth reevaluateAuthenticationOutgoingMessage(..): Got Exception.");
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

    private volatile boolean _holdOutgoingMessages;

    boolean isHoldOutgoingMessages() {
        return _holdOutgoingMessages;
    }

    private boolean doAuthentication(String newAuthorization) {
        /*
         * Any evaluation implication SessionAuthenticator, and setting of _authorization, _principal and _userId, is
         * done within sync of SessionAuthenticator.
         */
        synchronized (_sessionAuthenticator) {
            // ?: Do we have an existing Principal?
            if (_principal == null) {
                // -> No, we do not have an existing Principal
                // Assert that we then have a new Authorization
                String authorizationToEvaluate = newAuthorization;
                if (authorizationToEvaluate == null) {
                    throw new AssertionError("We have not got Principal, and a new Authorization header is"
                            + " not provided either.");
                }
                // Ask SessionAuthenticator if it likes this Authorization header
                AuthenticationResult authenticationResult;
                try {
                    authenticationResult = _sessionAuthenticator.initialAuthentication(_authenticationContext,
                            authorizationToEvaluate);
                }
                catch (RuntimeException re) {
                    log.error("Got Exception when invoking SessionAuthenticator.initialAuthentication(..),"
                            + " Authorization header: " + authorizationToEvaluate, re);
                    closeWithPolicyViolation("Initial Auth-eval: Got Exception");
                    return false;
                }
                if (authenticationResult instanceof AuthenticationResult_Authenticated) {
                    // -> Authenticated!
                    goodAuthentication(authorizationToEvaluate,
                            (AuthenticationResult_Authenticated) authenticationResult, "Authenticated");
                    return true;
                }
                else {
                    // -> InvalidAuthentication, null, or any other result.
                    badAuthentication(authorizationToEvaluate, authenticationResult, "Initial Auth-eval");
                    return false;
                }
            }
            else {
                // -> Yes, we already have Principal
                // Ask SessionAuthenticator whether he still is happy with this Principal being authenticated, or
                // supplies a new Principal
                String authorizationToEvaluate = (newAuthorization != null ? newAuthorization : _authorization);
                if (authorizationToEvaluate == null) {
                    throw new AssertionError("We have not gotten neither an existing or a new"
                            + " Authorization Header.");
                }
                AuthenticationResult authenticationResult;
                try {
                    authenticationResult = _sessionAuthenticator.reevaluateAuthentication(_authenticationContext,
                            authorizationToEvaluate, _principal);
                }
                catch (RuntimeException re) {
                    log.error("Got Exception when invoking SessionAuthenticator.reevaluateAuthentication(..),"
                            + " Authorization header: " + authorizationToEvaluate, re);
                    closeWithPolicyViolation("Auth re-eval: Got Exception.");
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
                    _lastAuthenticatedMillis = System.currentTimeMillis();
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

    private void goodAuthentication(String authorizationToEvaluate,
            AuthenticationResult_Authenticated authenticationResult,
            String what) {
        AuthenticationResult_Authenticated result = authenticationResult;
        _authorization = authorizationToEvaluate;
        _principal = result._principal;
        _userId = result._userId;
        _lastAuthenticatedMillis = System.currentTimeMillis();
        // We can now send outgoing messages
        _holdOutgoingMessages = false;
        MDC.put(MDC_PRINCIPAL_NAME, _principal.getName());
        MDC.put(MDC_USER_ID, _userId);
        log.info(what + " with UserId: [" + _userId + "] and Principal [" + _principal + "]");
    }

    private void badAuthentication(String authorizationToEvaluate, AuthenticationResult authenticationResult,
            String what) {
        log.error("SessionAuthenticator replied " + authenticationResult + ", Authorization header: "
                + authorizationToEvaluate);
        // ?: Is it NotAuthenticated?
        if (authenticationResult instanceof AuthenticationResult_NotAuthenticated) {
            // -> Yes, explicit NotAuthenticated
            AuthenticationResult_NotAuthenticated invalidAuthentication = (AuthenticationResult_NotAuthenticated) authenticationResult;
            closeWithPolicyViolation(what + ": " + invalidAuthentication.getReason());
        }
        else {
            // -> Null or some unexpected value - no good.
            closeWithPolicyViolation(what + ": Failed");
        }
    }

    private boolean handleHello(long clientMessageReceivedTimestamp, MatsSocketEnvelopeDto envelope) {
        log.info("Handling HELLO:" + envelope.st + "!");
        // ?: Auth is required - should already have been processed
        if ((_principal == null) || (_authorization == null) || (_userId == null)) {
            // NOTE: This shall really never happen, as the implicit state machine should not have put us in this
            // situation. But just as an additional check.
            closeWithPolicyViolation("Missing authentication when evaluating HELLO message");
            return false;
        }
        // We've received hello.
        _helloReceived = true;

        _clientLibAndVersion = envelope.clv;
        if (_clientLibAndVersion == null) {
            closeWithProtocolError("Missing ClientLibAndVersion (clv) in HELLO envelope.");
            return false;
        }
        MDC.put(MDC_CLIENT_LIB_AND_VERSIONS, _clientLibAndVersion);
        String appName = envelope.an;
        if (appName == null) {
            closeWithProtocolError("Missing AppName (an) in HELLO envelope.");
            return false;
        }
        String appVersion = envelope.av;
        if (appVersion == null) {
            closeWithProtocolError("Missing AppVersion (av) in HELLO envelope.");
            return false;
        }
        _appNameAndVersion = appName + ";" + appVersion;

        // ----- HELLO was good (and authentication is already performed, earlier in process)

        // ?: Do the client want to reconnecting using existing MatsSocketSessionId
        if ("RECONNECT".equals(envelope.st)) {
            if (envelope.sid == null) {
                closeWithProtocolError("Got HELLO:RECONNECT, but missing MatsSocketSessionId"
                        + " (sid) in HELLO envelope.");
                return false;
            }
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
                // ?: If the existing is open, then close it.
                if (existingSession.get().getWebSocketSession().isOpen()) {
                    existingSession.get().closeWithProtocolError("Cannot have two MatsSockets with the same"
                            + " MatsSocketSessionId - closing the previous");
                }
                // You're allowed to use this, since the sessionId was already existing.
                _matsSocketSessionId = envelope.sid;
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
            closeWithPolicyViolation("UserId of existing SessionId does not match currently logged in user.");
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

        MatsSocketEnvelopeDto replyEnvelope = new MatsSocketEnvelopeDto();
        // Stack it up with props
        replyEnvelope.t = "WELCOME";
        replyEnvelope.st = ("RECONNECT".equals(envelope.st) ? "RECONNECTED" : "NEW");
        replyEnvelope.sid = _matsSocketSessionId;
        replyEnvelope.cid = envelope.cid;
        replyEnvelope.tid = envelope.tid;
        replyEnvelope.cmcts = envelope.cmcts;
        replyEnvelope.cmrts = clientMessageReceivedTimestamp;
        replyEnvelope.mscts = System.currentTimeMillis();
        replyEnvelope.mscnn = _matsSocketServer.getMyNodename();

        log.info("Sending WELCOME:" + replyEnvelope.st + ", MatsSocketSessionId:[" + _matsSocketSessionId + "]!");

        // Pack it over to client
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
        String eid = envelope.eid;
        MessageType messageType = MessageType.valueOf(envelope.t);

        log.info("  \\- " + messageType + " to:[" + eid + "], reply:[" + envelope.reid + "], msg:["
                + envelope.msg + "].");

        // :: Assert some props.
        if (envelope.cmid == null) {
            closeWithProtocolError("Missing 'cmid' on " + envelope.t + ".");
            return false;
        }
        if (envelope.tid == null) {
            closeWithProtocolError("Missing 'tid' (TraceId) on " + envelope.t + ", cmid:[" + envelope.cmid + "].");
            return false;
        }
        if ((messageType == MessageType.REPLY) && (envelope.smid == null)) {
            closeWithProtocolError("Missing 'smid' on a REPLY from Client, cmid:[" + envelope.cmid + "].");
            return false;
        }

        MatsSocketEnvelopeDto handledEnvelope = new MatsSocketEnvelopeDto();
        // .. add common props on the received message
        commonPropsOnReceived(envelope, handledEnvelope, clientMessageReceivedTimestamp);

        Optional<MatsSocketEndpointRegistration<?, ?, ?, ?>> registrationO = _matsSocketServer
                .getMatsSocketEndpointRegistration(eid);
        // ?: Check if we found the endpoint
        if (!registrationO.isPresent()) {
            handledEnvelope.t = "RECEIVED";
            handledEnvelope.st = "NACK";
            handledEnvelope.desc = "An incoming REQUEST envelope targeted a non-existing MatsSocketEndpoint";
            // Add RECEIVED message to "queue"
            replyEnvelopes.add(handledEnvelope);
            return true;
        }
        MatsSocketEndpointRegistration<?, ?, ?, ?> registration = registrationO.get();
        IncomingAuthorizationAndAdapter incomingAuthEval = registration.getIncomingAuthEval();
        log.info("MatsSocketEndpointHandler for [" + eid + "]: " + incomingAuthEval);

        Object msg = deserialize((String) envelope.msg, registration.getMsIncomingClass());

        // :: Perform the entire handleIncoming(..) inside Mats initiate-lambda
        RequestCorrelation[] _correlationInfo_LambdaHack = new RequestCorrelation[1];
        try {
            _matsSocketServer.getMatsFactory().getDefaultInitiator().initiateUnchecked(init -> {

                String correlationString = null;
                byte[] correlationBinary = null;
                // ?: If this is a REPLY, we'll get-and-delete the Correlation information.
                if (messageType == MessageType.REPLY) {
                    try {
                        Optional<RequestCorrelation> correlationInfoO = _matsSocketServer.getClusterStoreAndForward()
                                .getAndDeleteRequestCorrelation(_matsSocketSessionId, envelope.smid);
                        if (!correlationInfoO.isPresent()) {
                            throw new DuplicateDeliveryException("Missing Correlation information for REPLY for smid:["
                                    + envelope.smid + "] - this implies double delivery.");
                        }
                        RequestCorrelation correlationInfo = correlationInfoO.get();
                        // Store it for half-assed attempt at un-fucking the situation if we get "VERY BAD!"-situation.
                        _correlationInfo_LambdaHack[0] = correlationInfo;
                        log.info("Incoming REPLY for Server-to-Client Request for smid[" + envelope.smid
                                + "], time since request: [" + (System.currentTimeMillis() - correlationInfo
                                        .getRequestTimestamp()) + " ms].");
                        correlationString = correlationInfo.getCorrelationString();
                        correlationBinary = correlationInfo.getCorrelationBinary();
                    }
                    catch (DataAccessException e) {
                        throw new DatabaseRuntimeException(
                                "Got problems trying to get Correlation information for REPLY for smid:["
                                        + envelope.smid + "].", e);
                    }
                }

                MatsSocketEndpointRequestContextImpl<?, ?, ?> requestContext = new MatsSocketEndpointRequestContextImpl(
                        _matsSocketServer, registration, _matsSocketSessionId, init, envelope,
                        clientMessageReceivedTimestamp, _authorization, _principal, _userId, messageType,
                        correlationString, correlationBinary, msg);

                incomingAuthEval.handleIncoming(requestContext, _principal, msg);
                // ?: If we insta-settled the request, then do a REPLY
                switch (requestContext._handled) {
                    case IGNORED:
                        if (messageType == MessageType.REQUEST) {
                            handledEnvelope.t = "RECEIVED";
                            handledEnvelope.st = "NACK";
                            handledEnvelope.desc = "An incoming REQUEST envelope was ignored by the MatsSocket incoming handler.";
                            log.warn("handleIncoming(..) ignored an incoming REQUEST, i.e. not answered at all."
                                    + " Replying with [RECEIVED:NACK] to reject the outstanding request promise");
                        }
                        else {
                            handledEnvelope.t = "RECEIVED";
                            handledEnvelope.st = "ACK";
                            log.info("handleIncoming(..) evidently ignored the incoming SEND envelope. Responding"
                                    + " [RECEIVED:ACK], since that is OK.");
                        }
                        break;
                    case DENIED:
                        handledEnvelope.t = "RECEIVED";
                        handledEnvelope.st = "NACK";
                        log.info("handleIncoming(..) denied the incoming message. Replying with"
                                + " [RECEIVED:NACK]");
                        break;
                    case SETTLED_RESOLVE:
                    case SETTLED_REJECT:
                        // -> Yes, the handleIncoming insta-settled the incoming message, so we insta-reply
                        // NOTICE: We thus elide the "RECEIVED", as the client will handle the missing RECEIVED
                        handledEnvelope.t = MessageType.REPLY.name();
                        handledEnvelope.st = requestContext._handled == Processed.SETTLED_RESOLVE
                                ? "RESOLVE"
                                : "REJECT";
                        handledEnvelope.msg = requestContext._matsSocketReplyMessage;
                        log.info("handleIncoming(..) insta-settled the incoming message with"
                                + " [REPLY:" + handledEnvelope.st + "]");
                        break;
                    case FORWARDED:
                        handledEnvelope.t = "RECEIVED";
                        handledEnvelope.st = "ACK";
                        // TODO: Debug
                        handledEnvelope.mmsts = System.currentTimeMillis();
                        log.info("handleIncoming(..) forwarded the incoming message. Replying with"
                                + " [RECEIVED:" + handledEnvelope.st + "]");
                        break;
                }
            });
        }
        catch (DuplicateDeliveryException e) {
            // Double delivery: Simply say "yes, yes, good, good" to client, as we have already processed this one.
            log.info("We have evidently got a double-delivery for ClientMessageId [" + envelope.cmid
                    + "] of type [" + envelope.t + (envelope.st != null ? ':' + envelope.st : "")
                    + "], fixing by RECEIVED:ACK it again (it's already processed).");
            handledEnvelope.t = "RECEIVED";
            handledEnvelope.st = "ACK";
            handledEnvelope.mmsts = System.currentTimeMillis();
        }
        catch (DatabaseRuntimeException e) {
            // Problems adding the ClientMessageId to outbox. Ask client to RETRY.
            log.warn("Got problems storing incoming ClientMessageId in inbox - replying RECEIVED:RETRY to client.",
                    e);
            handledEnvelope.t = "RETRY";
            handledEnvelope.desc = e.getMessage();
        }
        catch (MatsBackendRuntimeException e) {
            // Evidently got problems talking to Mats backend, probably DB commit fail. Ask client to RETRY.
            log.warn("Got problems running handleIncoming(..), probably due to DB - replying RECEIVED:RETRY to client.",
                    e);
            handledEnvelope.t = "RETRY";
            handledEnvelope.desc = e.getMessage();
        }
        catch (MatsMessageSendRuntimeException e) {
            // Evidently got problems talking to MQ, aka "VERY BAD!". Trying to do compensating tx, then client RETRY
            log.warn("Got major problems running handleIncoming(..) due to DB committing, but MQ not committing."
                    + " Now trying compensating transaction - deleting from inbox (SEND or REQUEST) or"
                    + " re-inserting Correlation info (REPLY) - then replying RETRY to client.", e);

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
                                        c.getRequestTimestamp(), c.getCorrelationString(), c.getCorrelationBinary());
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

            handledEnvelope.t = "RETRY";
            handledEnvelope.desc = e.getMessage();
        }
        catch (Throwable t) {
            // Evidently the handleIncoming didn't handle this message. This is a NACK.
            log.warn("handleIncoming(..) raised exception, assuming that it didn't like the incoming message"
                    + " - replying RECEIVED:NACK to client.", t);
            handledEnvelope.t = "RECEIVED";
            handledEnvelope.st = "NACK";
            handledEnvelope.desc = t.getMessage();
        }

        // Add RECEIVED message to "queue"
        replyEnvelopes.add(handledEnvelope);

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

    private enum Processed {
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

        private final String _correlationString;
        private final byte[] _correlationBinary;
        private final I _incomingMessage;

        private final MessageType _messageType;

        public MatsSocketEndpointRequestContextImpl(DefaultMatsSocketServer matsSocketServer,
                MatsSocketEndpointRegistration matsSocketEndpointRegistration, String matsSocketSessionId,
                MatsInitiate matsInitiate,
                MatsSocketEnvelopeDto envelope, long clientMessageReceivedTimestamp, String authorization,
                Principal principal, String userId, MessageType messageType,
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
        public boolean isReplyResolve() {
            return _envelope.t.equals("REPLY") && _envelope.st.equals("RESOLVE");
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

            // ?: Is this a REQUEST or SEND, meaning that we do not have a Correlation entry in the CSAF?
            if ((getMessageType() == MessageType.REQUEST) || (getMessageType() == MessageType.SEND)) {
                // -> Yes, REQUEST or SEND: Store the ClientMessageId in the Inbox, to catch double deliveries.
                /*
                 * This shall throw UniqueConstraintException if we've already processed this before.
                 *
                 * Notice: It MIGHT be that the SQLIntegrityConstraintViolationException (or similar) is not raised
                 * until commit due to races, albeit this seems rather far-fetched considering that there shall not be
                 * any concurrent handling of this particular MatsSocketSessionId. Anyway, failure on commit will lead
                 * to the Mats stage to throw, and the client shall then end up with redelivery. When redelivered, the
                 * other message should already be in place, and we should get the unique constraint violation right
                 * here.
                 */
                try {
                    _matsSocketServer.getClusterStoreAndForward().storeMessageIdInInbox(_matsSocketSessionId,
                            _envelope.cmid);
                }
                catch (ClientMessageIdAlreadyExistsException e) {
                    throw new DuplicateDeliveryException(e);
                }
                catch (DataAccessException e) {
                    throw new DatabaseRuntimeException(e);
                }
            }

            MatsInitiate init = _matsInitiate;
            init.from("MatsSocketEndpoint." + _envelope.eid)
                    .traceId(_envelope.tid);
            // Add a small extra side-load - the MatsSocketSessionId - since it seems nice.
            init.addString("matsSocketSessionId", _matsSocketSessionId);
            // -> Is this a REQUEST?
            if (getMessageType() == MessageType.REQUEST) {
                // -> Yes, this is a REQUEST, so we should forward as Mats .request(..)
                // Need to make state so that receiving terminator know what to do.
                ReplyHandleStateDto sto = new ReplyHandleStateDto(_matsSocketSessionId,
                        _matsSocketEndpointRegistration.getMatsSocketEndpointId(), _envelope.reid,
                        _envelope.cid, _envelope.cmid, _envelope.cmcts, _clientMessageReceivedTimestamp,
                        System.currentTimeMillis(), _matsSocketServer.getMyNodename());
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
            if (getMessageType() != MessageType.REQUEST) {
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
            if (getMessageType() != MessageType.REQUEST) {
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
