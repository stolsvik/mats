package com.stolsvik.mats.websocket.impl;

import java.io.IOException;
import java.io.Writer;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCode;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.MessageHandler.Whole;
import javax.websocket.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsInitiator.MatsBackendRuntimeException;
import com.stolsvik.mats.MatsInitiator.MatsMessageSendRuntimeException;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEndpointIncomingAuthEval;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEndpointRequestContext;
import com.stolsvik.mats.websocket.impl.ClusterStoreAndForward.DataAccessException;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer.MatsSocketEndpointRegistration;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer.ReplyHandleStateDto;

/**
 * @author Endre Stølsvik 2019-11-28 12:17 - http://stolsvik.com/, endre@stolsvik.com
 */
class MatsSocketSession implements Whole<String> {
    private static final Logger log = LoggerFactory.getLogger(MatsSocketSession.class);
    private static final JavaType LIST_OF_MSG_TYPE = TypeFactory.defaultInstance().constructType(
            new TypeReference<List<MatsSocketEnvelopeDto>>() {
            });

    private final Session _webSocketSession;
    private final String _connectionId;

    // Derived
    private final DefaultMatsSocketServer _matsSocketServer;

    // Set
    private String _matsSocketSessionId;
    private String _authorization;
    private Principal _principal;

    MatsSocketSession(DefaultMatsSocketServer matsSocketServer, Session webSocketSession) {
        _webSocketSession = webSocketSession;
        _connectionId = webSocketSession.getId() + "_" + DefaultMatsSocketServer.rnd(10);

        // Derived
        _matsSocketServer = matsSocketServer;
    }

    String getId() {
        return _matsSocketSessionId;
    }

    Session getWebSocketSession() {
        return _webSocketSession;
    }

    String getConnectionId() {
        return _connectionId;
    }

    @Override
    public void onMessage(String message) {
        long clientMessageReceivedTimestamp = System.currentTimeMillis();
        log.info("WebSocket received message:" + message + ", session:" + _webSocketSession.getId() + ", this:"
                + DefaultMatsSocketServer.id(this));

        List<MatsSocketEnvelopeDto> envelopes;
        try {
            envelopes = _matsSocketServer.getJackson().readValue(message, LIST_OF_MSG_TYPE);
        }
        catch (JsonProcessingException e) {
            // TODO: Handle parse exceptions.
            throw new AssertionError("Parse exception", e);
        }

        log.info("Messages: " + envelopes);
        List<MatsSocketEnvelopeDto> replyEnvelopes = new ArrayList<>();
        boolean shouldNotifyAboutExisting = false;
        String shouldCloseSession = null;
        for (int i = 0; i < envelopes.size(); i++) {
            MatsSocketEnvelopeDto envelope = envelopes.get(i);

            // ?: Pick out any Authorization header, i.e. the auth-string - it can come in any message.
            if (envelope.auth != null) {
                // -> Yes, there was an authorization string here
                _principal = _matsSocketServer.getAuthorizationToPrincipalFunction().apply(envelope.auth);
                if (_principal == null) {
                    // TODO: SEND AUTH_FAILED (also if auth function throws)
                    throw new AssertionError("The authorization header [" + DefaultMatsSocketServer.escape(
                            envelope.auth)
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
                    log.info("MatsSocketSession Reconnect requested to MatsSocketSessionId [" + envelope.sid + "]");
                    // -> Yes, try to find it

                    // TODO: Implement remote invalidation

                    // :: Local invalidation of existing session.
                    Optional<MatsSocketSession> existingSession = _matsSocketServer
                            .getRegisteredLocalMatsSocketSession(envelope.sid);
                    // ?: Is there an existing local Session?
                    if (existingSession.isPresent()) {
                        log.info(" \\- Existing LOCAL Session found!");
                        // -> Yes, thus you can use it.
                        /*
                         * NOTE: If it is open - which it "by definition" should not be - we close the *previous*. The
                         * question of whether to close this or previous: We chose previous because there might be
                         * reasons where the client feels that it has lost the connection, but the server hasn't yet
                         * found out. The client will then try to reconnect, and that is ok. So we close the existing.
                         * Since it is always the server that creates session Ids and they are large and globally
                         * unique, AND since we've already authenticated the user so things should be OK, this ID is
                         * obviously the one the client got the last time. So if he really wants to screw up his life by
                         * doing reconnects when he does not need to, then OK.
                         */
                        // ?: If the existing is open, then close it.
                        if (existingSession.get()._webSocketSession.isOpen()) {
                            try {
                                existingSession.get()._webSocketSession.close(new CloseReason(
                                        CloseCodes.PROTOCOL_ERROR,
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
                    else {
                        log.info(" \\- No existing local Session found, check CSAF..");
                        // -> No, no local existing session, but is there an existing session in CSAF?
                        try {
                            boolean sessionExists = _matsSocketServer.getClusterStoreAndForward()
                                    .isSessionExists(envelope.sid);
                            // ?: Is there a CSAF Session?
                            if (sessionExists) {
                                log.info(" \\- Existing CSAF Session found!");
                                // -> Yes, there is a CSAF Session - so client can use this session
                                _matsSocketSessionId = envelope.sid;
                            }
                            else {
                                log.info(" \\- No existing Session found..");
                            }
                        }
                        catch (DataAccessException e) {
                            // TODO: Fixup
                            throw new AssertionError("Damn.", e);
                        }
                    }
                }

                // ?: Do we have a MatsSocketSessionId by now?
                if (_matsSocketSessionId == null) {
                    // -> No, so make one.
                    _matsSocketSessionId = DefaultMatsSocketServer.rnd(16);
                }

                // Add Session to our active-map
                _matsSocketServer.registerLocalMatsSocketSession(this);
                try {
                    _matsSocketServer.getClusterStoreAndForward().registerSessionAtThisNode(_matsSocketSessionId,
                            _connectionId);
                }
                catch (DataAccessException e) {
                    // TODO: Fix
                    throw new AssertionError("Damn", e);
                }

                // ----- We're now a live MatsSocketSession

                // :: Create reply WELCOME message

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
                // Add WELCOME reply message to "queue"
                replyEnvelopes.add(replyEnvelope);

                // Notify ourselves about "new" (as in existing) messages, just in case there are any.
                shouldNotifyAboutExisting = true;

                continue;
            }

            if ("PING".equals(envelope.t)) {
                // TODO: HANDLE PING
                continue;
            }

            if ("CLOSE_SESSION".equals(envelope.t)) {
                // Local deregister
                _matsSocketServer.deregisterLocalMatsSocketSession(_matsSocketSessionId, _connectionId);
                try {
                    // CSAF terminate
                    _matsSocketServer.getClusterStoreAndForward().terminateSession(_matsSocketSessionId);
                }
                catch (DataAccessException e) {
                    // TODO: Fix
                    throw new AssertionError("Damn", e);
                }
                shouldCloseSession = (envelope.desc != null ? envelope.desc : "");
                // Won't do rest of pipeline of messages, as it seems rather absurd to stick more after a close session.
                break;
            }

            // ----- We do NOT KNOW whether we're authenticated!

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
                MatsSocketEndpointRegistration<?, ?, ?, ?> registration = _matsSocketServer
                        .getMatsSocketEndpointRegistration(eid);
                MatsSocketEndpointIncomingAuthEval incomingAuthEval = registration.getIncomingAuthEval();
                log.info("MatsSocketEndpointHandler for [" + eid + "]: " + incomingAuthEval);

                Object msg = deserialize((String) envelope.msg, registration.getMsIncomingClass());
                MatsSocketEndpointRequestContextImpl<?, ?> matsSocketContext = new MatsSocketEndpointRequestContextImpl(
                        _matsSocketServer, registration, _matsSocketSessionId, envelope,
                        clientMessageReceivedTimestamp, _authorization, _principal, msg);

                try {
                    incomingAuthEval.handleIncoming(matsSocketContext, _principal, msg);
                }
                catch (MatsBackendRuntimeException | MatsMessageSendRuntimeException e) {
                    // Evidently got problems talking to MQ. This is a SERVER_ERROR
                    // TODO: If this throws, send error back.
                }
                long nowMillis = System.currentTimeMillis();

                // :: Send RECEIVED message right away.
                // TODO: NO, not right away! Let's at least pipeline the same pipeline we are receiving.
                // TODO: Actually, should only acknowledge the last one, implicitly acknowledging the others.
                MatsSocketEnvelopeDto replyEnvelope = new MatsSocketEnvelopeDto();
                replyEnvelope.t = "RECEIVED";
                replyEnvelope.st = "ACK"; // TODO: Handle failures.
                replyEnvelope.mseq = envelope.mseq; //
                // Note: Not setting MatsSocketSessionId - Client already has this
                replyEnvelope.tid = envelope.tid; // TraceId
                replyEnvelope.cid = envelope.cid; // CorrelationId
                replyEnvelope.cmcts = envelope.cmcts; // Set by client..
                replyEnvelope.cmrts = clientMessageReceivedTimestamp;
                replyEnvelope.cmrnn = _matsSocketServer.getMyNodename();
                replyEnvelope.mmsts = nowMillis;
                // Note: Not setting Reply Message to Client Timestamp and Nodename: This ain't no Reply.

                // Add RECEIVED message to "queue"
                replyEnvelopes.add(replyEnvelope);

                continue;
            }
        }

        // TODO: Store last messageSequenceId

        // Send all replies
        if (replyEnvelopes.size() > 0) {
            try {
                Writer sendWriter = _webSocketSession.getBasicRemote().getSendWriter();
                // Evidently this closes the Writer..
                _matsSocketServer.getJackson().writeValue(sendWriter, replyEnvelopes);
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
        if (shouldNotifyAboutExisting) {
            // -> Yes, so do it now.
            _matsSocketServer.getMessageToWebSocketForwarder().notifyMessageFor(this);
        }
        // ?: Should we close the session?
        if (shouldCloseSession != null) {
            closeWebSocket(CloseCodes.NORMAL_CLOSURE, "From Server: Client said CLOSE_SESSION (" +
                    DefaultMatsSocketServer.escape(shouldCloseSession)
                    + "): Terminated MatsSocketSession, closing WebSocket.");
        }
    }

    void closeWebSocket(CloseCode closeCode, String reasonPhrase) {
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
            return _matsSocketServer.getJackson().readValue(serialized, clazz);
        }
        catch (JsonProcessingException e) {
            // TODO: Handle parse exceptions.
            throw new AssertionError("Damn", e);
        }
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
            _matsSocketServer.getMatsFactory().getDefaultInitiator().initiateUnchecked(init -> {
                init.from("MatsSocketEndpoint." + _envelope.eid)
                        .traceId(_envelope.tid);
                if (isRequest()) {
                    ReplyHandleStateDto sto = new ReplyHandleStateDto(_matsSocketSessionId,
                            _matsSocketEndpointRegistration.getMatsSocketEndpointId(), _envelope.reid,
                            _envelope.cid, _envelope.mseq, _envelope.cmcts, _clientMessageReceivedTimestamp,
                            System.currentTimeMillis(), _matsSocketServer.getMyNodename());
                    // Set ReplyTo parameter
                    init.replyTo(_matsSocketServer.getReplyTerminatorId(), sto);
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
        public void reply(R matsSocketReplyMessage) {
            // TODO: Implement
            throw new IllegalStateException("Not yet implemented.");
        }
    }
}
