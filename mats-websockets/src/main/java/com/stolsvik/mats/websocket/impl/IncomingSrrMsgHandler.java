package com.stolsvik.mats.websocket.impl;

import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.ACK;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.NACK;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.REJECT;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.REQUEST;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.RESOLVE;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.RETRY;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.SEND;

import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsInitiator.MatsBackendRuntimeException;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;
import com.stolsvik.mats.MatsInitiator.MatsInitiateWrapper;
import com.stolsvik.mats.MatsInitiator.MatsMessageSendRuntimeException;
import com.stolsvik.mats.websocket.AuthenticationPlugin.DebugOption;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.ClientMessageIdAlreadyExistsException;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.DataAccessException;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.RequestCorrelation;
import com.stolsvik.mats.websocket.ClusterStoreAndForward.StoredInMessage;
import com.stolsvik.mats.websocket.MatsSocketServer.ActiveMatsSocketSession.MatsSocketSessionState;
import com.stolsvik.mats.websocket.MatsSocketServer.IncomingAuthorizationAndAdapter;
import com.stolsvik.mats.websocket.MatsSocketServer.LiveMatsSocketSession;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketCloseCodes;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEndpoint;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEndpointIncomingContext;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeDto.DebugDto;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto.Direction;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto.IncomingResolution;
import com.stolsvik.mats.websocket.MatsSocketServer.MessageType;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer.MatsSocketEndpointRegistration;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer.ReplyHandleStateDto;

/**
 * Incoming (Client-to-Server) Send, Request and Replies (Resolve and Reject) handler.
 *
 * @author Endre Stølsvik 2020-05-23 11:59 - http://stolsvik.com/, endre@stolsvik.com
 */
public class IncomingSrrMsgHandler implements MatsSocketStatics {

    private static final Logger log = LoggerFactory.getLogger(IncomingSrrMsgHandler.class);

    private final DefaultMatsSocketServer _matsSocketServer;

    private final SaneThreadPoolExecutor _threadPool;

    public IncomingSrrMsgHandler(DefaultMatsSocketServer matsSocketServer, int corePoolSize,
            int maxPoolSize) {
        _matsSocketServer = matsSocketServer;
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

    void handleSendOrRequestOrReply(MatsSocketSessionAndMessageHandler session, long receivedTimestamp,
            long nanosStart, MatsSocketEnvelopeWithMetaDto envelope) {
        /*
         * Note that when handed over to the ThreadPool, we're in async-land. This implies that the MatsSocketSession
         * can be invalidated before the ThreadPool even gets a chance to start executing the code. This was caught in a
         * unit test, whereby the session.getPrincipal() returned Optional.empty() when it was invoked.
         *
         * Pick out the parts that are needed to make the handler context and invoke the handler, which are guaranteed
         * to be here when still in synchronous mode.
         */

        String authorization = session.getAuthorization()
                .orElseThrow(() -> new AssertionError("Authorization should be here at this point."));
        Principal principal = session.getPrincipal()
                .orElseThrow(() -> new AssertionError("Principal should be here at this point."));

        _threadPool.execute(() -> handlerRunnable(session, receivedTimestamp, nanosStart, envelope, authorization,
                principal));
    }

    void handlerRunnable(MatsSocketSessionAndMessageHandler session, long receivedTimestamp,
            long nanosStart, MatsSocketEnvelopeWithMetaDto envelope, String authorization, Principal principal) {

        String matsSocketSessionId = session.getMatsSocketSessionId();
        MessageType type = envelope.t;

        // Hack for lamba processing
        MatsSocketEnvelopeWithMetaDto[] handledEnvelope = new MatsSocketEnvelopeWithMetaDto[] {
                new MatsSocketEnvelopeWithMetaDto() };
        handledEnvelope[0].cmid = envelope.cmid; // Client MessageId.

        // :: Perform the entire handleIncoming(..) inside Mats initiate-lambda
        RequestCorrelation[] _correlationInfo_LambdaHack = new RequestCorrelation[1];
        try {
            _matsSocketServer.getMatsFactory().getDefaultInitiator().initiateUnchecked(init -> {

                // ===== PRE message handling.

                // NOTE NOTE!! THIS IS WITHIN THE TRANSACTIONAL DEMARCATION OF THE MATS INITIATION!! NOTE NOTE!!

                String targetEndpointId;

                String correlationString = null;
                byte[] correlationBinary = null;

                // ?: (Pre-message-handling state-mods) Is it a Client-to-Server REQUEST or SEND?
                if ((type == REQUEST) || (type == SEND)) {
                    // -> Yes, this is a Client-to-Server REQUEST or SEND:

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
                     *
                     * Also notice: If we get "VERY BAD!", we try to perform compensating transaction.
                     */
                    try {
                        _matsSocketServer.getClusterStoreAndForward().storeMessageIdInInbox(matsSocketSessionId,
                                envelope.cmid);
                    }
                    catch (DataAccessException e) {
                        // DB-Problems: Throw out of the lambda, handled outside, letting Mats do rollback.
                        throw new DatabaseRuntimeException("Got problems when trying to store incoming " + type
                                + " Client Message Id [" + envelope.cmid + "] in CSAF Inbox", e);
                    }
                    catch (ClientMessageIdAlreadyExistsException e) {
                        // -> Already have this in the inbox, so this is a dupe
                        // Double delivery: Fetch the answer we said last time, and just answer that!

                        // :: Fetch previous answer if present and answer that, otherwise answer default; ACK.
                        StoredInMessage messageFromInbox;
                        try {
                            messageFromInbox = _matsSocketServer.getClusterStoreAndForward()
                                    .getMessageFromInbox(matsSocketSessionId, envelope.cmid);
                        }
                        catch (DataAccessException ex) {
                            // DB-Problems: Throw out of the lambda, handled outside, letting Mats do rollback.
                            throw new DatabaseRuntimeException("Got problems when trying to store incoming " + type
                                    + " Client Message Id [" + envelope.cmid + "] in CSAF Inbox", e);
                        }

                        // ?: Did we have a serialized message here?
                        if (!messageFromInbox.getFullEnvelope().isPresent()) {
                            // -> We did NOT have a previous JSON stored, which means that it was the default: ACK
                            handledEnvelope[0].t = MessageType.ACK;
                            // Note that it was a dupe in desc-field
                            handledEnvelope[0].desc = "dupe " + envelope.t + " ACK";
                            log.info("We have evidently got a double-delivery for ClientMessageId [" + envelope.cmid
                                    + "] of type [" + envelope.t + "] - it was NOT stored, thus it was an ACK.");
                        }
                        else {
                            // -> Yes, we had the JSON from last processing stored!
                            log.info("We had an envelope from last time!");
                            // :: We'll just reply whatever we replied previous time.
                            // Deserializing the Envelope
                            // NOTE: Will get any message ('msg'-field) as a String directly representing JSON.
                            MatsSocketEnvelopeWithMetaDto previousReplyEnvelope;
                            try {
                                previousReplyEnvelope = _matsSocketServer
                                        .getEnvelopeObjectReader().readValue(messageFromInbox.getFullEnvelope().get());
                            }
                            catch (JsonProcessingException ex) {
                                throw new AssertionError("Could not deserialize. This should not happen.", ex);
                            }

                            // Doctor the deserialized envelope: The 'msg' field is currently a proper JSON String,
                            // we want it re-serialized directly as-is, thus use "magic" DirectJson class.
                            previousReplyEnvelope.msg = DirectJson.of((String) previousReplyEnvelope.msg);
                            // Now just REPLACE the existing handledEnvelope with the old one.
                            handledEnvelope[0] = previousReplyEnvelope;
                            // Note that it was a dupe in desc-field
                            handledEnvelope[0].desc = "dupe " + envelope.t + " stored";
                            log.info("We have evidently got a double-delivery for ClientMessageId [" + envelope.cmid
                                    + "] of type [" + envelope.t + "] - we had it stored, so just replying the"
                                    + " previous answer again.");
                        }
                        // Return from Mats-initiate lambda - We're done here.
                        return;
                    }
                }
                // ?: (Pre-message-handling state-mods) Is this a Client Reply (RESOLVE or REJECT) to S2C Request?
                else if ((type == RESOLVE) || (type == REJECT)) {
                    // -> Yes, Reply (RESOLVE or REJECT), so we'll get-and-delete the Correlation information.
                    // Find the CorrelationInformation - or NOT, if this is a duplicate delivery.
                    try {
                        // REMEMBER!! THIS IS WITHIN THE MATS INITIATION TRANSACTION!!
                        // Therefore: Delete won't go through unless entire message handling goes through.
                        // Also: If we get "VERY BAD!", we try to do compensating transaction.
                        Optional<RequestCorrelation> correlationInfoO = _matsSocketServer.getClusterStoreAndForward()
                                .getAndDeleteRequestCorrelation(matsSocketSessionId, envelope.smid);
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
                        // TODO: Do something with these exceptions - more types?
                        throw new DatabaseRuntimeException("Got problems trying to get Correlation information for"
                                + " REPLY for smid:[" + envelope.smid + "].", e);
                    }
                }
                else {
                    throw new AssertionError("Received an unhandled message type [" + type + "].");
                }

                // ===== Message handling.

                // Go get the Endpoint registration.
                Optional<MatsSocketEndpointRegistration<?, ?, ?>> registrationO = _matsSocketServer
                        .getMatsSocketEndpointRegistration(targetEndpointId);

                // ?: Check if we found the endpoint
                if (!registrationO.isPresent()) {
                    // -> No, unknown MatsSocket EndpointId.
                    handledEnvelope[0].t = NACK;
                    handledEnvelope[0].desc = "An incoming " + envelope.t
                            + " envelope targeted a non-existing MatsSocketEndpoint";
                    log.warn("Unknown MatsSocketEndpointId [" + targetEndpointId + "] for incoming envelope "
                            + envelope);
                    // Return from Mats-initiate lambda - We're done here.
                    return;
                }

                MatsSocketEndpointRegistration<?, ?, ?> registration = registrationO.get();

                // -> Developer-friendliness assert for Client REQUESTs going to a Terminator (which won't ever Reply).
                if ((type == REQUEST) &&
                        ((registration.getReplyClass() == Void.class) || (registration.getReplyClass() == Void.TYPE))) {
                    handledEnvelope[0].t = NACK;
                    handledEnvelope[0].desc = "An incoming REQUEST envelope targeted a MatsSocketEndpoint which is a"
                            + " Terminator, i.e. it won't ever reply";
                    log.warn("MatsSocketEndpointId targeted by Client REQUEST is a Terminator [" + targetEndpointId
                            + "] for incoming envelope " + envelope);
                    // Return from Mats-initiate lambda - We're done here.
                    return;
                }

                // Deserialize the message with the info from the registration
                Object msg = deserializeIncomingMessage((String) envelope.msg, registration.getIncomingClass());

                // ===== Actually invoke the IncomingAuthorizationAndAdapter.handleIncoming(..)

                // .. create the Context
                @SuppressWarnings({ "unchecked", "rawtypes" })
                MatsSocketEndpointIncomingContextImpl<?, ?, ?> requestContext = new MatsSocketEndpointIncomingContextImpl(
                        _matsSocketServer, registration, matsSocketSessionId, init, envelope,
                        receivedTimestamp, session, authorization, principal,
                        type, correlationString, correlationBinary, msg);

                /*
                 * NOTICE: When we changed to async handling of incoming information bearing messages (using a thread
                 * pool), we can come in a situation where the MatsSocketSession has closed while we are trying to
                 * handle an incoming message. We will not accept processing for a dead MatsSocketSession, so we check
                 * for this after the handler has processed, and force rollback if this is the situation. However, if
                 * the handler did certain operations, e.g. 'context.getSession().getPrincipal().get()', this will lead
                 * to him /throwing/ out. Since in most cases throwing out should NACK the message, we check for this
                 * specific case in a catch-block here: If the handler threw out, AND the session is not
                 * SESSION_ESTABLISHED anymore, we assume that the reason is due to the explained situation, and hence
                 * rollback the entire message handling (and replying RETRY, but that will fail..!), instead of replying
                 * NACK.
                 */
                // .. invoke the handler
                try {
                    invokeHandleIncoming(registration, msg, requestContext);
                }
                catch (RuntimeException e) {
                    // ?: Is the session still in SESSION_ESTABLISHED?
                    if (session.getState() == MatsSocketSessionState.SESSION_ESTABLISHED) {
                        // -> Yes, so then this was "normal" user code exception, which should result in NACK.
                        throw e;
                    }
                    // E-> No, and then we go into default handling for /not/ SESSION_ESTABLISHED.
                }

                // ?: Is the session still in SESSION_ESTABLISHED?
                if (session.getState() != MatsSocketSessionState.SESSION_ESTABLISHED) {
                    // -> No, so then we tell the outside handling by throwing out, which also will lead to rollback of
                    // all db-operations above (which is vital, since we not have not handled this message after all).
                    throw new SessionLostException("Session is not SESSION_ESTABLISHED when exiting handler. Rolling"
                            + " back this message handling by throwing out of Mats Initiation.");
                }

                // Record the resolution in the incoming Envelope
                envelope.ir = requestContext._handled;

                // :: Based on the situation in the RequestContext, we return ACK/NACK/RETRY/RESOLVE/REJECT
                switch (requestContext._handled) {
                    case NO_ACTION:
                        // ?: Is this a REQUEST?
                        if (type == REQUEST) {
                            // -> Yes, REQUEST, so then it is not allowed to Ignore it.
                            handledEnvelope[0].t = NACK;
                            handledEnvelope[0].desc = "An incoming REQUEST envelope was ignored by the MatsSocket incoming handler.";
                            log.warn("handleIncoming(..) ignored an incoming REQUEST, i.e. not answered at all."
                                    + " Replying with [" + handledEnvelope[0]
                                    + "] to reject the outstanding request promise");
                        }
                        else {
                            // -> No, not REQUEST, i.e. either SEND, RESOLVE or REJECT, and then Ignore is OK.
                            handledEnvelope[0].t = ACK;
                            log.info("handleIncoming(..) evidently ignored the incoming SEND envelope. Responding"
                                    + " [" + handledEnvelope[0] + "], since that is OK.");
                        }
                        break;
                    case DENY:
                        handledEnvelope[0].t = NACK;
                        log.info("handleIncoming(..) denied the incoming message. Replying with"
                                + " [" + handledEnvelope[0] + "]");
                        break;
                    case RESOLVE:
                    case REJECT:
                        // -> Yes, the handleIncoming insta-settled the incoming message, so we insta-reply
                        // NOTICE: We thus elide the "RECEIVED", as the client will handle the missing RECEIVED
                        handledEnvelope[0].t = requestContext._handled == IncomingResolution.RESOLVE
                                ? RESOLVE
                                : REJECT;
                        // Add standard Reply message properties, since this is no longer just an ACK/NACK
                        handledEnvelope[0].tid = envelope.tid; // TraceId

                        // Handle DebugOptions
                        EnumSet<DebugOption> debugOptions = DebugOption.enumSetOf(envelope.rd);
                        debugOptions.retainAll(session.getAllowedDebugOptions());
                        if (!debugOptions.isEmpty()) {
                            DebugDto debug = new DebugDto();
                            if (debugOptions.contains(DebugOption.TIMESTAMPS)) {
                                debug.cmrts = receivedTimestamp;
                                debug.mscts = System.currentTimeMillis();
                            }
                            if (debugOptions.contains(DebugOption.NODES)) {
                                debug.cmrnn = _matsSocketServer.getMyNodename();
                                debug.mscnn = _matsSocketServer.getMyNodename();
                            }
                            handledEnvelope[0].debug = debug;
                        }

                        // NOTE: We serialize the message here, so that all sent envelopes use the DirectJson logic
                        String replyMessageJson = _matsSocketServer.serializeMessageObject(
                                requestContext._matsSocketReplyMessage);

                        // Set the message as DirectJson
                        handledEnvelope[0].msg = DirectJson.of(replyMessageJson);
                        log.info("handleIncoming(..) insta-settled the incoming message with"
                                + " [" + handledEnvelope[0].t + "]");
                        break;
                    case FORWARD:
                        handledEnvelope[0].t = ACK;
                        // Record the forwarded-to-Mats Endpoint as resolution.
                        envelope.fmeid = requestContext._forwardedMatsEndpoint;
                        log.info("handleIncoming(..) forwarded the incoming message to Mats Endpoint ["
                                + requestContext._forwardedMatsEndpoint + "]. Replying with"
                                + " [" + handledEnvelope[0] + "]");
                        break;
                }

                // ===== POST message handling.

                // :: NOW, if we got anything else than an ACK out of this, we must store the reply in the inbox
                // (ACK is default, and does not need storing - as an optimization)
                // ?: (Post-message-handling state-mods) Did we get anything else than ACK out of this?
                if (handledEnvelope[0].t != MessageType.ACK) {
                    // -> Yes, this was not ACK
                    log.debug("Got handledEnvelope of type [" + handledEnvelope[0].t + "], so storing it.");
                    try {
                        String envelopeJson = _matsSocketServer.getEnvelopeObjectWriter().writeValueAsString(
                                handledEnvelope[0]);
                        _matsSocketServer.getClusterStoreAndForward().updateMessageInInbox(matsSocketSessionId,
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
                // NOTE NOTE!! EXITING MATS INITIATION TRANSACTIONAL DEMARCATION!! NOTE NOTE!!
            });

        }
        catch (

        SessionLostException e) {
            // During handling, we found that the session was /not/ SESSION_ESTABLISHED anymore.
            envelope.ir = IncomingResolution.EXCEPTION;
            log.warn("Session evidently lost - replying RETRY to client (however, the sending of the message"
                    + " will probably also crash..).", e);
            // Futile attempt at telling the client to RETRY. Which will not work, since this WebSocket is closed..!
            handledEnvelope[0].t = RETRY;
            handledEnvelope[0].desc = e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        catch (DatabaseRuntimeException e) {
            // Problems adding the ClientMessageId to outbox. Ask client to RETRY.
            envelope.ir = IncomingResolution.EXCEPTION;
            // TODO: This log line is wrong.
            log.warn("Got problems storing incoming ClientMessageId in inbox - replying RETRY to client.",
                    e);
            handledEnvelope[0].t = RETRY;
            handledEnvelope[0].desc = e.getClass().getSimpleName() + ':' + e.getMessage();
        }
        catch (MatsBackendRuntimeException e) {
            // Evidently got problems talking to Mats backend, probably DB commit fail. Ask client to RETRY.
            envelope.ir = IncomingResolution.EXCEPTION;
            log.warn("Got problems running handleIncoming(..), probably due to DB - replying RETRY to client.",
                    e);
            handledEnvelope[0].t = RETRY;
            handledEnvelope[0].desc = e.getClass().getSimpleName() + ':' + e.getMessage();
        }
        catch (MatsMessageSendRuntimeException e) {
            // Evidently got problems talking to MQ, aka "VERY BAD!". Trying to do compensating tx, then client RETRY
            envelope.ir = IncomingResolution.EXCEPTION;
            log.warn("Got major problems running handleIncoming(..) due to DB committing, but MQ not committing."
                    + " Now trying compensating transaction - deleting from inbox (SEND or REQUEST) or"
                    + " re-inserting Correlation info (REPLY) - then replying RETRY to client.", e);

            /*
             * NOTICE! With "Outbox pattern" enabled on the MatsFactory, this exception shall never come. This because
             * if the sending to MQ does not work out, it will still have stored the message in the outbox, and the
             * MatsFactory will then get the message sent onto MQ at a later time, thus the need for this particular
             * exception is not there anymore, and will never be raised.
             */

            // :: Compensating transaction, i.e. delete that we've received the message (if SEND or REQUEST), or store
            // back the Correlation information (if REPLY), so that we can RETRY.
            // Go for a massively crude attempt to fix this if DB is down now: Retry the operation for some seconds.

            /*
             * NOTICE: ASYNC PROBLEM: Since we changed to async handling of incoming information bearing messages (using
             * a thread pool), there can be a race here if the MatsSocketSession also reconnects at the same time as
             * this screwup happened: The client could resubmit his messages, while we still have not performed the
             * compensating transaction. The message will then be assumed already handled, and we will answer that to
             * the client - and thus the message is gone forever... Since this is a handling of an extremely rare
             * situation, AND since the MatsSocket libs have a small delay before reconnecting, let's just cross our
             * fingers..
             */
            int retry = 0;
            while (true) {
                try {
                    // ?: Was this a Reply (RESOLVE or REJECT) (that wasn't a double-delivery)
                    if (_correlationInfo_LambdaHack[0] != null) {
                        // -> Yes, REPLY, so try to store back the Correlation Information since we did not handle
                        // it after all (so go for RETRY from Client).
                        RequestCorrelation c = _correlationInfo_LambdaHack[0];
                        _matsSocketServer.getClusterStoreAndForward()
                                .storeRequestCorrelation(matsSocketSessionId, envelope.smid,
                                        c.getRequestTimestamp(), c.getReplyTerminatorId(),
                                        c.getCorrelationString(), c.getCorrelationBinary());
                    }
                    else {
                        // -> No, this was SEND or REQUEST, so try to delete the entry in the inbox since we did not
                        // handle it after all (so go for RETRY from Client).
                        _matsSocketServer.getClusterStoreAndForward()
                                .deleteMessageIdsFromInbox(matsSocketSessionId, Collections.singleton(envelope.cmid));
                    }
                    // YES, this worked out!
                    break;
                }
                catch (DataAccessException ex) {
                    retry++;
                    if (retry >= MAX_NUMBER_OF_COMPENSATING_TRANSACTIONS_ATTEMPTS) {
                        log.error("Dammit, didn't manage to recover from a MatsMessageSendRuntimeException."
                                + " Closing MatsSocketSession and WebSocket with UNEXPECTED_CONDITION.", ex);
                        session.closeSessionAndWebSocket(MatsSocketCloseCodes.UNEXPECTED_CONDITION,
                                "Server error (data store), could not reliably recover (retry count exceeded)");
                        // This did NOT go OK - the WebSocket is now closed.
                        return;
                    }
                    log.warn("Didn't manage to get out of a MatsMessageSendRuntimeException situation at attempt ["
                            + retry + "], will try again after sleeping half a second.", e);
                    try {
                        Thread.sleep(MILLIS_BETWEEN_COMPENSATING_TRANSACTIONS_ATTEMPTS);
                    }
                    catch (InterruptedException exc) {
                        log.warn("Got interrupted while chill-sleeping trying to recover from"
                                + " MatsMessageSendRuntimeException. Closing MatsSocketSession and WebSocket with"
                                + " UNEXPECTED_CONDITION.", exc);
                        session.closeSessionAndWebSocket(MatsSocketCloseCodes.UNEXPECTED_CONDITION,
                                "Server error (data store), could not reliably recover (interrupted).");
                        // This did NOT go OK - the WebSocket is now closed.
                        return;
                    }
                }
            }

            // ----- Compensating transaction worked, now ask client to go for retrying.

            handledEnvelope[0].t = RETRY;
            handledEnvelope[0].desc = e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        catch (Throwable t) {
            // Evidently the handleIncoming didn't handle this message. This is a NACK.
            envelope.ir = IncomingResolution.EXCEPTION;
            log.warn("handleIncoming(..) raised exception, and the session is still SESSION_ESTABLISHED -"
                    + " must assume that it didn't like the incoming message - replying NACK to client.", t);
            handledEnvelope[0].t = NACK;
            handledEnvelope[0].desc = t.getClass().getSimpleName() + ": " + t.getMessage();
        }

        // Store processing time taken on incoming envelope.
        envelope.rm = msSince(nanosStart);
        // Record the incoming envelope
        session.recordEnvelopes(Collections.singletonList(envelope), receivedTimestamp, Direction.C2S);

        // Special handling for ACKs, unless the incoming message was a SEND
        if ((handledEnvelope[0].t == ACK) && (envelope.t != SEND)) {
            _matsSocketServer.getWebSocketOutgoingAcks().sendAck(matsSocketSessionId, envelope.cmid);
            return;
        }
        // E-> The Reply was not an ACK for a C2S SEND

        // Return our produced ACK/NACK/RETRY/RESOLVE/REJECT

        List<MatsSocketEnvelopeWithMetaDto> envelopeList = Collections.singletonList(handledEnvelope[0]);
        try {
            String json = _matsSocketServer.getEnvelopeListObjectWriter().writeValueAsString(
                    envelopeList);
            session.webSocketSendText(json);
        }
        catch (IOException e) {
            // I believe IOExceptions here to be utterly final - the session is gone. So just ignore this.
            // If he comes back, he will have to send the causes for these ACKs again.
            log.info("When trying to send handled-envelope of type [" + handledEnvelope[0].t
                    + "] for MatsSocketSession [" + matsSocketSessionId + "], we got IOException. Assuming session is"
                    + " gone. Ignoring, if the session comes back, he will send the causes for these ACKs again.", e);
            return;
        }

        // Set the incoming time
        handledEnvelope[0].icts = receivedTimestamp;
        // Set total time.
        handledEnvelope[0].rttm = msSince(nanosStart);

        // Record outgoing envelopes
        session.recordEnvelopes(envelopeList, System.currentTimeMillis(), Direction.S2C);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void invokeHandleIncoming(MatsSocketEndpointRegistration<?, ?, ?> registration, Object msg,
            MatsSocketEndpointIncomingContextImpl<?, ?, ?> requestContext) {
        IncomingAuthorizationAndAdapter incomingAuthEval = registration.getIncomingAuthEval();
        incomingAuthEval.handleIncoming(requestContext, requestContext.getPrincipal(), msg);
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

    /**
     * Raised from the Mats Initiation part of the handling if the session is not still in SESSION_ESTABLISHED when we
     * exit the handler.
     */
    private static class SessionLostException extends RuntimeException {
        public SessionLostException(String message) {
            super(message);
        }
    }

    private <T> T deserializeIncomingMessage(String serialized, Class<T> clazz) {
        try {
            return _matsSocketServer.getJackson().readValue(serialized, clazz);
        }
        catch (JsonProcessingException e) {
            // TODO: Handle parse exceptions.
            throw new AssertionError("Damn", e);
        }
    }

    private static class MatsSocketEndpointIncomingContextImpl<I, MR, R> implements
            MatsSocketEndpointIncomingContext<I, MR, R> {
        private final DefaultMatsSocketServer _matsSocketServer;
        private final MatsSocketEndpointRegistration<I, MR, R> _matsSocketEndpointRegistration;

        private final String _matsSocketSessionId;

        private final MatsInitiate _matsInitiate;

        private final MatsSocketEnvelopeWithMetaDto _envelope;
        private final long _clientMessageReceivedTimestamp;

        private final LiveMatsSocketSession _session;
        private final String _authorization;
        private final Principal _principal;

        private final String _correlationString;
        private final byte[] _correlationBinary;
        private final I _incomingMessage;

        private final MessageType _messageType;

        public MatsSocketEndpointIncomingContextImpl(DefaultMatsSocketServer matsSocketServer,
                MatsSocketEndpointRegistration<I, MR, R> matsSocketEndpointRegistration, String matsSocketSessionId,
                MatsInitiate matsInitiate,
                MatsSocketEnvelopeWithMetaDto envelope, long clientMessageReceivedTimestamp,
                LiveMatsSocketSession liveMatsSocketSession, String authorization, Principal principal,
                MessageType messageType,
                String correlationString, byte[] correlationBinary, I incomingMessage) {
            _matsSocketServer = matsSocketServer;
            _matsSocketEndpointRegistration = matsSocketEndpointRegistration;
            _matsSocketSessionId = matsSocketSessionId;
            _matsInitiate = matsInitiate;
            _envelope = envelope;
            _clientMessageReceivedTimestamp = clientMessageReceivedTimestamp;

            _session = liveMatsSocketSession;
            _authorization = authorization;
            _principal = principal;

            _messageType = messageType;

            _correlationString = correlationString;
            _correlationBinary = correlationBinary;
            _incomingMessage = incomingMessage;
        }

        private R _matsSocketReplyMessage;
        private IncomingResolution _handled = IncomingResolution.NO_ACTION;
        private String _forwardedMatsEndpoint;

        @Override
        public MatsSocketEndpoint<I, MR, R> getMatsSocketEndpoint() {
            return _matsSocketEndpointRegistration;
        }

        @Override
        public LiveMatsSocketSession getSession() {
            return _session;
        }

        @Override
        public String getAuthorizationValue() {
            return _authorization;
        }

        @Override
        public Principal getPrincipal() {
            return _principal;
        }

        @Override
        public String getUserId() {
            return _session.getUserId();
        }

        @Override
        public EnumSet<DebugOption> getAllowedDebugOptions() {
            return _session.getAllowedDebugOptions();
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
            if (_handled != IncomingResolution.NO_ACTION) {
                throw new IllegalStateException("Already handled.");
            }
            _handled = IncomingResolution.DENY;
        }

        @Override
        public void forwardInteractiveUnreliable(Object matsMessage) {
            forwardCustom(matsMessage, customInit -> {
                customInit.to(getMatsSocketEndpoint().getMatsSocketEndpointId());
                if (_envelope.to != null) {
                    customInit.nonPersistent(_envelope.to + 5000);
                }
                else {
                    customInit.nonPersistent();
                }
                customInit.interactive();
            });
        }

        @Override
        public void forwardInteractivePersistent(Object matsMessage) {
            forwardCustom(matsMessage, customInit -> {
                customInit.to(getMatsSocketEndpoint().getMatsSocketEndpointId());
                customInit.interactive();
            });
        }

        @Override
        public void forwardCustom(Object matsMessage, InitiateLambda customInit) {
            if (_handled != IncomingResolution.NO_ACTION) {
                throw new IllegalStateException("Already handled.");
            }

            _handled = IncomingResolution.FORWARD;

            // Record which Mats Endpoint we forward to.
            MatsInitiate init = new MatsInitiateWrapper(_matsInitiate) {
                @Override
                public MatsInitiate to(String endpointId) {
                    _forwardedMatsEndpoint = endpointId;
                    return super.to(endpointId);
                }
            };
            init.from("MatsSocketEndpoint." + _envelope.eid)
                    .traceId(_envelope.tid);
            // Add a small extra side-load - the MatsSocketSessionId - since it seems nice.
            init.addString("matsSocketSessionId", _matsSocketSessionId);
            // -> Is this a REQUEST?
            if (getMessageType() == REQUEST) {
                // -> Yes, this is a REQUEST, so we should forward as Mats .request(..)
                // :: Need to make state so that receiving terminator know what to do.

                // Handle the resolved DebugOptions for this flow
                EnumSet<DebugOption> resolvedDebugOptions = getResolvedDebugOptions();
                Integer debugFlags = DebugOption.flags(resolvedDebugOptions);
                // Hack to save a tiny bit of space for these flags that mostly will be 0 (null serializes "not there")
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
        public MatsInitiate getMatsInitiate() {
            return _matsInitiate;
        }

        @Override
        public void resolve(R matsSocketResolveMessage) {
            if (getMessageType() != REQUEST) {
                throw new IllegalStateException("This is not a Request, thus you cannot resolve nor reject it."
                        + " For a SEND, your options is to deny() it, forward it to Mats, or ignore it (and just return).");
            }
            if (_handled != IncomingResolution.NO_ACTION) {
                throw new IllegalStateException("Already handled.");
            }
            _matsSocketReplyMessage = matsSocketResolveMessage;
            _handled = IncomingResolution.RESOLVE;
        }

        @Override
        public void reject(R matsSocketRejectMessage) {
            if (getMessageType() != REQUEST) {
                throw new IllegalStateException("This is not a Request, thus you cannot resolve nor reject it."
                        + " For a SEND, your options is to deny() it, forward it to Mats, or ignore it (and just return).");
            }
            if (_handled != IncomingResolution.NO_ACTION) {
                throw new IllegalStateException("Already handled.");
            }
            _matsSocketReplyMessage = matsSocketRejectMessage;
            _handled = IncomingResolution.REJECT;
        }
    }
}
