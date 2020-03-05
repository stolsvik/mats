package com.stolsvik.mats.websocket;

import java.security.Principal;
import java.util.EnumSet;

import javax.websocket.CloseReason.CloseCode;
import javax.websocket.CloseReason.CloseCodes;

import com.stolsvik.mats.MatsEndpoint.DetachedProcessContext;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;

/**
 * @author Endre Stølsvik 2019-11-28 16:15 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsSocketServer {

    /**
     * Registers a MatsSocket Endpoint, including a {@link ReplyAdapter} which can adapt the reply from the Mats
     * endpoint before being fed back to the MatsSocket - and also decide whether to resolve or reject the waiting
     * Client Promise.
     */
    <I, MI, MR, R> MatsSocketEndpoint<I, MI, MR, R> matsSocketEndpoint(String matsSocketEndpointId,
            Class<I> msIncomingClass, Class<MI> matsIncomingClass, Class<MR> matsReplyClass, Class<R> msReplyClass,
            IncomingAuthorizationAndAdapter<I, MI, R> incomingAuthEval, ReplyAdapter<MR, R> replyAdapter);

    /**
     * <i>(Convenience-variant of the base method)</i> Registers a MatsSocket Endpoint where there is no replyAdapter -
     * the reply from the Mats endpoint is directly fed back (as "resolved") to the MatsSocket. The Mats Reply class and
     * MatsSocket Reply class is thus the same.
     */
    default <I, MI, R> MatsSocketEndpoint<I, MI, R, R> matsSocketEndpoint(String matsSocketEndpointId,
            Class<I> msIncomingClass, Class<MI> matsIncomingClass, Class<R> replyClass,
            IncomingAuthorizationAndAdapter<I, MI, R> incomingAuthEval) {
        // Create an endpoint having the MR and R both being the same class, and lacking the AdaptReply.
        return matsSocketEndpoint(matsSocketEndpointId, msIncomingClass, matsIncomingClass, replyClass, replyClass,
                incomingAuthEval, null);
    }

    /**
     * <i>(Convenience-variant of the base method)</i> Registers a MatsSocket Endpoint meant for situations where you
     * intend to reply directly in the {@link IncomingAuthorizationAndAdapter} without forwarding to Mats.
     */
    default <I, R> MatsSocketEndpoint<I, Void, Void, R> matsSocketDirectReplyEndpoint(String matsSocketEndpointId,
            Class<I> msIncomingClass, Class<R> msReplyClass,
            IncomingAuthorizationAndAdapter<I, Void, R> incomingAuthEval) {
        // Create an endpoint having the MI and MR both being Void, and lacking the AdaptReply.
        return matsSocketEndpoint(matsSocketEndpointId, msIncomingClass, Void.class, Void.class, msReplyClass,
                incomingAuthEval, null);
    }

    /**
     * <i>(Convenience-variant of the base method)</i> Registers a MatsSocket Terminator (no reply), specifically for
     * "SEND" and "REPLY" (reply to a Server-to-Client {@link #request(String, String, String, Object, String, String)
     * request}) operations from the Client.
     */
    default <I, MI> MatsSocketEndpoint<I, MI, Void, Void> matsSocketTerminator(String matsSocketEndpointId,
            Class<I> msIncomingClass, Class<MI> matsIncomingClass,
            IncomingAuthorizationAndAdapter<I, MI, Void> incomingAuthEval) {
        // Create an endpoint having the MR and R both being Void, and lacking the AdaptReply.
        return matsSocketEndpoint(matsSocketEndpointId, msIncomingClass, matsIncomingClass, Void.class, Void.class,
                incomingAuthEval, null);
    }

    /**
     * Should handle (preliminary) Authorization evaluation on the supplied
     * {@link MatsSocketEndpointRequestContext#getPrincipal() Principal} and decide whether this message should be
     * forwarded to the Mats fabric (or directly resolved, rejected or denied). If it decides to forward to Mats, it
     * then adapt the incoming message to a message that can be forwarded to the Mats fabric
     * <p/>
     * <b>Note: Do remember that the MatsSocket is "live connected directly to the Internet" and ANY data coming in as
     * the incoming message can be utter garbage, and designed methodically to hack your system!</b>
     * <p/>
     * <b>Note: This should <i>preferentially</i> only be pure and quick Java code, without much database access or
     * lengthy computations</b>, as such things should happen in the Mats stages. You hold up the incoming message
     * handler while inside this method, hindering new messages for this MatsSocketSession from being processed in a
     * timely fashion - and you can also potentially deplete the WebSocket/Servlet thread pool.
     * <p/>
     * Note: It is imperative that this does not perform any state-changes to the system - it should be utterly
     * idempotent, i.e. invoking it a hundred times with the same input should yield the same result. (Note: Logging is
     * never considered state changing!)
     */
    @FunctionalInterface
    interface IncomingAuthorizationAndAdapter<I, MI, R> {
        void handleIncoming(MatsSocketEndpointRequestContext<I, MI, R> ctx, Principal principal, I msIncoming);
    }

    /**
     * Used to transform the message from the Mats-side to MatsSocket-side, and decide whether to resolve or reject the
     * waiting Client-side Promise - <b>this must only be pure Java DTO transformation code!</b>
     * <p/>
     * <b>Note: This should <i>definitely</i> only be pure and fast Java code, without <i>any</i> database access or
     * lengthy computations</b>, as these things should have been done in the Mats stages. This method is run by the
     * receiving Mats subscription terminator, of which there is literally only one per node, meaning that it affects
     * <i>all</i> other replies that are coming in for <i>all</i> MatsSocketSessions on this node!!
     * <p/>
     * Note: It is imperative that this does not perform any state-changes to the system - it should be utterly
     * idempotent, i.e. invoking it a hundred times with the same input should yield the same result. (Note: Logging is
     * never considered state changing!)
     */
    @FunctionalInterface
    interface ReplyAdapter<MR, R> {
        void adaptReply(MatsSocketEndpointReplyContext<MR, R> ctx, MR matsReply);
    }

    interface MatsSocketEndpoint<I, MI, MR, R> {
    }

    /**
     * Sends a message to the specified MatsSocketSession, to the specified Client TerminatorId. This is "fire into the
     * void" style messaging, where you have no idea of whether the client received the message. Usage scenarios include
     * <i>"New information about order progress"</i> which may or may not include said information (if not included, the
     * client must do a request to update) - but where the server does not really care if the client gets the
     * information, only that <i>if</i> he actually has the webpage/app open at the time, he will get the message and
     * thus update his view of the changed world.
     * <p/>
     * Note: If the specified session is closed when this method is invoked, the message will (effectively) silently be
     * dropped. Even if you just got hold of the sessionId and it was active then, it might asynchronously close while
     * you invoke this method.
     * <p/>
     * Note: The message is put in the outbox, and if the session is actually connected, it will be delivered ASAP,
     * otherwise it will rest in the outbox for delivery once the session reconnects. If the session then closes or
     * times out while the message is in the outbox, it will be deleted.
     * <p/>
     * Note: Given that the session actually is live and the client is connected or connects before the session is
     * closed or times out, the guaranteed delivery and exactly-once features are in effect, and this still holds in
     * face of session reconnects.
     */
    void send(String sessionId, String traceId, String clientTerminatorId, Object messageDto);

    /**
     * Initiates a request to the specified MatsSocketSession, to the specified Client EndpointId, with a replyTo
     * specified to (typically) a {@link #matsSocketTerminator(String, Class, Class, IncomingAuthorizationAndAdapter)
     * MatsSocket terminator} - which includes a String "correlationSpecifier" which is used to correlate the reply to
     * the request (available {@link MatsSocketEndpointRequestContext#getCorrelationSpecifier() here} for the reply
     * processing). Do note that since you have no control of when the Client decides to close the browser or terminate
     * the app, you have no guarantee that a reply will ever come - so code accordingly.
     * <p/>
     * Note: If the specified session is closed when this method is invoked, the message will (effectively) silently be
     * dropped. Even if you just got hold of the sessionId and it was active then, it might asynchronously close while
     * you invoke this method.
     * <p/>
     * Note: The message is put in the outbox, and if the session is actually connected, it will be delivered ASAP,
     * otherwise it will rest in the outbox for delivery once the session reconnects. If the session then closes or
     * times out while the message is in the outbox, it will be deleted.
     * <p/>
     * Note: Given that the session actually is live and the client is connected or connects before the session is
     * closed or times out, the guaranteed delivery and exactly-once features are in effect, and this still holds in
     * face of session reconnects.
     */
    void request(String sessionId, String traceId, String clientEndpointId, Object requestDto,
            String replyToMatsSocketTerminatorId, String correlationSpecifier);

    /**
     * Closes the specified MatsSocket Session - to be used for out-of-band closing of Session if the WebSocket is down.
     *
     * @param sessionId
     *            the id of the Session to close.
     */
    void closeSession(String sessionId);

    /**
     * Closes all WebSockets with {@link CloseCodes#SERVICE_RESTART} (assuming that a MatsSocket service will never
     * truly go down, thus effectively asking the client to reconnect, hopefully to another instance). Should be invoked
     * at application shutdown.
     */
    void stop(int gracefulShutdownMillis);

    interface MatsSocketEndpointContext {
        /**
         * @return the WebSocket-facing endpoint Id.
         */
        String getMatsSocketEndpointId();
    }

    interface MatsSocketEndpointRequestContext<I, MI, R> extends MatsSocketEndpointContext {
        /**
         * @return current <i>Authorization Header</i> in effect for the MatsSocket that delivered the message. This
         *         String is what resolves to the {@link #getPrincipal() current Principal} via the
         *         {@link AuthenticationPlugin}.
         */
        String getAuthorizationHeader();

        /**
         * @return the resolved Principal from the {@link #getAuthorizationHeader() Authorization Header}, via the
         *         {@link AuthenticationPlugin}. It is assumed that you must cast this a more specific class which the
         *         authentication plugin provides.
         */
        Principal getPrincipal();

        /**
         * @return the resolved UserId for the {@link #getAuthorizationHeader() Authorization header}.
         */
        String getUserId();

        /**
         * @return the MatsSocketSession Id. This can be useful when wanting to do a
         *         {@link MatsSocketServer#send(String, String, String, Object)}.
         */
        String getMatsSocketSessionId();

        /**
         * @return the incoming MatsSocket Message.
         */
        I getMatsSocketIncomingMessage();

        /**
         * If this is a REPLY from a {@link MatsSocketServer#request(String, String, String, Object, String, String)
         * request}, this method returns the CorrelationSpecifier that was provided in the request.
         *
         * @return the CorrelationSpecifier that was provided in the
         *         {@link MatsSocketServer#request(String, String, String, Object, String, String) request}, otherwise
         *         <code>null</code>.
         */
        String getCorrelationSpecifier();

        /**
         * @return whether this is a "REQUEST" (true) or "SEND" (false).
         */
        boolean isRequest();

        /**
         * Invoke if you want to deny this message from being processed, e.g. your preliminary Authorization checks
         * determined that the {@link #getPrincipal() current Principal} is not allowed to perform the requested
         * operation. Will send a "negative acknowledgement" to the client.
         */
        void deny();

        /**
         * <b>TYPICALLY for pure "GET-style" requests, or log event processing (not audit logging, though).</b>: Both
         * the "nonPersistent" flag <i>(messages in flow are not stored and only lives "in-memory", can thus be lost,
         * i.e. is unreliable, but is very fast)</i> and "interactive" flag <i>(prioritized since a human is
         * waiting)</i> will be set. Forwards the MatsSocket message to the Mats endpoint of the same endpointId as the
         * MatsSocketEndpointId.
         * <p/>
         * If it was a MatsSocket "REQUEST" from the client, it will be a Mats request(..) message, while if it was a
         * "SEND", it will be a Mats send(..) message.
         *
         * TODO: What about timeout? Must be implemented client side.
         */
        void forwardInteractiveUnreliable(MI matsMessage);

        /**
         * <b>For requests or sends whose call flow can potentially change state in the system</b>: The "interactive"
         * flag will be set, since there is a human waiting. Forwards the MatsSocket message to the Mats endpoint of the
         * same endpointId as the MatsSocketEndpointId, as a normal <i>persistent</i> message, which should imply that
         * it is using reliable messaging.
         * <p/>
         * If it was a MatsSocket "REQUEST" from the client, it will be a Mats request(..) message, while if it was a
         * "SEND", it will be a Mats send(..) message.
         */
        void forwardInteractivePersistent(MI matsMessage);

        /**
         * <b>Customized Mats message creation:</b> Using this method, you can customize how the Mats message will be
         * created, including setting {@link MatsInitiate#setTraceProperty(String, Object) TraceProperties} - <b>NOTE:
         * 'to(..) must be set by you!</b>. The message properties "from", and if REQUEST, "replyTo" with correlation
         * information state, will already be set. No other properties are changed, which includes the 'interactive'
         * flag, which is not set either (you can set it, though).
         * <p/>
         * If it was a MatsSocket "REQUEST" from the client, it will be a Mats request(..) message, while if it was a
         * "SEND", it will be a Mats send(..) message.
         *
         * @param matsMessage
         *            the message to send to the Mats Endpoint.
         * @param customInit
         *            the {@link InitiateLambda} instance where you can customize the Mats message - read more at
         *            {@link MatsInitiator#initiate(InitiateLambda)}.
         */
        void forwardCustom(MI matsMessage, InitiateLambda customInit);

        /**
         * <b>Only for {@link #isRequest()}:</b> Send "Resolve" reply (resolves the client side Promise) to the
         * MatsSocket directly, i.e. without forward to Mats - can be used if you can answer the MatsSocket request
         * directly without going onto the Mats MQ fabric.
         *
         * @param matsSocketResolveMessage
         *            the resolve message (the actual reply), or {@code null} if you just want to resolve it without
         *            adding any information.
         */
        void resolve(R matsSocketResolveMessage);

        /**
         * <b>Only for {@link #isRequest()}:</b> Send "Reject" reply (rejects the client side Promise) to the MatsSocket
         * directly, i.e. without forward to Mats - can be used if you can answer the MatsSocket request directly
         * without going onto the Mats MQ fabric.
         *
         * @param matsSocketRejectMessage
         *            the reject message, or {@code null} if you just want to reject it without adding any information.
         */
        void reject(R matsSocketRejectMessage);
    }

    interface MatsSocketEndpointReplyContext<MR, R> extends MatsSocketEndpointContext {
        /**
         * @return the {@link DetachedProcessContext} of the Mats incoming handler.
         */
        DetachedProcessContext getMatsContext();

        /**
         * Send "Resolve" reply (resolves the client side Promise) to the MatsSocket directly, i.e. without forward to
         * Mats - can be used if you can answer the MatsSocket request directly without going onto the Mats MQ fabric.
         *
         * @param matsSocketResolveMessage
         *            the resolve message (the actual reply), or {@code null} if you just want to resolve it without
         *            adding any information.
         */
        void resolve(R matsSocketResolveMessage);

        /**
         * Send "Reject" reply (rejects the client side Promise) to the MatsSocket directly, i.e. without forward to
         * Mats - can be used if you can answer the MatsSocket request directly without going onto the Mats MQ fabric.
         *
         * @param matsSocketRejectMessage
         *            the reject message, or {@code null} if you just want to reject it without adding any information.
         */
        void reject(R matsSocketRejectMessage);
    }

    /**
     * WebSocket CloseCodes used in MatsSocket, and for what. Using both standard codes, and app-specific/defined codes.
     */
    enum MatsSocketCloseCodes implements CloseCode {
        /**
         * Standard code 1002 - From Server side, Client should REJECT all outstanding and "crash"/reboot application:
         * used when the client does not observe the protocol.
         */
        PROTOCOL_ERROR(CloseCodes.PROTOCOL_ERROR.getCode()),

        /**
         * Standard code 1008 - From Server side, Client should REJECT all outstanding and "crash"/reboot application:
         * used when the we cannot authenticate.
         */
        VIOLATED_POLICY(CloseCodes.VIOLATED_POLICY.getCode()),

        /**
         * Standard code 1011 - From Server side, Client should REJECT all outstanding and "crash"/reboot application.
         * This is the default close code if the MatsSocket "onMessage"-handler throws anything, and may also explicitly
         * be used by the implementation if it encounters a situation it cannot recover from.
         */
        UNEXPECTED_CONDITION(CloseCodes.UNEXPECTED_CONDITION.getCode()),

        /**
         * Standard code 1012 - From Server side, Client should REISSUE all outstanding upon reconnect: used when
         * {@link MatsSocketServer#stop(int)} is invoked. Please reconnect.
         */
        SERVICE_RESTART(CloseCodes.SERVICE_RESTART.getCode()),

        /**
         * Standard code 1001 - From Client/Browser side, client should have REJECTed all outstanding: Synonym for
         * {@link #CLOSE_SESSION}, as the WebSocket documentation states <i>"indicates that an endpoint is "going away",
         * such as a server going down <b>or a browser having navigated away from a page.</b>"</i>, the latter point
         * being pretty much exactly correct wrt. when to close a session. So, if a browser decides to use this code
         * when the user navigates away and the client MatsSocket library or employing application does not catch it,
         * we'd want to catch this as a Close Session. Notice that I've not experienced a browser that actually utilizes
         * this close code yet, though!
         * <p/>
         * <b>Notice that if a close with this close code <i>is initiated from the Server-side</i>, this should NOT be
         * considered a CLOSE_SESSION by neither the client nor the server!</b> At least Jetty's implementation of JSR
         * 356 WebSocket API for Java sends GOING_AWAY upon socket close due to timeout. Since a timeout can happen if
         * we loose connection and thus can't convey PINGs, the MatsSocketServer must not interpret Jetty's
         * timeout-close as Close Session. Likewise, if the client just experienced massive lag on the connection, and
         * thus didn't get the PING over to the server in a timely fashion, but then suddenly gets Jetty's timeout close
         * with GOING_AWAY, this should not be interpreted by the client as the server wants to close the session.
         */
        GOING_AWAY(CloseCodes.GOING_AWAY.getCode()),

        /**
         * 4000: Both from Server side and Client/Browser side, client should REJECT all outstanding:
         * <ul>
         * <li>From Client/Browser: Used when the client closes WebSocket "on purpose", wanting to close the session -
         * typically when the user explicitly logs out, or navigates away from web page. All traces of the
         * MatsSocketSession are effectively deleted from the server, including any undelivered replies and messages
         * ("push") from server.</li>
         * <li>From Server: {@link MatsSocketServer#closeSession(String)} was invoked, and the WebSocket to that client
         * was still open, so we close it.</li>
         * </ul>
         */
        CLOSE_SESSION(4000),

        /**
         * 4001: From Server side, Client should REJECT all outstanding and should consider "rebooting" the application,
         * in particular if if there was any outstanding requests as their state is now indeterminate: A HELLO:RECONNECT
         * was attempted, but the session was gone. A new session was provided instead. The client application must get
         * its state synchronized with the server side's view of the world, thus the suggestion of "reboot".
         */
        SESSION_LOST(4001),

        /**
         * 4002: Both from Server side and from Client/Browser side: REISSUE all outstanding upon reconnect:
         * <ul>
         * <li>From Client: The client just fancied a little break (just as if lost connection in a tunnel), used from
         * integration tests.</li>
         * <li>From Server: We ask that the client reconnects. This gets us a clean state and in particular new
         * authentication (In case of using OAuth/OIDC tokens, the client is expected to fetch a fresh token from token
         * server).</li>
         * </ul>
         */
        RECONNECT(4002);

        private final int _closeCode;

        MatsSocketCloseCodes(int closeCode) {
            _closeCode = closeCode;
        }

        @Override
        public int getCode() {
            return _closeCode;
        }

        /**
         * @param code
         *            the code to get a CloseCode instance of.
         * @return either a {@link MatsSocketCloseCodes}, or a standard {@link CloseCodes}, or a newly created object
         *         containing the unknown close code with a toString() returning "UNKNOWN(code)".
         */
        public static CloseCode getCloseCode(int code) {
            for (MatsSocketCloseCodes mscc : EnumSet.allOf(MatsSocketCloseCodes.class)) {
                if (mscc.getCode() == code) {
                    return mscc;
                }
            }
            for (CloseCodes stdcc : EnumSet.allOf(CloseCodes.class)) {
                if (stdcc.getCode() == code) {
                    return stdcc;
                }
            }
            return new CloseCode() {
                @Override
                public int getCode() {
                    return code;
                }

                @Override
                public String toString() {
                    return "UNKNOWN(" + code + ")";
                }
            };
        }
    }
}
