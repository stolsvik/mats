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
     * Registers a MatsSocket.
     *
     * TODO: What about timeouts?!
     *
     * @param matsSocketEndpointId
     */
    <I, MI, MR, R> MatsSocketEndpoint<I, MI, MR, R> matsSocketEndpoint(String matsSocketEndpointId,
            Class<I> msIncomingClass, Class<MI> matsIncomingClass, Class<MR> matsReplyClass, Class<R> msReplyClass,
            IncomingAuthorizationAndAdapter<I, MI, R> incomingAuthEval);

    interface MatsSocketEndpoint<I, MI, MR, R> {
        /**
         * Used to transform the message from the Mats-side to MatsSocket-side - or throw an Exception. <b>This should
         * only be pure Java code, no IPC or lengthy computations</b>, such things should have happened in the Mats
         * stages.
         *
         * @param replyAdapter
         *            a function-like lambda that transform the incoming Mats reply into the outgoing MatsSocket reply.
         */
        void replyAdapter(ReplyAdapter<MR, R> replyAdapter);
    }

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

    @FunctionalInterface
    interface IncomingAuthorizationAndAdapter<I, MI, R> {
        void handleIncoming(MatsSocketEndpointRequestContext<MI, R> ctx, Principal principal, I msIncoming);
    }

    @FunctionalInterface
    interface ReplyAdapter<MR, R> {
        void adaptReply(MatsSocketEndpointReplyContext<MR, R> ctx, MR matsReply);
    }

    interface MatsSocketEndpointContext {
        /**
         * @return the WebSocket-facing endpoint Id.
         */
        String getMatsSocketEndpointId();
    }

    interface MatsSocketEndpointRequestContext<MI, R> extends MatsSocketEndpointContext {
        /**
         * @return current "Authorization header" in effect for the MatsSocket that delivered the message.
         */
        String getAuthorizationHeader();

        /**
         * @return the resolved Principal for the {@link #getAuthorizationHeader() Authorization header}. It is assumed
         *         that you must cast this a more specific class which the authentication plugin provides.
         */
        Principal getPrincipal();

        /**
         * @return the resolved UserId for the {@link #getAuthorizationHeader() Authorization header}.
         */
        String getUserId();

        /**
         * @return the incoming MatsSocket Message.
         */
        MI getMatsSocketIncomingMessage();

        /**
         * @return whether this is a "REQUEST" (true) or "SEND" (false).
         */
        boolean isRequest();

        /**
         * <b>FOR PURE "GET-style" REQUESTS!</b>: Both the "nonPersistent" flag <i>(messages in flow are not stored and
         * only lives "in-memory", can thus be lost, i.e. is unreliable, but is very fast)</i> and "interactive" flag
         * <i>(prioritized since a human is waiting)</i> will be set. Forwards the MatsSocket message to the Mats
         * endpoint of the same endpointId as the MatsSocketEndpointId. If it was a MatsSocket "REQUEST" from the
         * client, it will be a Mats request(..) message, while if it was a "SEND", it will be a Mats send(..) message.
         *
         * TODO: What about timeout? Must be implemented client side.
         */
        void forwardInteractiveUnreliable(MI matsMessage);

        /**
         * <b>For requests whose call flow can potentially change state in the system</b>: The "interactive" flag will
         * be set, since there is a human waiting. Forwards the MatsSocket message to the Mats endpoint of the same
         * endpointId as the MatsSocketEndpointId, as a normal <i>persistent</i> message, which should imply that it is
         * using reliable messaging. If it was a MatsSocket "REQUEST" from the client, it will be a Mats request(..)
         * message, while if it was a "SEND", it will be a Mats send(..) message.
         */
        void forwardInteractivePersistent(MI matsMessage);

        /**
         * <b>Customized Mats message creation:</b> Using the method, you can customize how the Mats message will be
         * created, including setting {@link MatsInitiate#setTraceProperty(String, Object) TraceProperties} - <b>NOTE:
         * 'to(..) must be set by you!</b>. The message properties "from", and if REQUEST, "replyTo" with correlation
         * information state, will already be set. No other properties are changed, which includes the 'interactive'
         * flag, which is not set either (you can set it, though). If it was a MatsSocket "REQUEST" from the client, it
         * will be a Mats request(..) message, while if it was a "SEND", it will be a Mats send(..) message.
         *
         * @param matsMessage
         *            the message to send to the Mats Endpoint.
         * @param customInit
         *            the {@link InitiateLambda} instance where you can customize the Mats message - read more at
         *            {@link MatsInitiator#initiate(InitiateLambda)}.
         */
        void forwardCustom(MI matsMessage, InitiateLambda customInit);

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
         * Standard code 1002 - From Server side, REJECT all outstanding: used for when the client does not observe the
         * protocol.
         */
        PROTOCOL_ERROR(CloseCodes.PROTOCOL_ERROR.getCode()),

        /**
         * Standard code 1008 - From Server side, REJECT all outstanding: used for when the we cannot authenticate.
         */
        VIOLATED_POLICY(CloseCodes.VIOLATED_POLICY.getCode()),

        /**
         * Standard code 1011 - From Server side, REISSUE all outstanding upon reconnect: used when the server cannot
         * talk to the underlying systems (DB or MQ). This should be a temporary situation, so doing periodic
         * re-connects would be correct.
         */
        UNEXPECTED_CONDITION(CloseCodes.UNEXPECTED_CONDITION.getCode()),

        /**
         * Standard code 1012 - From Server side, REISSUE all outstanding upon reconnect: used when
         * {@link MatsSocketServer#stop(int)} is invoked. Please reconnect.
         */
        SERVICE_RESTART(CloseCodes.SERVICE_RESTART.getCode()),

        /**
         * Standard code 1001 - From Client/Browser side, client should have REJECTed all outstanding: Synonym for
         * {@link #CLOSE_SESSION}, as the WebSocket documentation states <i>"indicates that an endpoint is "going away",
         * such as a server going down <b>or a browser having navigated away from a page.</b>"</i>, the latter point
         * being pretty much exactly correct wrt. when to close a session. So, if a browser decides to use this code
         * when the user navigates away and the library or application does not catch it, we'd want to catch this as a
         * Close Session.
         */
        GOING_AWAY(CloseCodes.GOING_AWAY.getCode()),

        /**
         * 4000: Both from Server side and Client/Browser side, client should REJECTed all outstanding:
         * <ul>
         * <li>From Browser: Used when the browser closes WebSocket "on purpose", wanting to close the session -
         * typically when the user explicitly logs out, or navigates away from web page. All traces of the
         * MatsSocketSession are effectively deleted from the server, including any undelivered replies and messages
         * ("push") from server.</li>
         * <li>From Server: {@link MatsSocketServer#closeSession(String)} was invoked, and the WebSocket to that client
         * was still open, so we close it.</li>
         * </ul>
         */
        CLOSE_SESSION(4000),

        /**
         * 4001: From Server side, REJECT all outstanding: A HELLO:RECONNECT was attempted, but the session was gone.
         */
        SESSION_LOST(4001),

        /**
         * 4002: Both from Server side and from Client/Browser side: REISSUE all outstanding upon reconnect: From
         * Server: We ask that the client reconnects. This gets us a clean state and in particular new authentication
         * (In case of using OAuth/OIDC tokens, the client is expected to fetch a fresh token from token server). From
         * Client: The client just fancied a little break (just as if lost connection in a tunnel), used form
         * integration tests.
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
         * @param code the code to get a CloseCode instance of.
         * @return either a {@link MatsSocketCloseCodes}, or a standard {@link CloseCodes}, or a newly created object
         * containing the unknown close code with a toString() returning "UNKNOWN(code)".
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
                    return "UNKNOWN("+code+")";
                }
            };
        }
    }
}
