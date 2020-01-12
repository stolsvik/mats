package com.stolsvik.mats.websocket;

import java.security.Principal;

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

    /**
     * Closes all WebSockets with {@link CloseCodes#SERVICE_RESTART} (assuming that a MatsSocket service will never
     * truly go down..)
     */
    void shutdown();

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

    @FunctionalInterface
    interface IncomingAuthorizationAndAdapter<I, MI, R> {
        void handleIncoming(MatsSocketEndpointRequestContext<MI, R> ctx, Principal principal, I msIncoming);
    }

    @FunctionalInterface
    interface ReplyAdapter<MR, R> {
        R adaptReply(MatsSocketEndpointReplyContext<MR, R> ctx, MR matsReply);
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
        String getAuthorization();

        /**
         * @return the resolved Principal for the {@link #getAuthorization() Authorization header}. It is assumed that
         *         you must cast this a more specific class which the authentication plugin provides.
         */
        Principal getPrincipal();

        /**
         * @return the incoming MatsSocket Message.
         */
        MI getMatsSocketIncomingMessage();

        /**
         * <b>FOR PURE "GET-style" REQUESTS!</b>: Both the "nonPersistent" flag <i>(messages in flow are not stored and
         * only lives "in-memory", can thus be lost, i.e. is unreliable, but is very fast)</i> and "interactive" flag
         * <i>(prioritized since a human is waiting)</i> will be set. Forwards the MatsSocket message to the Mats
         * endpoint of the same endpointId as the MatsSocketEndpointId. If it was a MatsSocket "REQUEST" from the
         * client, it will be a Mats request(..) message, while if it was a "SEND", it will be a Mats send(..) message.
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
         * @return whether this is a "REQUEST" (true) or "SEND" (false).
         */
        boolean isRequest();

        /**
         * Send reply to the MatsSocket directly, i.e. without forward - can be used if you can answer the MatsSocket
         * request directly without going onto the Mats MQ fabric.
         *
         * @param matsSocketReplyMessage
         */
        void reply(R matsSocketReplyMessage);
    }

    interface MatsSocketEndpointReplyContext<MR, R> extends MatsSocketEndpointContext {
        /**
         * @return the {@link DetachedProcessContext} of the Mats incoming handler.
         */
        DetachedProcessContext getMatsContext();

        void addBinary(String key, byte[] payload);
    }

}
