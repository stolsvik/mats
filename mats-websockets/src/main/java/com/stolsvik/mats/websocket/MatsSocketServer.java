package com.stolsvik.mats.websocket;

import java.security.Principal;
import java.util.function.Function;

import javax.websocket.CloseReason.CloseCodes;

import com.stolsvik.mats.MatsEndpoint.DetachedProcessContext;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;

/**
 * @author Endre Stølsvik 2019-11-28 16:15 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsSocketServer {

    /**
     * Registers a MatsSocket with default timeout 90 seconds.
     *
     * TODO: What about timeouts?!
     *
     * @param matsSocketEndpointId
     */
    <I, MI, MR, R> MatsSocketEndpoint<I, MI, MR, R> matsSocketEndpoint(String matsSocketEndpointId,
            Class<I> msIncomingClass, Class<MI> matsIncomingClass, Class<MR> matsReplyClass, Class<R> msReplyClass);

    /**
     * This is mandatory. Must be very fast, as it is invoked synchronously - keep any IPC fast and keep relatively
     * short timeouts, otherwise all your threads of the container might be used up. If the function throws or returns
     * null, authorization did not go through.
     *
     * @param authorizationToPrincipalFunction
     *            a Function that turns an Authorization String into a Principal.
     */
    void setAuthorizationToPrincipalFunction(Function<String, Principal> authorizationToPrincipalFunction);

    /**
     * Closes all WebSockets with {@link CloseCodes#SERVICE_RESTART} (assuming that a MatsSocket service will never
     * truly go down..)
     */
    void shutdown();

    interface MatsSocketEndpoint<I, MI, MR, R> {
        void incomingForwarder(MatsSocketEndpointIncomingHandler<I, MI, R> matsSocketEndpointIncomingForwarder);

        void replyAdapter(MatsSocketEndpointReplyAdapter<MR, R> matsSocketEndpointReplyAdapter);
    }

    @FunctionalInterface
    interface MatsSocketEndpointIncomingHandler<I, MI, R> {
        void handleIncoming(MatsSocketEndpointRequestContext<MI, R> ctx, I msIncoming);
    }

    @FunctionalInterface
    interface MatsSocketEndpointReplyAdapter<MR, R> {
        R adaptReply(MatsSocketEndpointReplyContext<MR, R> ctx, MR matsReply);
    }

    interface MatsSocketEndpointContext {
        /**
         * @return the WebSocket-facing endpoint Id.
         */
        String getMatsSocketEndpointId();

        /**
         * @return current "Authorization header" in effect for the MatsSocket that delivered the message.
         */
        String getAuthorization();

        /**
         * @return the resolved Principal for the {@link #getAuthorization() Authorization header}.
         */
        Principal getPrincipal();
    }

    interface MatsSocketEndpointRequestContext<MI, R> extends MatsSocketEndpointContext {
        /**
         * <b>FOR PURE "GET-style" REQUESTS!</b>: Both the "nonPersistent" flag (messages in flow are not stored and
         * only lives "in-memory", can thus be lost, i.e. is unreliable, but is very fast) and "interactive"
         * (prioritized since a human is waiting) flag will be set. Forwards the MatsSocket message to the Mats endpoint
         * of the same endpointId as the MatsSocketEndpointId - if it was a "Request" from the WebSocket client, it will
         * be a Mats REQUEST message, while if it was a "Send", it will be a Mats SEND message.
         */
        void forwardInteractiveUnreliable(MI matsMessage);

        /**
         * If you rather want to create the message which will be forwarded onto the Mats MQ fabric, then use this. The
         * message property "from", and if REQUEST, "replyTo" with correlation information state, will already be set.
         *
         * @param msg
         *            the lambda on which to initiate the message - read more at
         *            {@link MatsInitiator#initiate(InitiateLambda)}.
         */
        void initiate(InitiateLambda msg);

        void initiateRaw(InitiateLambda msg);

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
