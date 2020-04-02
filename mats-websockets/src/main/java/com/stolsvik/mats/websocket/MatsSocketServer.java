package com.stolsvik.mats.websocket;

import java.security.Principal;
import java.util.EnumSet;

import javax.websocket.CloseReason.CloseCode;
import javax.websocket.CloseReason.CloseCodes;

import com.stolsvik.mats.MatsEndpoint.DetachedProcessContext;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;
import com.stolsvik.mats.websocket.AuthenticationPlugin.DebugOption;

/**
 * The MatsSocket Java library, along with its several clients libraries, is a WebSocket-"extension" of the Mats library
 * <i>(there are currently clients for JavaScript (web and Node.js) and Dart (Dart and Flutter))</i>. It provides for
 * asynchronous communications between a Client and a MatsSocketServer using WebSockets, which again asynchronously
 * interfaces with the Mats API. The result is a simple programming model on the client, providing Mats's asynchronous
 * and guaranteed delivery aspects all the way from the client, to the server, and back, and indeed the other way
 * ("push", including requests from Server to Client). It is a clearly demarcated solution in that it only utilizes the
 * API of the Mats library and the API of the <i>JSR 356 WebSocket API for Java</i> (with some optional hooks that
 * typically will be implemented using the Servlet API, but any HTTP server implementation can be employed).
 * MatsSocketEndpoints are simple to define, code and reason about, simple to authenticate and authorize, and the
 * interface with Mats is very simple, yet flexible.
 * <p/>
 * Features:
 * <ul>
 * <li>Very lightweight transport protocol, which is human readable and understandable, with little overhead
 * ({@link MessageType all message types} and {@link MatsSocketCloseCodes all socket closure modes}).</li>
 * <li>A TraceId is created on the Client, sent with the client call, through the MatsSocketServer, all the way through
 * all Mats stages, back to the Reply - logged along all the steps.</li>
 * <li>Provides "Send" and "Request" features both Client-to-Server and Server-to-Client.</li>
 * <li>Reconnection in face of broken connections is handled by the Client library, with full feedback solution to the
 * end user via event listeners</li>
 * <li>Guaranteed delivery both ways, also in face of reconnects at any point, employing an "outbox" and "inbox" on both
 * sides, with a three-way handshake protocol for each "information bearing message": Message, acknowledge of message,
 * acknowledge of acknowledge.</li>
 * <li>Client side Event Listener for ConnectionEvents (e.g. connecting, waiting, session_established, lost_connection),
 * for display to end user.</li>
 * <li>Client side Event Listener for SessionClosedEvents, which is when the system does not manage to keep the
 * guarantees in face of adverse situation on the server side (typically lost database connection in a bad spot).</li>
 * <li>Pipelining of messages, which is automatic (i.e. delay of 2 ms after last message enqueued before the pipeline is
 * sent) - but with optional "flush()" command to send a pipeline right away.</li>
 * <li>Several debugging features in the protocol (full round-trip TraceId, comprehensive logging, and
 * {@link DebugOption DebugOptions})</li>
 * <li>Built-in and simple statistics gathering on the client.</li>
 * <li>Simple and straight-forward, yet comprehensive and mandatory, authentication model employing a
 * MatsSocketServer-wide server {@link AuthenticationPlugin} paired with a Client authentication callback, and a
 * per-MatsSocketEndpoint, per-message {@link IncomingAuthorizationAndAdapter IncomingAuthorizationAndAdapter}.</li>
 * <li>The WebSocket protocol itself has built-in "per-message" compression.</li>
 * </ul>
 * <p/>
 * Notes:
 * <ul>
 * <li>There is no way to cancel or otherwise control individual messages: The library's simple "send" and
 * Promise-yielding "request" operations (with optional "receivedCallback" for the client) is the only way to talk with
 * the library. When those methods returns, the operation is queued, and will be transferred to the other side ASAP.
 * This works like magic for short and medium messages, but does not constitute a fantastic solution for large payloads
 * over bad networks (as typically <i>can</i> be the case with Web apps and mobile apps).</li>
 * <li>MatsSockets does not provide "channel multiplexing", meaning that one message (or pipeline of messages) will have
 * to be fully transferred before the next one is. This means that if you decide to send over a 200 MB PDF using the
 * MatsSocket instance over a 2G cellular network, any concurrent messages will experience a massive delay. This again
 * means that you should probably not do that: Either you should do such a download or upload using ordinary HTTP
 * solutions, or employ a second MatsSocket instance for this if you just cannot get enough of the MatsSocket API - but
 * you will get more control over e.g. cancellation of the transfer with the HTTP approach.</li>
 * </ul>
 *
 * @author Endre Stølsvik 2019-11-28 16:15 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsSocketServer {

    /**
     * Registers a MatsSocket Endpoint, including a {@link ReplyAdapter} which can adapt the reply from the Mats
     * endpoint before being fed back to the MatsSocket - and also decide whether to resolve or reject the waiting
     * Client Promise.
     */
    <I, MR, R> MatsSocketEndpoint<I, MR, R> matsSocketEndpoint(String matsSocketEndpointId,
            Class<I> msIncomingClass, Class<MR> matsReplyClass, Class<R> msReplyClass,
            IncomingAuthorizationAndAdapter<I, R> incomingAuthEval, ReplyAdapter<MR, R> replyAdapter);

    /**
     * <i>(Convenience-variant of the base method)</i> Registers a MatsSocket Endpoint where there is no replyAdapter -
     * the reply from the Mats endpoint is directly fed back (as "resolved") to the MatsSocket. The Mats Reply class and
     * MatsSocket Reply class is thus the same.
     */
    default <I, R> MatsSocketEndpoint<I, R, R> matsSocketEndpoint(String matsSocketEndpointId,
            Class<I> msIncomingClass, Class<R> replyClass,
            IncomingAuthorizationAndAdapter<I, R> incomingAuthEval) {
        // Create an endpoint having the MR and R both being the same class, and lacking the AdaptReply.
        return matsSocketEndpoint(matsSocketEndpointId, msIncomingClass, replyClass, replyClass,
                incomingAuthEval, null);
    }

    /**
     * <i>(Convenience-variant of the base method)</i> Registers a MatsSocket Endpoint meant for situations where you
     * intend to reply directly in the {@link IncomingAuthorizationAndAdapter} without forwarding to Mats.
     */
    default <I, R> MatsSocketEndpoint<I, Void, R> matsSocketDirectReplyEndpoint(String matsSocketEndpointId,
            Class<I> msIncomingClass, Class<R> msReplyClass,
            IncomingAuthorizationAndAdapter<I, R> incomingAuthEval) {
        // Create an endpoint having the MI and MR both being Void, and lacking the AdaptReply.
        return matsSocketEndpoint(matsSocketEndpointId, msIncomingClass, Void.class, msReplyClass,
                incomingAuthEval, null);
    }

    /**
     * <i>(Convenience-variant of the base method)</i> Registers a MatsSocket Terminator (no reply), specifically for
     * "SEND" and "REPLY" (reply to a Server-to-Client
     * {@link #request(String, String, String, Object, String, String, byte[])} request}) operations from the Client.
     */
    default <I> MatsSocketEndpoint<I, Void, Void> matsSocketTerminator(String matsSocketEndpointId,
            Class<I> msIncomingClass, IncomingAuthorizationAndAdapter<I, Void> incomingAuthEval) {
        // Create an endpoint having the MR and R both being Void, and lacking the AdaptReply.
        return matsSocketEndpoint(matsSocketEndpointId, msIncomingClass, Void.class, Void.class,
                incomingAuthEval, null);
    }

    /**
     * Should handle (preliminary) Authorization evaluation on the supplied
     * {@link MatsSocketEndpointRequestContext#getPrincipal() Principal} and decide whether this message should be
     * forwarded to the Mats fabric (or directly resolved, rejected or denied). If it decides to forward to Mats, it
     * then adapt the incoming MatsSocket message to a message that can be forwarded to the Mats fabric.
     * <p/>
     * <b>Note: Do remember that the MatsSocket is "live connected directly to the Internet" and ANY data coming in as
     * the incoming message can be utter garbage, or methodically designed to hack your system! Act accordingly!</b>
     * <p/>
     * <b>Note: This should <i>preferentially</i> only be pure and quick Java code, without much database access or
     * lengthy computations</b> - such things should happen in the Mats stages. You hold up the incoming WebSocket
     * message handler while inside the IncomingAuthorizationAndAdapter, hindering new messages for this
     * MatsSocketSession from being processed in a timely fashion - and you can also potentially deplete the
     * WebSocket/Servlet thread pool. You should therefore just do authorization evaluation, decide whether to deny,
     * forward to Mats, or insta-resolve or -reject, and adapt the MatsSocket incoming DTO to the Mats endpoint's
     * expected incoming DTO (if forward), or MatsSocket reply DTO (if insta-resolve or -reject).
     * <p/>
     * Note: It is imperative that this does not perform any state-changes to the system - it should be utterly
     * idempotent, i.e. invoking it a hundred times with the same input should yield the same result. (Note: Logging is
     * never considered state changing!)
     */
    @FunctionalInterface
    interface IncomingAuthorizationAndAdapter<I, R> {
        void handleIncoming(MatsSocketEndpointRequestContext<I, R> ctx, Principal principal, I msg);
    }

    /**
     * Used to transform the reply message from the Mats endpoint to the reply for the MatsSocket endpoint, and decide
     * whether to resolve or reject the waiting Client-side Promise - <b>this should only be pure Java DTO
     * transformation code!</b>
     * <p/>
     * <b>Note: This should <i>preferentially</i> only be pure and quick Java code, without much database access or
     * lengthy computations</b> - such things should happen in the Mats stages. The ReplyAdapter is run in the common
     * MatsSocket reply Mats Terminator, which is shared between all MatsSocket Endpoints on this node, and should thus
     * preferably just adapt the Mats reply DTO to the MatsSocket endpoint's expected reply DTO (and potentially decide
     * whether to resolve or reject).
     * <p/>
     * Note: It is imperative that this does not perform any state-changes to the system - it should be utterly
     * idempotent, i.e. invoking it a hundred times with the same input should yield the same result. (Note: Logging is
     * never considered state changing!)
     */
    @FunctionalInterface
    interface ReplyAdapter<MR, R> {
        void adaptReply(MatsSocketEndpointReplyContext<MR, R> ctx, MR matsReply);
    }

    /**
     * Representation of a MatsSocketEndpoint.
     *
     * @param <I>
     *            type of the incoming MatsSocket message ("Incoming")
     * @param <MR>
     *            type of the returned Mats message ("Mats Reply")
     * @param <R>
     *            type of the outgoing MatsSocket message ("Reply")
     */
    interface MatsSocketEndpoint<I, MR, R> {
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
     * specified to (typically) a {@link #matsSocketTerminator(String, Class, IncomingAuthorizationAndAdapter)
     * MatsSocket terminator} - which includes a String "correlationString" and byte array "correlationBinary" which can
     * be used to correlate the reply to the request (available
     * {@link MatsSocketEndpointRequestContext#getCorrelationString() here} and
     * {@link MatsSocketEndpointRequestContext#getCorrelationString() here} for the reply processing). Do note that
     * since you have no control of when the Client decides to close the browser or terminate the app, you have no
     * guarantee that a reply will ever come - so code accordingly.
     * <p/>
     * Note: the {@code correlationString} and {@code correlationBinary} are not sent over to the client, but stored
     * server side in the {@link ClusterStoreAndForward}. This both means that you do not need to be afraid of size (but
     * storing megabytes is silly anyway), but more importantly, this data cannot be tampered with client side - you can
     * be safe that what you gave in here is what you get out in the
     * {@link MatsSocketEndpointRequestContext#getCorrelationString()} and
     * {@link MatsSocketEndpointRequestContext#getCorrelationBinary()} ()}
     * <p/>
     * Note: To check whether the client Resolved or Rejected the request, use
     * {@link MatsSocketEndpointRequestContext#getMessageType()}.
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
            String replyToMatsSocketTerminatorId, String correlationString, byte[] correlationBinary);

    /**
     * Publish a Message to the specified Topic, with the specified TraceId.
     *
     * @param traceId
     *            traceId for the flow.
     * @param topicId
     *            which Topic to Publish on.
     * @param messageDto
     *            the message to Publish.
     */
    void publish(String traceId, String topicId, Object messageDto);

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

    interface MatsSocketEndpointRequestContext<I, R> extends MatsSocketEndpointContext {
        /**
         * @return current <i>Authorization Value</i> in effect for the MatsSocket that delivered the message. This
         *         String is what resolves to the {@link #getPrincipal() current Principal} and {@link #getUserId()
         *         UserId} via the {@link AuthenticationPlugin}.
         */
        String getAuthorizationValue();

        /**
         * @return the resolved Principal from the {@link #getAuthorizationValue() Authorization Value}, via the
         *         {@link AuthenticationPlugin}. It is assumed that you must cast this a more specific class which the
         *         authentication plugin provides.
         */
        Principal getPrincipal();

        /**
         * @return the resolved UserId for the {@link #getAuthorizationValue() Authorization Value}.
         */
        String getUserId();

        /**
         * @return the set of {@link DebugOption} the the active {@link AuthenticationPlugin} allows the
         *         {@link #getPrincipal() current Principal} to request.
         */
        EnumSet<DebugOption> getAllowedDebugOptions();

        /**
         * @return the set of {@link DebugOption} the the current message tells that us the Client requests, intersected
         *         with what active {@link AuthenticationPlugin} allows the {@link #getPrincipal() current Principal} to
         *         request.
         */
        EnumSet<DebugOption> getResolvedDebugOptions();

        /**
         * @return the MatsSocketSession Id. This can be useful when wanting to do a
         *         {@link MatsSocketServer#send(String, String, String, Object)}.
         */
        String getMatsSocketSessionId();

        /**
         * @return the TraceId accompanying the incoming message.
         */
        String getTraceId();

        /**
         * @return the {@link MessageType} of the message being processed - either {@link MessageType#SEND SEND},
         *         {@link MessageType#REQUEST REQUEST}, {@link MessageType#RESOLVE RESOLVE} or {@link MessageType#REJECT
         *         REJECT} (the two latter are Reply-types to a previous REQUEST).
         */
        MessageType getMessageType();

        /**
         * @return the incoming MatsSocket Message.
         */
        I getMatsSocketIncomingMessage();

        /**
         * If this is a Client Reply from a Server-to-Client
         * {@link MatsSocketServer#request(String, String, String, Object, String, String, byte[]) request}, this method
         * returns the 'correlationString' that was provided in the request.
         *
         * @return the 'correlationString' that was provided in the
         *         {@link MatsSocketServer#request(String, String, String, Object, String, String, byte[]) request} ,
         *         otherwise <code>null</code>.
         */
        String getCorrelationString();

        /**
         * If this is a Client Reply from a Server-to-Client
         * {@link MatsSocketServer#request(String, String, String, Object, String, String, byte[]) request}, this method
         * returns the 'correlationBinary' that was provided in the request.
         *
         * @return the 'correlationBinary' that was provided in the
         *         {@link MatsSocketServer#request(String, String, String, Object, String, String, byte[]) request} ,
         *         otherwise <code>null</code>.
         */
        byte[] getCorrelationBinary();

        /**
         * Invoke if you want to deny this message from being processed, e.g. your preliminary Authorization checks
         * determined that the {@link #getPrincipal() current Principal} is not allowed to perform the requested
         * operation. Will send a "negative acknowledgement" to the client.
         */
        void deny();

        /**
         * <b>TYPICALLY for pure "GET-style" Requests, or Sends for e.g. log event store/processing</b> (not audit
         * logging, though - those you want reliable): Both the "nonPersistent" flag <i>(messages in flow are not stored
         * and only lives "in-memory", can thus be lost, i.e. is unreliable, but is very fast)</i> and "interactive"
         * flag <i>(prioritized since a human is waiting)</i> will be set. Forwards the MatsSocket message to the Mats
         * endpoint of the same endpointId as the MatsSocketEndpointId.
         * <p/>
         * If the {@link #getMessageType() MessageType} of the incoming message from the Client is
         * {@link MessageType#REQUEST REQUEST}, it will be a Mats request(..) message, while if it was a
         * {@link MessageType#SEND SEND}, {@link MessageType#RESOLVE RESOLVE} or {@link MessageType#REJECT REJECT}, it
         * will be a Mats send(..) message.
         */
        void forwardInteractiveUnreliable(Object matsMessage);

        /**
         * <b>For requests or sends whose call flow can potentially change state in the system</b>: The "interactive"
         * flag will be set, since there is a human waiting. Forwards the MatsSocket message to the Mats endpoint of the
         * same endpointId as the MatsSocketEndpointId, as a normal <i>persistent</i> message, which should imply that
         * it is using reliable messaging.
         * <p/>
         * If the {@link #getMessageType() MessageType} of the incoming message from the Client is
         * {@link MessageType#REQUEST REQUEST}, it will be a Mats request(..) message, while if it was a
         * {@link MessageType#SEND SEND}, {@link MessageType#RESOLVE RESOLVE} or {@link MessageType#REJECT REJECT}, it
         * will be a Mats send(..) message.
         */
        void forwardInteractivePersistent(Object matsMessage);

        /**
         * <b>Customized Mats message creation:</b> Using this method, you can customize how the Mats message will be
         * created, including setting {@link MatsInitiate#setTraceProperty(String, Object) TraceProperties} - <b>NOTE:
         * 'to(..) must be set by you!</b>. The message properties "from", and if REQUEST, "replyTo" with correlation
         * information state, will already be set. No other properties are changed, which includes the 'interactive'
         * flag, which is not set either (you can set it, though).
         * <p/>
         * If the {@link #getMessageType() MessageType} of the incoming message from the Client is
         * {@link MessageType#REQUEST REQUEST}, it will be a Mats request(..) message, while if it was a
         * {@link MessageType#SEND SEND}, {@link MessageType#RESOLVE RESOLVE} or {@link MessageType#REJECT REJECT}, it
         * will be a Mats send(..) message.
         *
         * @param matsMessage
         *            the message to send to the Mats Endpoint.
         * @param customInit
         *            the {@link InitiateLambda} instance where you can customize the Mats message - read more at
         *            {@link MatsInitiator#initiate(InitiateLambda)}.
         */
        void forwardCustom(Object matsMessage, InitiateLambda customInit);

        /**
         * Using the returned {@link MatsInitiate MatsInitiate} instance, which is the same as the
         * {@link #forwardCustom(Object, InitiateLambda) forwardXX(..)} methods utilize, you can initiate one or several
         * Mats flows, <i>in addition</i> to your actual handling of the incoming message - within the same Mats
         * transactional demarcation as the handling of the incoming message ("actual handling" referring to
         * {@link #forwardCustom(Object, InitiateLambda) forward}, {@link #resolve(Object) resolve},
         * {@link #reject(Object) reject} or even {@link #deny() deny} or ignore - the latter two being a bit hard to
         * understand why you'd want).
         * <p/>
         * Notice: As mentioned, this is the same instance as the forward-methods utilize, so you must not put it into
         * some "intermediate" state where you've invoked some of its methods, but not invoked any of the
         * finishing-methods {@link MatsInitiate#send(Object) matsInitiate.send(..)} or
         * {@link MatsInitiate#request(Object) .request(..)} methods.
         *
         * @return the {@link MatsInitiate MatsInitiate} instance which the
         *         {@link #forwardCustom(Object, InitiateLambda) forward} methods utilize, where you can initiate one or
         *         several other Mats flows (in addition to actual handling of incoming message) within the same Mats
         *         transactional demarcation as the handling of the message.
         */
        MatsInitiate getMatsInitiate();

        /**
         * <b>Only for {@link MessageType#REQUEST REQUESTs}:</b> Send "Resolve" reply (resolves the client side Promise)
         * to the MatsSocket directly, i.e. without forward to Mats - can be used if you can answer the MatsSocket
         * request directly without going onto the Mats MQ fabric.
         *
         * @param matsSocketResolveMessage
         *            the resolve message (the actual reply), or {@code null} if you just want to resolve it without
         *            adding any information.
         */
        void resolve(R matsSocketResolveMessage);

        /**
         * <b>Only for {@link MessageType#REQUEST REQUESTs}:</b> Send "Reject" reply (rejects the client side Promise)
         * to the MatsSocket directly, i.e. without forward to Mats - can be used if you can answer the MatsSocket
         * request directly without going onto the Mats MQ fabric.
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
     * All Message Types (aka MatsSocket Envelope Types) used in the wire-protocol of MatsSocket.
     */
    enum MessageType {
        /**
         * A HELLO message must be part of the first Pipeline of messages, preferably alone. One of the messages in the
         * first Pipeline must have the "auth" field set, and it might as well be the HELLO.
         */
        HELLO,

        /**
         * The reply to a {@link #HELLO}, where the MatsSocketSession is established, and the MatsSocketSessionId is
         * returned. If you included a MatsSocketSessionId in the HELLO, signifying that you want to reconnect to an
         * existing session, and you actually get a WELCOME back, it will be the same as what you provided - otherwise
         * the connection is closed with {@link MatsSocketCloseCodes#SESSION_LOST}.
         */
        WELCOME,

        /**
         * The sender sends a "fire and forget" style message.
         */
        SEND,

        /**
         * The sender initiates a request, to which a {@link #RESOLVE} or {@link #REJECT} message is expected.
         */
        REQUEST,

        /**
         * The sender should retry the message (the receiver could not handle it right now, but a Retry might fix it).
         */
        RETRY,

        /**
         * The specified message was Received, and acknowledged positively - i.e. the other party has decided to process
         * it.
         * <p/>
         * The sender of the ACK has now taken over responsibility of the specified message, put it (at least the
         * reference ClientMessageId) in its <i>Inbox</i>, and possibly started processing it. The reason for the Inbox
         * is so that if it Receives the message again, it may just insta-ACK/NACK it and toss this copy out the window
         * (since it has already handled it).
         * <p/>
         * When an ACK is received, the receiver may safely delete the acknowledged message from its <i>Outbox</i>.
         */
        ACK,

        /**
         * The specified message was Received, but it did not acknowledge it - i.e. the other party has decided to NOT
         * process it.
         * <p/>
         * The sender of the NACK has now taken over responsibility of the specified message, put it (at least the
         * reference Client/Server MessageId) in its <i>Inbox</i> - but has evidently decided not to process it. The
         * reason for the Inbox is so that if it Receives the message again, it may just insta-ACK/NACK it and toss this
         * copy out the window (since it has already handled it).
         * <p/>
         * When an NACK is received, the receiver may safely delete the acknowledged message from its <i>Outbox</i>.
         */
        NACK,

        /**
         * An "Acknowledge ^ 2", i.e. an acknowledge of the {@link #ACK} or {@link #NACK}. When the receiver gets this,
         * it may safely delete the entry it has for the specified message from its <i>Inbox</i>.
         * <p/>
         * The message is now fully transferred from one side to the other, and both parties again has no reference to
         * this message in their Inbox and Outbox.
         */
        ACK2,

        /**
         * A RESOLVE-reply to a previous {@link #REQUEST} - if the Client did the {@code REQUEST}, the Server will
         * answer with either a RESOLVE or {@link #REJECT}.
         */
        RESOLVE,

        /**
         * A REJECT-reply to a previous {@link #REQUEST} - if the Client did the {@code REQUEST}, the Server will answer
         * with either a REJECT or {@link #RESOLVE}.
         */
        REJECT,

        /**
         * Request from Client: The Client want to subscribe to a Topic, the TopicId is specified in 'eid'.
         */
        SUB,

        /**
         * Request from Client: The Client want to unsubscribe from a Topic, the TopicId is specified in 'eid'.
         */
        UNSUB,

        /**
         * Reply from Server: Subscription was OK. If this is a reconnect, this indicates that any messages that was
         * lost "while offline" will now be delivered/"replayed".
         */
        SUB_OK,

        /**
         * Reply from Server: Subscription went OK, but you've lost messages: The messageId that was referenced in the
         * {@link #SUB} was not known to the server, implying that there are at least one message that has expired, but
         * we don't know whether it was 1 or 1 million - so you won't get any "replayed".
         */
        SUB_LOST,

        /**
         * Reply from Server: Subscription was not authorized - no messages for this Topic will be delivered.
         */
        SUB_NO_AUTH,

        /**
         * Topic message from Server: A message is issued on Topic, the TopicId is specified in 'eid', while the message
         * is in 'msg'.
         */
        PUB,

        /**
         * The server requests that the Client re-authenticates, where the Client should immediately get a fresh
         * authentication and send it back using either any message it has pending, or in a separate {@link #AUTH}
         * message. Message processing - both processing of received messages, and sending of outgoing messages (i.e.
         * Replies to REQUESTs, or Server-initiated SENDs and REQUESTs) will be stalled until such auth is gotten.
         */
        REAUTH,

        /**
         * From Client: The client can use a separate AUTH message to send over the requested {@link #REAUTH} (it could
         * just as well put the 'auth' in a PING or any other message it had pending).
         */
        AUTH,

        /**
         * A PING, to which a {@link #PONG} is expected.
         */
        PING,

        /**
         * A Reply to a {@link #PING}.
         */
        PONG,
    }

    /**
     * WebSocket CloseCodes used in MatsSocket, and for what. Using both standard codes, and MatsSocket-specific/defined
     * codes.
     * <p/>
     * Note: Plural "Codes" since that is what the JSR 356 Java WebSocket API {@link CloseCodes does..!}
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
         * <p/>
         * May also be used locally from the Client: If the PreConnectOperation return status code 401 or 403 or the
         * WebSocket connect attempt raises error too many times (e.g. total 3x number of URLs), the MatsSocket will be
         * "Closed Session" with this status code.
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
         * 356 WebSocket API for Java sends GOING_AWAY upon socket close <i>due to timeout</i>. Since a timeout can
         * happen if we loose connection and thus can't convey PINGs, the MatsSocketServer must not interpret Jetty's
         * timeout-close as Close Session. Likewise, if the client just experienced massive lag on the connection, and
         * thus didn't get the PING over to the server in a timely fashion, but then suddenly gets Jetty's timeout close
         * with GOING_AWAY, this should not be interpreted by the client as the server wants to close the
         * MatsSocketSession.
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
         * 4001: From Server side, Client should REJECT all outstanding and "crash"/reboot application: A
         * HELLO:RECONNECT was attempted, but the session was gone. A considerable amount of time has probably gone by
         * since it last was connected. The client application must get its state synchronized with the server side's
         * view of the world, thus the suggestion of "reboot".
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
        RECONNECT(4002),

        /**
         * 4003: From Server side: Currently used in the specific situation where a MatsSocket client connects with the
         * same MatsSocketSessionId as an existing WebSocket connection. This could happen if the client has realized
         * that a connection is wonky and wants to ditch it, but the server has not realized the same yet. When the
         * server then gets the new connect, it'll see that there is an active WebSocket already. It needs to close
         * that. But the client "must not do anything" other than what it already is doing - reconnecting.
         */
        DISCONNECT(4003);

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
