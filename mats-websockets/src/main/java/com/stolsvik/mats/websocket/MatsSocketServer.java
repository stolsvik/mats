package com.stolsvik.mats.websocket;

import java.security.Principal;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentMap;

import javax.websocket.CloseReason.CloseCode;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.Session;

import com.stolsvik.mats.MatsEndpoint.DetachedProcessContext;
import com.stolsvik.mats.MatsEndpoint.MatsObject;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsInitiator.MatsBackendRuntimeException;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;
import com.stolsvik.mats.websocket.AuthenticationPlugin.AuthenticationContext;
import com.stolsvik.mats.websocket.AuthenticationPlugin.DebugOption;
import com.stolsvik.mats.websocket.AuthenticationPlugin.SessionAuthenticator;
import com.stolsvik.mats.websocket.MatsSocketServer.ActiveMatsSocketSession.MatsSocketSessionState;
import com.stolsvik.mats.websocket.MatsSocketServer.SessionEstablishedEvent.SessionEstablishedEventType;
import com.stolsvik.mats.websocket.MatsSocketServer.SessionRemovedEvent.SessionRemovedEventType;

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
 * solutions (which can go concurrent with MatsSocket's WebSocket), or employ a secondary MatsSocket instance for such
 * larger message if you just cannot get enough of the MatsSocket API - but you would still get more control over e.g.
 * progress and cancellation of the transfer with the HTTP approach.</li>
 * </ul>
 * <p />
 * <b>WARNING! Please make absolutely certain that you understand that incoming messages to MatsSocketEndpoints
 * originate directly from the hostile Internet, and you cannot assume that any values are benign - they might be
 * tailored to crash or hack your system. Act and code accordingly! It is imperative that you NEVER rely on information
 * from the incoming message to determine which user to act as: It would e.g. be absolutely crazy to rely on a parameter
 * in an incoming DTO declaring "userId" when deciding which user to place an order for, or to withdraw money from. Such
 * information must be gotten by the authenticated elements, which are
 * {@link MatsSocketEndpointIncomingContext#getPrincipal() incomingContext.getPrincipal()} and
 * {@link MatsSocketEndpointIncomingContext#getUserId() incomingContext.getUserId()}. The same holds for authorization
 * of access: If the incoming DTO from the Client demands to see <i>'top secret folder'</i>, you cannot rely on this,
 * even though you filtered which elements the user can request in a <i>'folders you are allowed to access'</i>-list in
 * a previous Reply to the Client; the user handling the Client could just hack the request DTO to request the top
 * secret folder even though he does not have access. You must therefore again do such authorization upon every handling
 * of incoming messages.</b>
 *
 * @author Endre Stølsvik 2019-11-28 16:15 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsSocketServer {

    /**
     * Registers a MatsSocket Endpoint, including a {@link ReplyAdapter} which can adapt the reply from the Mats
     * endpoint before being fed back to the MatsSocket - and also decide whether to resolve or reject the waiting
     * Client Promise.
     * <p />
     * NOTE: If you supply {@link MatsObject MatsObject} as the type 'MR', you will get such an instance, and can decide
     * yourself what to deserialize it to - it will be like having a Java method taking Object as argument. However,
     * there is no "instanceof" functionality, so you will need to know what type of object it is by other means, e.g.
     * by putting some up-flow information on the Mats {@link ProcessContext ProcessContext} as a
     * {@link ProcessContext#setTraceProperty(String, Object) TraceProperty}.
     * <p />
     * NOTE: You need not be specific with the 'R' type being created in the {@link ReplyAdapter} - it can be any
     * superclass of your intended Reply DTO(s), up to {@link Object}. However, the introspection aspects will take a
     * hit, i.e. when listing {@link #getMatsSocketEndpoints() all MatsSocketEndpoints} on some monitoring/introspection
     * page. This is also a bit like with Java: Methods returning Object as return type are annoying, but can
     * potentially be of value in certain convoluted scenarios.
     */
    <I, MR, R> MatsSocketEndpoint<I, MR, R> matsSocketEndpoint(String matsSocketEndpointId,
            Class<I> incomingClass, Class<MR> matsReplyClass, Class<R> replyClass,
            IncomingAuthorizationAndAdapter<I, MR, R> incomingAuthEval, ReplyAdapter<I, MR, R> replyAdapter);

    /**
     * <i>(Convenience-variant of the base method)</i> Registers a MatsSocket Endpoint where there is no replyAdapter -
     * the reply from the Mats endpoint is directly fed back (as "resolved") to the MatsSocket. The Mats Reply class and
     * MatsSocket Reply class is thus the same.
     * <p />
     * Types: <code>MR = R</code>
     * <p />
     * NOTE: In this case, you cannot specify {@link Object} as the 'R' type - this is due to technical limitations with
     * how MatsSocket interacts with Mats: You probably have something in mind where a Mats endpoint is configured to
     * "return Object", i.e. can return whatever type of DTO it wants, and then feed the output of this directly over as
     * the Reply of the MatsSocket endpoint, and over to the Client. However, Mats's and MatsSocket's serialization
     * mechanisms are not the same, and can potentially be completely different. Therefore, there needs to be an
     * intermediary that deserializes whatever comes out of Mats, and (re-)serializes this to the MatsSocket Endpoint's
     * Reply. This can EITHER be accomplished by specifying a specific class, in which case MatsSocket can handle this
     * task itself by asking Mats to deserialize to this specified type, and then returning the resulting instance as
     * the MatsSocket Endpoint Reply (which then will be serialized using the MatsSocket serialization mechanism). With
     * this solution, there is no need for ReplyAdapter, which is the very intent of the present variant of the
     * endpoint-creation methods. OTHERWISE, this can be accomplished using user-supplied code, i.e. the ReplyAdapter.
     * The MatsSocket endpoint can then forward to one, or one of several, Mats endpoints that return a Reply with one
     * of a finite set of types. The ReplyAdapter would then have to choose which type to deserialize the Mats Reply
     * into (using the {@link MatsObject#toClass(Class) matsObject.toClass(&lt;class&gt;)} functionality), and then
     * return the desired MatsSocket Reply (which, again, will be serialized using the MatsSocket serialization
     * mechanism).
     */
    default <I, R> MatsSocketEndpoint<I, R, R> matsSocketEndpoint(String matsSocketEndpointId,
            Class<I> incomingClass, Class<R> replyClass,
            IncomingAuthorizationAndAdapter<I, R, R> incomingAuthEval) {
        // Create an endpoint having the MR and R both being the same class, and lacking the AdaptReply.
        return matsSocketEndpoint(matsSocketEndpointId, incomingClass, replyClass, replyClass,
                incomingAuthEval, null);
    }

    /**
     * <i>(Convenience-variant of the base method)</i> Registers a MatsSocket Endpoint meant for situations where you
     * intend to reply directly in the {@link IncomingAuthorizationAndAdapter} without forwarding to Mats.
     * <p />
     * Types: <code>MR = void</code>
     * <p />
     * NOTE: In this case, it is possible to specify 'R' = {@link Object}. This is because you do not intend to
     * interface with Mats at all, so there is no need for MatsSocket Server to know which type any Mats Reply is.
     */
    default <I, R> MatsSocketEndpoint<I, ?, R> matsSocketDirectReplyEndpoint(String matsSocketEndpointId,
            Class<I> incomingClass, Class<R> replyClass,
            IncomingAuthorizationAndAdapter<I, Void, R> incomingAuthEval) {
        // Create an endpoint having the MI and MR both being Void, and lacking the AdaptReply.
        return matsSocketEndpoint(matsSocketEndpointId, incomingClass, Void.TYPE, replyClass,
                incomingAuthEval, null);
    }

    /**
     * <i>(Convenience-variant of the base method)</i> Registers a MatsSocket Terminator (no reply), specifically for
     * Client-to-Server "SEND", and to accept a "REPLY" from a Server-to-Client "REQUEST".
     * <p />
     * Types: <code>MR = R = void</code>>
     *
     * {@link #request(String, String, String, Object, String, String, byte[])} request}) operations from the Client.
     */
    default <I> MatsSocketEndpoint<I, Void, Void> matsSocketTerminator(String matsSocketEndpointId,
            Class<I> incomingClass, IncomingAuthorizationAndAdapter<I, Void, Void> incomingAuthEval) {
        // Create an endpoint having the MR and R both being Void, and lacking the AdaptReply.
        return matsSocketEndpoint(matsSocketEndpointId, incomingClass, Void.TYPE, Void.TYPE,
                incomingAuthEval, null);
    }

    /**
     * Should handle (preliminary) Authorization evaluation on the supplied
     * {@link MatsSocketEndpointIncomingContext#getPrincipal() Principal} and decide whether this message should be
     * forwarded to the Mats fabric (or directly resolved, rejected or denied). If it decides to forward to Mats, it
     * then adapt the incoming MatsSocket message to a message that can be forwarded to the Mats fabric - it is assumed
     * that the type for the incoming MatsSocket message and the type of the incoming Mats message seldom will be
     * identical.
     * <p />
     * <b>Please make absolutely certain that you understand that these messages originate directly from the hostile
     * Internet, and you cannot assume that any values are benign - they might be tailored to crash or hack your system.
     * Act and code accordingly! Read more at the "Warning" in the {@link MatsSocketServer} class JavaDoc.</b>
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
    interface IncomingAuthorizationAndAdapter<I, MR, R> {
        void handleIncoming(MatsSocketEndpointIncomingContext<I, MR, R> ctx, Principal principal, I msg);
    }

    /**
     * Used to transform the reply message from the Mats endpoint to the reply for the MatsSocket endpoint, and decide
     * whether to resolve or reject the waiting Client-side Promise - <b>this should only be pure Java DTO
     * transformation code!</b>
     * <p/>
     * <b>Note: This should <i>preferentially</i> only be pure and quick Java code, without much database access or
     * lengthy computations</b> - such things should happen in the Mats stages. The ReplyAdapter is run in the common
     * MatsSocket reply Mats Terminator, which is shared between all MatsSocket Endpoints on this node, and should thus
     * preferably just adapt the Mats reply DTO to the MatsSocket endpoint's expected reply DTO (and decide whether to
     * resolve or reject).
     * <p/>
     * Note: It is imperative that this does not perform any state-changes to the system - it should be utterly
     * idempotent, i.e. invoking it a hundred times with the same input should yield the same result. (Note: Logging is
     * never considered state changing!)
     */
    @FunctionalInterface
    interface ReplyAdapter<I, MR, R> {
        void adaptReply(MatsSocketEndpointReplyContext<I, MR, R> ctx, MR matsReply);
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
     *
     * @throws DataStoreException
     *             if the {@link ClusterStoreAndForward} makes any problems when putting the outgoing message in the
     *             oubox.
     */
    void send(String sessionId, String traceId, String clientTerminatorId, Object messageDto) throws DataStoreException;

    /**
     * Initiates a request to the specified MatsSocketSession, to the specified Client EndpointId, with a replyTo
     * specified to (typically) a {@link #matsSocketTerminator(String, Class, IncomingAuthorizationAndAdapter)
     * MatsSocket terminator} - which includes a String "correlationString" and byte array "correlationBinary" which can
     * be used to correlate the reply to the request (available
     * {@link MatsSocketEndpointIncomingContext#getCorrelationString() here} and
     * {@link MatsSocketEndpointIncomingContext#getCorrelationString() here} for the reply processing). Do note that
     * since you have no control of when the Client decides to close the browser or terminate the app, you have no
     * guarantee that a reply will ever come - so code accordingly.
     * <p/>
     * Note: the {@code correlationString} and {@code correlationBinary} are not sent over to the client, but stored
     * server side in the {@link ClusterStoreAndForward}. This both means that you do not need to be afraid of size (but
     * storing megabytes is silly anyway), but more importantly, this data cannot be tampered with client side - you can
     * be safe that what you gave in here is what you get out in the
     * {@link MatsSocketEndpointIncomingContext#getCorrelationString()} and
     * {@link MatsSocketEndpointIncomingContext#getCorrelationBinary()}.
     * <p/>
     * Note: To check whether the client Resolved or Rejected the request, use
     * {@link MatsSocketEndpointIncomingContext#getMessageType()}.
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
     *
     * @throws DataStoreException
     *             if the {@link ClusterStoreAndForward} makes any problems when putting the outgoing message in the
     *             oubox.
     */
    void request(String sessionId, String traceId, String clientEndpointId, Object requestDto,
            String replyToMatsSocketTerminatorId, String correlationString, byte[] correlationBinary)
            throws DataStoreException;

    /**
     * Publish a Message to the specified Topic, with the specified TraceId. This is pretty much a direct invocation of
     * {@link MatsInitiate#publish(Object)} on the {@link MatsFactory}, and thus you might get the
     * {@link MatsBackendRuntimeException} which {@link MatsInitiate#initiateUnchecked(InitiateLambda)} raises.
     * <p />
     * Note: A published message will be broadcast to all nodes in the MatsSocketServer instance (where each instance
     * then evaluates if it have subscribers to the topic and forwards to those). In addition, a certain number of
     * messages per topic will be retained in memory to support "replay of lost messages" when a Client looses
     * connection and must reconnect. You should consider these facts when designing usage of pub/sub. Messages over
     * topics should generally be of interest to more than one party. While it is certainly feasible to have
     * user-specific, or even session-specific topics, which could be authorized to only be subscribable by the "owning
     * user" or even "owning session" (by use of the
     * {@link SessionAuthenticator#authorizeUserForTopic(AuthenticationContext, String) AuthenticationPlugin}), the
     * current implementation of pub/sub will result in quite a bit of overhead with such an approach. Also, even for
     * messages that are of interest to multiple parties, you should consider the size of the messages: Maybe not send
     * large PDFs or the entire ISO-images of "newly arrived BlueRays" over a topic - instead send a small notification
     * about the fresh BlueRay availability including just essential information and an id, and then the client can
     * decide whether he wants to download it.
     *
     * @param traceId
     *            traceId for the flow.
     * @param topicId
     *            which Topic to Publish on.
     * @param messageDto
     *            the message to Publish.
     * @throws MatsBackendRuntimeException
     *             if the Mats implementation cannot connect to the underlying message broker, or are having problems
     *             interacting with it.
     */
    void publish(String traceId, String topicId, Object messageDto) throws MatsBackendRuntimeException;

    /**
     * Root for both {@link MatsSocketEndpointIncomingContext} and {@link MatsSocketEndpointReplyContext}.
     *
     * @param <I>
     *            MatsSocket Incoming type
     * @param <MR>
     *            Mats reply type (i.e. the type of the reply from the Mats endpoint which was forwarded to)
     * @param <R>
     *            MatsSocket Reply type
     */
    interface MatsSocketEndpointContext<I, MR, R> {
        /**
         * @return the {@link MatsSocketEndpoint} for which this context relates - has e.g. the endpointId.
         */
        MatsSocketEndpoint<I, MR, R> getMatsSocketEndpoint();
    }

    /**
     * The context which the {@link IncomingAuthorizationAndAdapter} gets to work with when handling an incoming
     * MatsSocket message.
     * <p />
     * <b>Please make absolutely certain that you understand that these messages originate directly from the hostile
     * Internet, and you cannot assume that any values are benign - they might be tailored to crash or hack your system.
     * It is imperative that you NEVER rely on information from the incoming message to determine which user to act as -
     * a parameter in an incoming DTO declaring "userId" would e.g. be absolutely crazy to rely on when deciding which
     * user to withdraw money from. Such information must be gotten by the authenticated elements, which are
     * {@link #getPrincipal()} and {@link #getUserId()}. The same holds for authorization of access: If the incoming DTO
     * from the Client demands to see 'top secret folder', you cannot rely on this, even though you filtered which
     * elements the user can request in a 'folders you are allowed to access'-list in a previous reply; the user
     * handling the Client could just hack the request DTO to request that folder anyway. You must therefore again do
     * the authorization upon every handling of incoming messages.</b>
     *
     * @param <I>
     *            MatsSocket Incoming type
     * @param <MR>
     *            Mats reply type (i.e. the type of the reply from the Mats endpoint which was forwarded to)
     * @param <R>
     *            MatsSocket Reply type
     */
    interface MatsSocketEndpointIncomingContext<I, MR, R> extends MatsSocketEndpointContext<I, MR, R> {
        /**
         * @return the {@link LiveMatsSocketSession} for the requesting MatsSocketSession.
         */
        LiveMatsSocketSession getSession();

        /**
         * @return current <i>Authorization Value</i> in effect for the MatsSocket that delivered the message. This
         *         String is what resolves to the {@link #getPrincipal() current Principal} and {@link #getUserId()
         *         UserId} via the {@link AuthenticationPlugin}.
         */
        String getAuthorizationValue();

        /**
         * @return the resolved Principal from the {@link #getAuthorizationValue() Authorization Value}, via the
         *         {@link AuthenticationPlugin}. It is assumed that you must cast this to a more specific class which
         *         the current {@link AuthenticationPlugin} provides.
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

    interface MatsSocketEndpointReplyContext<I, MR, R> extends MatsSocketEndpointContext<I, MR, R> {
        /**
         * @return the {@link DetachedProcessContext} of the Mats incoming handler.
         */
        DetachedProcessContext getMatsContext();

        /**
         * @return the reply from Mats, which is to be adapted to the MatsSocketReply.
         */
        MR getMatsReplyMessage();

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
        /**
         * @return the Id of the MatsSocketEndpoint / MatsSocketTerminator (remember, a "Terminator" is just a special
         *         case of an endpoint, one which does not Reply).
         */
        String getMatsSocketEndpointId();

        /**
         * @return the expected type of incoming messages (coming in over the WebSocket, from the MatsSocket Client).
         */
        Class<I> getIncomingClass();

        /**
         * @return the expected type of Replies from the invoked/forwarded-to Mats endpoint. Will be
         *         <code>Void.TYPE</code> (i.e. <code>void.class</code>) for Terminators.
         */
        Class<MR> getMatsReplyClass();

        /**
         * @return the type of the Replies from this endpoint, if any (being sent back over the WebSocket, to the
         *         MatsSocket Client). Will be <code>Void.TYPE</code> (i.e. <code>void.class</code>) for Terminators.
         */
        Class<R> getReplyClass();
    }

    /**
     * @return all registered MatsSocketEndpoints, as a <code>SortedMap[endpointId, endpoint]</code>.
     */
    SortedMap<String, MatsSocketEndpoint<?, ?, ?>> getMatsSocketEndpoints();

    /**
     * Unless restricted by the "constraint parameters", this method returns <i>all</i> MatsSocketSessions on this
     * MatsSocketServer instance, regardless of whether the session currently is connected, and if connected, which node
     * it is connected to. This is done by reading from the {@link ClusterStoreAndForward data store}, as opposed to
     * methods {@link #getActiveMatsSocketSessions()} and {@link #getLiveMatsSocketSessions()}, which returns result
     * from <i>this node's</i> internal structures - and therefore only returns sessions that are connected <i>right
     * now</i>, and are connected to <i>this node</i>. This means that you will get returned both connected sessions,
     * and sessions that are not currently connected (unless restricting this via parameter 'onlyActive'). The latter
     * implies that they are state={@link MatsSocketSessionState#DEREGISTERED}, and the
     * {@link MatsSocketSession#getNodeName()} returns <code>Optional.empty()</code>.
     * <p />
     * The parameters are constraints - if a parameter is <code>null</code> or <code>false</code>, that parameter is not
     * used in the search criteria, while if it is set, that parameter will constrain the search.
     *
     * @param onlyActive
     *            If <code>true</code>, only returns "active" MatsSocketSessions, currently being connected to some
     *            node, i.e. having {@link MatsSocketSession#getNodeName()} NOT returning <code>Optional.empty()</code>.
     * @param userId
     *            If non-<code>null</code>, restricts the results to sessions for this particular userId
     * @param appName
     *            If non-<code>null</code>, restricts the results to sessions for this particular app-name. Do realize
     *            that it is the Client that specifies this value, there is no restriction and you cannot trust that
     *            this String falls within your expected values.
     * @param appVersionAtOrAbove
     *            If non-<code>null</code>, restricts the results to sessions having app-version at or above the
     *            specified value, using ordinary alphanum comparison. Do realize that it is the Client that specifies
     *            this value, there is no restriction and you cannot trust that this String falls within your expected
     *            values.
     * @return the list of all MatsSocketSessions currently registered with this MatsSocketServer instance matching the
     *         constraints if set - as read from the {@link ClusterStoreAndForward data store}.
     *
     * @throws DataStoreException
     *             if the {@link ClusterStoreAndForward} makes any problems when reading sessions from it.
     */
    List<MatsSocketSessionDto> getMatsSocketSessions(boolean onlyActive, String userId,
            String appName, String appVersionAtOrAbove) throws DataStoreException;

    /**
     * Like {@link #getMatsSocketSessions(boolean, String, String, String)}, only returning the count - this might be
     * interesting if there are very many sessions, and you do not need the full DTOs of every Session, just the count
     * for a metric to graph or similar.
     *
     * @return the count of all MatsSocketSessions currently registered with this MatsSocketServer instance matching the
     *         constraints if set - as read from the {@link ClusterStoreAndForward data store}.
     *
     * @throws DataStoreException
     *             if the {@link ClusterStoreAndForward} makes any problems when reading sessions from it.
     */
    int getMatsSocketSessionsCount(boolean onlyActive, String userId,
            String appName, String appVersionAtOrAbove) throws DataStoreException;

    /**
     * A MatsSocketSession, either as represented in the {@link ClusterStoreAndForward data store} when gotten via
     * {@link #getMatsSocketSessions(boolean, String, String, String)} (returning {@link MatsSocketSessionDto
     * MatsSocketSessionDto}), or an {@link ActiveMatsSocketSession ActiveMatsSocketSession} representing an active
     * MatsSocketSession connected to this node of the MatsSocketServer instance when gotten via
     * {@link #getActiveMatsSocketSessions()} (returning {@link ActiveMatsSocketSessionDto ActiveMatsSocketSessionDto}),
     * or a {@link LiveMatsSocketSession LiveMatsSocketSession} which is an interface view over the actual live session
     * in the MatsSocketServer when gotten via {@link #getLiveMatsSocketSessions()}.
     */
    interface MatsSocketSession {
        /**
         * @return the MatsSocketSessionId of this MatsSocketSession.
         */
        String getMatsSocketSessionId();

        /**
         * @return the userId owning this MatsSocketSession.
         */
        String getUserId();

        /**
         * @return when the MatsSocketSession and its Id was initially created. Will not be reset due to cycles of
         *         {@link SessionRemovedEventType#DEREGISTER} and {@link SessionEstablishedEventType#RECONNECT}, as
         *         {@link ActiveMatsSocketSession#getSessionEstablishedTimestamp()} does.
         */
        Instant getSessionCreatedTimestamp();

        /**
         * @return if this is an instance of {@link ActiveMatsSocketSessionDto}, effectively returns
         *         <code>System.currentTimeMillis()</code> of when
         *         {@link MatsSocketServer#getActiveMatsSocketSessions()} was invoked, as it was live at the time of
         *         invocation). If this is an instance of {@link LiveMatsSocketSession}, it literally returns
         *         <code>System.currentTimeMillis()</code>, as it is live right now. If this is an instance of
         *         {@link MatsSocketSessionDto}, the reason you have this instance is that its contents was read from
         *         the {@link ClusterStoreAndForward data store} using
         *         {@link MatsSocketServer#getMatsSocketSessions(boolean, String, String, String)}, and then this method
         *         returns last "liveliness update" from the data store, which is the last time this MatsSocketSession
         *         registered (i.e. became SESSION_ESTABLISHED), or from later periodic updates every few minutes while
         *         it is/was active.
         */
        Instant getSessionLivelinessTimestamp();

        /**
         * Note: Do realize that it is the Client that specifies this value, there is no restriction and you cannot
         * trust that this String falls within your expected values.
         *
         * @return a descriptive String representing which MatsSocket Client lib is in use ("name,version"), with
         *         additional runtime "name and version" information tacked on separated by ";". For MatsSocket.js, this
         *         may read as follows: <code>"MatsSocket.js,v0.10.0; User-Agent: {userAgentString}"</code>, or
         *         <code>"MatsSocket.js,v0.10.0; Node.js: {node version}"</code>. For MatsSocket.dart, it could read
         *         <code>"MatsSocket.dart,v0.10.0; Dart: {dart version}; Flutter: {flutter SDK version}"</code>, or
         *         <code>"MatsSocket.dart,v0.10.0; Dart: {dart version}; User-Agent: {userAgentString}"</code>
         */
        String getClientLibAndVersions();

        /**
         * Note: Do realize that it is the Client that specifies this value, there is no restriction and you cannot
         * trust that this String falls within your expected values.
         *
         * @return the "AppName" that the Client registers as, could be something like "MegaCorpWebBank". This should be
         *         rather long-term, not changing between versions of this particular application. It may be used to
         *         distinguish between two different apps using the same MatsSocketServer instance, where one of them
         *         handles Server-to-Client Requests of type "UserInfo.getCurrentGpsPosition", while the other app does
         *         not have this endpoint.
         */
        String getAppName();

        /**
         * Note: Do realize that it is the Client that specifies this value, there is no restriction and you cannot
         * trust that this String falls within your expected values.
         *
         * @return the "AppVersion" that the Client registers as, could be something like "1.2.3-2020-04-07_beta". This
         *         should be something sane, preferably which can be alphanum-compared, so that the server could know
         *         that at-or-above "1.2.1xxxx", the app "MegaCorpWebBank" handles Server-to-Client Requests of type
         *         "UserInfo.getCurrentGpsPosition", but before this version, that particular endpoint was not
         *         available.
         */
        String getAppVersion();

        /**
         * @return the name of the node that holds the {@link LiveMatsSocketSession LiveMatsSocketSession} if it is
         *         active (i.e. state={@link MatsSocketSessionState#SESSION_ESTABLISHED SESSION_ESTABLISHED}),
         *         <code>Optional.empty()</code> if not active. Will always return non-empty on an
         *         {@link ActiveMatsSocketSession} and {@link LiveMatsSocketSession} instance.
         */
        Optional<String> getNodeName();
    }

    /**
     * This returns static, frozen-in-time, "copied-out" DTO-variants of the {@link #getLiveMatsSocketSessions()
     * LiveMatsSocketSessions}. Please observe the difference between {@link ActiveMatsSocketSession} and
     * {@link LiveMatsSocketSession}. If you have a massive amount of sessions, and only need the sessions for
     * appName="MegaCorpWebBank", then you should consider not employing this method, but instead do a variant of what
     * this method does, where you restrict the "copy out" to the relevant sessions:
     *
     * <pre>
     * SortedMap&lt;String, ActiveMatsSocketSessionDto&gt; ret = new TreeMap&lt;&gt;();
     * for (LiveMatsSocketSession liveSession : getLiveMatsSocketSessions().values()) {
     *     // === HERE YOU MAY ADD CRITERIA on the LiveMatsSocketSession, doing 'continue' if not matched ===
     *     // :: "Copy it out"
     *     ActiveMatsSocketSessionDto activeSession = liveSession.toActiveMatsSocketSession();
     *     // ?: Check that the LiveSession is still SESSION_ESTABLISHED
     *     if (liveSession.getState() != MatsSocketSessionState.SESSION_ESTABLISHED) {
     *         // -&gt; No, it changed during copying, so then we drop this.
     *         continue;
     *     }
     *     // Add to result Map
     *     ret.put(activeSession.getMatsSocketSessionId(), activeSession);
     * }
     * return ret;
     * </pre>
     *
     * @return a current snapshot of {@link ActiveMatsSocketSession ActiveMatsSocketSession}s - these are the
     *         <b>active</b> MatsSocketSessions <b>which are active right now on <i>this node</i></b> of the set of
     *         nodes (i.e. cluster) that represents this <i>instance</i> of MatsSocketServer. Notice that all returned
     *         instances had state={@link MatsSocketSessionState#SESSION_ESTABLISHED SESSION_ESTABLISHED} at the time of
     *         capture.
     * @see ActiveMatsSocketSession
     * @see #getLiveMatsSocketSessions()
     */
    SortedMap<String, ActiveMatsSocketSessionDto> getActiveMatsSocketSessions();

    /**
     * Represents an active node-local MatsSocketSession - i.e. this instance represents a connected MatsSocketSession,
     * having - or just had - an open WebSocket, which by definition can only be connected to a single node. Note that
     * this means that in a fairly load balanced 3-node MatsSocketServer cluster, you should get approximately 1/3 of
     * the ActiveMatsSocketSessions residing on "this" node, while 2/3 of them will reside on the "other" two nodes.
     * <p />
     * Note: The difference between "active" and "live" is that the "active" are dumb "data transfer objects" (DTOs)
     * which will serialize nicely with both Mats and MatsSocket serialization mechanisms, while the "live" are an
     * interface to the actual live sessions in the MatsSocketServer, and as such has more information - but cannot be
     * (easily) serialized, and not passed as in- or out objects on Mats endpoints.
     */
    interface ActiveMatsSocketSession extends MatsSocketSession {
        /**
         * @return the active Authorization value, only available when
         *         {@link MatsSocketSessionState#SESSION_ESTABLISHED}.
         */
        Optional<String> getAuthorization();

        /**
         * @return The name of the Principal, as returned by {@link Principal#getName()}, only available when
         *         {@link MatsSocketSessionState#SESSION_ESTABLISHED}.
         */
        Optional<String> getPrincipalName();

        /**
         * If we have a way to find the connected (remote) address, it will be exposed here. It can be set/overridden in
         * the {@link AuthenticationPlugin}. <b>Be advised that when the WebSocket Server (i.e. Servlet Container) is
         * behind a proxy, you will get the proxy's address, not the end client's address, aka <i>originating IP
         * address</i>.</b> This can, if you add the functionality to the {@link AuthenticationPlugin}, be gotten by
         * {@link #getOriginatingRemoteAddr()}.
         * <p />
         * Note: The remote address is not exposed by the <i>JSR 356 Java API for WebSockets</i> - but there are hacks
         * for different Servlet Containers to get it anyway. Check out class <code>RemoteAddressContainerHacks</code>.
         *
         * @return the remote address <b>if available</b> (should be an IP address).
         */
        Optional<String> getRemoteAddr();

        /**
         * The {@link AuthenticationPlugin} can set the originating remote IP address - which must be derived by headers
         * like <code>X-Forwarded-For</code> (<a href="https://en.wikipedia.org/wiki/X-Forwarded-For">Wikipedia</a>).
         * <b>Note the problems with this header: Since the originating client can set whatever he wants as initial
         * value, you cannot rely on the "first IP address in the list", as this might well be bogus. You must go from
         * the last and work your way up, and only trust the information given by proxies you are in control of: The
         * first IP address in the list that you do not know, is what you should set as originating remote address.</b>
         * Each proxy adds the remote address that he sees (i.e. who is the remote address for him). This means that the
         * last proxy's ip address is not in the list - this must be gotten by {@link #getRemoteAddr()}.
         *
         * @return the originating remote address <b>if {@link AuthenticationPlugin} has set it</b> (should be an IP
         *         address).
         */
        Optional<String> getOriginatingRemoteAddr();

        /**
         * @return which Topics this session has subscribed to.
         */
        SortedSet<String> getTopicSubscriptions();

        /**
         * @return when this MatsSocketSession was established - this timestamp is "reset" on every
         *         {@link SessionEstablishedEventType#RECONNECT RECONNECT} (and of course set on initial connect, i.e.
         *         {@link SessionEstablishedEventType#NEW NEW}), as opposed to {@link #getSessionCreatedTimestamp()}
         *         which is set a single time when the session if first created.
         */
        Instant getSessionEstablishedTimestamp();

        /**
         * @return timestamp (millis-since-epoch) of when we the client was last authenticated: When
         *         {@link SessionAuthenticator#initialAuthentication(AuthenticationContext, String)} returned
         *         {@link AuthenticationContext#authenticated(Principal, String)}, or when
         *         {@link SessionAuthenticator#reevaluateAuthentication(AuthenticationContext, String, Principal)}
         *         returned {@link AuthenticationContext#authenticated(Principal, String)} or
         *         {@link AuthenticationContext#stillValid()}.
         */
        Instant getLastAuthenticatedTimestamp();

        /**
         * @return timestamp (millis-since-epoch) of when we last received a PING message from the Client.
         */
        Instant getLastClientPingTimestamp();

        /**
         * @return timestamp (millis-since-epoch) of when the Server last received an <i>information bearing message</i>
         *         (SEND, REQUEST, RESOLVE or REJECT) from the Client, or when the Server last sent an information
         *         bearing message (SEND, REQUEST, RESOLVE, REJECT or PUB) to the Client
         */
        Instant getLastActivityTimestamp();

        /**
         * @return snapshot (i.e. newly created ArrayList) of last 200 (per default) envelopes going between client and
         *         server in both directions.
         */
        List<MatsSocketEnvelopeWithMetaDto> getLastEnvelopes();

        /**
         * The state of ActiveMatsSocketSession.
         */
        enum MatsSocketSessionState {
            /**
             * HELLO not yet processed - only accepts messages up to HELLO. This is the initial state of a
             * MatsSocketSession, but you will probably never see an instance in this state, as the session is not
             * registered with the MatsSocketServer until HELLO is processed, and thus the session is
             * {@link #SESSION_ESTABLISHED}
             */
            NO_SESSION(true),

            /**
             * HELLO is processed and auth verified, and we are processing all kinds of messages.
             * <p/>
             * <b>Note: All fields shall be set in this state, i.e. auth string, principal, userId, websocket etc.</b>
             */
            SESSION_ESTABLISHED(true),

            /**
             * This {@link ActiveMatsSocketSession} instance is dead - <b>but the MatsSocketSession that this
             * {@link ActiveMatsSocketSession} represented is still "live" in the {@link ClusterStoreAndForward data
             * store}, i.e. it was just DEREGISTERED</b>, and can be "resurrected" by starting a new MatsSocketSession
             * and include the existing MatsSocketSessionId in the HELLO message. <b>Do understand that it will not be
             * the same instance of {@link ActiveMatsSocketSession ActiveMatsSocketSession} that is "reanimated" - a new
             * instance will be created if the client reconnects.</b>
             * <p/>
             * <b>Note: Several "active fields" are nulled out when in this state.</b>
             */
            DEREGISTERED(false),

            /**
             * This {@link ActiveMatsSocketSession} instance is dead - <b>and the MatsSocketSession that this
             * {@link ActiveMatsSocketSession} represented was CLOSED</b> - i.e. the MatsSocketSession is gone forever,
             * any outstanding replies are dead etc, <b>and the MatsSocketSession cannot be "resurrected".</b>
             * <p/>
             * <b>Note: Several "active fields" are nulled out when in this state.</b>
             */
            CLOSED(false);

            private final boolean acceptMessages;

            MatsSocketSessionState(boolean acceptMessages) {
                this.acceptMessages = acceptMessages;
            }

            /**
             * @return whether in this state the MatsSocketSession handles incoming and outgoing messages - i.e. whether
             *         the WebSocket message handler should process incoming messages, or the outgoing handler should
             *         send outgoing messages ({@link #SESSION_ESTABLISHED} and {@link #NO_SESSION}, for the latter only
             *         accepts HELLO), or reject them outright ({@link #DEREGISTERED} and {@link #CLOSED}).
             */
            public boolean isHandlesMessages() {
                return acceptMessages;
            }
        }
    }

    /**
     * Imagine that the MatsSocketServer uses a {@link ConcurrentMap} to keep its set of local, live, currently
     * connected MatsSocketSessions. This method then returns an unmodifiable view of this Map. This means that you can
     * get session instances, and iterate over it, but the contents will change over time as Clients come and go, i.e.
     * connects and disconnects. It also means that you can get this Map instance once, and keep a local copy of it, and
     * it will always be current. It again also means that if you want a "static list" of these sessions, either use
     * {@link #getActiveMatsSocketSessions()} which gives you a snapshot, "frozen-in-time" view of the active sessions,
     * where both the sessions, and the contents of the sessions, are static. Or you may copy the values of this
     * returned Map into another container - but in the latter case, the <i>contents</i> of those LiveMatsSocketSession
     * instances are still live. Please observe the difference between {@link ActiveMatsSocketSession} and
     * {@link LiveMatsSocketSession}.
     *
     * @return an unmodifiable concurrent live view of {@link LiveMatsSocketSession LiveMatsSocketSession}s - these are
     *         the <b>live</b> MatsSocketSessions <b>which are active right now on <i>this node</i></b> of the set of
     *         nodes (i.e. cluster) that represents this <i>instance</i> of MatsSocketServer.
     * @see #getActiveMatsSocketSessions()
     */
    Map<String, LiveMatsSocketSession> getLiveMatsSocketSessions();

    /**
     * A <i>live</i> representation of a MatsSocketSession.
     * <p />
     * Note: The difference between "active" and "live" is that the "active" are dumb "data transfer objects" (DTOs)
     * which will serialize nicely with both Mats and MatsSocket field-based serialization mechanisms, while the "live"
     * are an interface to the actual live sessions in the MatsSocketServer, and as such has more information - but
     * cannot be (easily) serialized, and not passed as in- or out objects on Mats endpoints.
     * <p/>
     * <b>NOTE: The underlying instance is live in that it is just an interface over the live representation which the
     * MatsSocketServer is using to hold the MatsSocketSession.</b> As such, most methods can return different values
     * between each time you invoke them - and there is no synchronization between the methods, so you could read ACTIVE
     * using {@link #getState()}, but when you read {@link #getAuthorization()}, it could return Optional.empty() -
     * because the user concurrently closed the session (and if reading getState() again, it would now return CLOSED).
     */
    interface LiveMatsSocketSession extends ActiveMatsSocketSession {
        /**
         * @return the current state of this {@link ActiveMatsSocketSession ActiveMatsSocketSession}. You should really
         *         only ever see {@link MatsSocketSessionState#SESSION_ESTABLISHED SESSION_ESTABLISHED}, but due to the
         *         concurrency wrt. you getting hold of an LiveMatsSocketSession instance, and the Client performing a
         *         {@link SessionRemovedEventType#DEREGISTER DEREGISTER} or {@link SessionRemovedEventType#CLOSE CLOSE},
         *         you may also observe {@link MatsSocketSessionState#DEREGISTERED DEREGISTERED} and
         *         {@link MatsSocketSessionState#CLOSED CLOSED}.
         */
        MatsSocketSessionState getState();

        /**
         * @return the WebSocket Session - the JSR 356 Java WebSocket API representation of the actual WebSocket
         *         connection. You should not really mess too much with this..!
         */
        Session getWebSocketSession();

        /**
         * @return the active Principal, only available when {@link MatsSocketSessionState#SESSION_ESTABLISHED}.
         */
        Optional<Principal> getPrincipal();

        /**
         * @return the set of {@link DebugOption} the the active {@link AuthenticationPlugin} allows the
         *         {@link #getPrincipal() current Principal} to request.
         */
        EnumSet<DebugOption> getAllowedDebugOptions();

        /**
         * @return a "frozen in time" copy of the this LiveMatsSocketSession as an {@link ActiveMatsSocketSessionDto}.
         *         Do observe that due to the concurrency of these live sessions, you may get a copy of when the session
         *         had become ({@link #getState() state}) {@link MatsSocketSessionState#DEREGISTERED DEREGISTERED} or
         *         {@link MatsSocketSessionState#CLOSED CLOSED}, and where some of the Optional-returning methods then
         *         returns Optional.empty(). If you do not want to handle such instances, then you might want to check
         *         {@link #getState()} <i>after</i> invoking this method, and if not
         *         {@link MatsSocketSessionState#SESSION_ESTABLISHED SESSION_ESTABLISHED}, then ditch the instance that
         *         you just copied out - this is what {@link #getActiveMatsSocketSessions()} does.
         */
        ActiveMatsSocketSessionDto toActiveMatsSocketSession();
    }

    /**
     * {@link SessionEstablishedEvent SessionEstablishedEvent} listeners will be invoked when an
     * {@link LiveMatsSocketSession LiveMatsSocketSession} is established <b>on this node</b> of the
     * {@link MatsSocketServer} instance cluster, i.e. the authentication/authorization is accepted, HELLO message from
     * Client is processed and MatsSocketSessionId is established. Note that this means that in a fairly load balanced
     * 3-node MatsSocketServer cluster, you should get approximately 1/3 of the SessionEstablishedEvents on "this" node,
     * while 2/3 of them will come on the "other" two nodes.
     * <p />
     * Note: A specific MatsSocketSession with a specific MatsSocketSessionId can be established multiple times, due to
     * {@link SessionEstablishedEventType#RECONNECT RECONNECT}.
     * <p />
     * <b>NOTE: You are advised against keeping hold of the {@link LiveMatsSocketSession LiveMatsSocketSession} instance
     * that is provided in the {@link SessionEstablishedEvent SessionEstablishedEvent}.</b> You can instead get a view
     * of the currently live sessions for this node by means of {@link #getLiveMatsSocketSessions()}. If you still
     * decide to hold on to these active sessions instances, you must be <b><i>very</i></b> certain to remove it from
     * your held instances when getting <b>any</b> {@link #addSessionRemovedEventListener(SessionRemovedEventListener)
     * SessionRemovedEvent}, meaning that you must remove it for any of the {@link SessionRemovedEventType#DEREGISTER
     * DEREGISTER}, {@link SessionRemovedEventType#CLOSE CLOSE} and {@link SessionRemovedEventType#TIMEOUT TIMEOUT}
     * event types: The live session instance is <i>dead</i> for all of these events. If you were to remove it only on
     * CLOSE or TIMEOUT, believing that a DEREGISTER is a "softer" removal, you have basically misunderstood! You could
     * then get a DEREGISTER (which actually is the server informing you that it has ditched this LiveMatsSocketSession
     * and the session is now solely represented in the {@link ClusterStoreAndForward data store}, while you still
     * stubbornly hold on to it!), and then not get a corresponding TIMEOUT for the same MatsSocketSessionId until many
     * hours, or days, later. If you fail to remove it at all, you will eventually get an OutOfMemory situation. The
     * reason here is that a MatsSocketSession instance is never "reanimated", even if the MatsSocketSession is just
     * DEREGISTERed: A new LiveMatsSocketSession instance is <i>always</i> created upon a {@link SessionEstablishedEvent
     * SessionEstablishedEvent}, both for {@link SessionEstablishedEventType#NEW NEW} <i>and</i>
     * {@link SessionEstablishedEventType#RECONNECT RECONNECT}
     *
     * @see #addSessionRemovedEventListener(SessionRemovedEventListener)
     * @see #getLiveMatsSocketSessions()
     * @see #getActiveMatsSocketSessions()
     *
     * @param listener
     *            the {@link SessionEstablishedEventListener SessionEstablishedListener} that shall get invoked when
     *            MatsSocketSessions are established.
     */
    void addSessionEstablishedEventListener(SessionEstablishedEventListener listener);

    @FunctionalInterface
    interface SessionEstablishedEventListener {
        void sessionEstablished(SessionEstablishedEvent event);
    }

    interface SessionEstablishedEvent {
        SessionEstablishedEventType getType();

        default String getMatsSocketSessionId() {
            return getMatsSocketSession().getMatsSocketSessionId();
        }

        LiveMatsSocketSession getMatsSocketSession();

        enum SessionEstablishedEventType {
            NEW,

            RECONNECT
        }
    }

    /**
     * {@link SessionRemovedEvent} listeners will be invoked when an {@link LiveMatsSocketSession} is removed from this
     * node of the {@link MatsSocketServer} instance cluster - this is both when a MatsSocketSession is
     * {@link SessionRemovedEventType#DEREGISTER DEREGISTERed}, in which case the Client can still
     * {@link SessionEstablishedEventType#RECONNECT RECONNECT} to the same MatsSocketSessionId, and when a
     * MatsSocketSession is {@link SessionRemovedEventType#CLOSE CLOSEd} or {@link SessionRemovedEventType#TIMEOUT
     * TIMEOUTed}. In the latter cases, any information of the MatsSocketSession and its MatsSocketSessionId are deleted
     * from the MatsSocketServer, and the session cannot be reconnected again.
     * <p />
     * Note: A specific MatsSocketSession can {@link SessionRemovedEventType#DEREGISTER DEREGISTER} multiple times, due
     * to it can {@link SessionEstablishedEventType#RECONNECT RECONNECT} again after each DEREGISTER. However, once it
     * has {@link SessionRemovedEventType#CLOSE CLOSE} or {@link SessionRemovedEventType#TIMEOUT TIMEOUT}, the session
     * cannot RECONNECT ever again, and hence those events are terminal wrt. to that specific MatsSocketSessionId.
     *
     * @see #addSessionEstablishedEventListener(SessionEstablishedEventListener)
     *
     * @param listener
     *            the {@link SessionEstablishedEventListener SessionEstablishedListener} that shall get invoked when
     *            MatsSocketSessions are removed (either deregistered, closed or timed out).
     */
    void addSessionRemovedEventListener(SessionRemovedEventListener listener);

    @FunctionalInterface
    interface SessionRemovedEventListener {
        void sessionRemoved(SessionRemovedEvent event);
    }

    interface SessionRemovedEvent {
        SessionRemovedEventType getType();

        String getMatsSocketSessionId();

        /**
         * @return the WebSocket "close code" if relevant (i.e. a WebSocket was actually connected: Won't have been with
         *         TIMEOUT, can have been with CLOSE, always will have been with DEREGISTER) - the close codes used with
         *         MatsSocket are detailed in {@link MatsSocketCloseCodes}.
         */
        Optional<Integer> getCloseCode();

        /**
         * @return a "reason phrase" corresponding to the {@link #getCloseCode() close code} in the WebSocket lingo -
         *         but will also be set to something interesting if a WebSocket connection was not present.
         */
        String getReason();

        /**
         * Type of "remove": Either DEREGISTER, CLOSE or TIMEOUT. You may get one or several invocations for a session:
         * You may get a CLOSE by itself: The user has logged in, does some stuff, and then "logs out" / closes the
         * browser tab (i.e. invoking <code>client.close()</code>), thus actively closing the session explicitly. Or,
         * you may first get a DEREGISTER e.g. due to the Client driving through a tunnel and loosing connection, in
         * which case the most likely result is (hopefully) that the Client again
         * {@link SessionEstablishedEventType#RECONNECT SessionEstablishedEventType.RECONNECT} - but if not, the Server
         * may shortly get a CLOSE - or much later a TIMEOUT. This means that the session may alter back and forth
         * between {@link #DEREGISTER} and {@link SessionEstablishedEventType#RECONNECT RECONNECT} multiple times, but
         * will always eventually get a {@link #CLOSE} or {@link #TIMEOUT}.
         */
        enum SessionRemovedEventType {
            /**
             * The WebSocket connection was severed without an explicit MatsSocketSession closing, e.g. because the user
             * driving through a Tunnel or closing the lid of his laptop, or similar things that break the WebSocket
             * connection. The MatsSocketSession is now still NOT {@link #isSessionClosed() closed} - the session will
             * <i>either</i> {@link SessionEstablishedEventType#RECONNECT SessionEstablishedEventType.RECONNECT} again,
             * or you will get a {@link #CLOSE} or {@link #TIMEOUT} at a later point.
             */
            DEREGISTER(false),

            /**
             * The MatsSocketSession was explicitly closed. This may, or may not, have implied that the WebSocket
             * connection was closed (either it was open, and was closed with this event - or it was already closed
             * (DEREGISTER)). The MatsSocketSession is now {@link #isSessionClosed() closed}.
             */
            CLOSE(true),

            /**
             * The MatsSocketSession was previously {@link #DEREGISTER}, but was now "cleaned out" by the timeouter. The
             * MatsSocketSession is now {@link #isSessionClosed() closed}.
             */
            TIMEOUT(true);

            private final boolean sessionClosed;

            SessionRemovedEventType(boolean sessionClosed) {
                this.sessionClosed = sessionClosed;
            }

            /**
             * @return whether this event type represents a closing of the session (i.e. removing all traces of it -
             *         type {@link #CLOSE} or {@link #TIMEOUT}), or if it can still
             *         {@link SessionEstablishedEventType#RECONNECT RECONNECT} with the same MatsSocketSessionId (type
             *         {@link #DEREGISTER}).
             */
            public boolean isSessionClosed() {
                return sessionClosed;
            }
        }
    }

    /**
     * {@link MessageEventListener MessageEventListener}s will be invoked for every processed incoming and outgoing
     * message for any session. It will be invoked <i>after</i> the message is processed OK on incoming, and
     * <i>after</i> the message is sent for outgoing. Note that the {@link MatsSocketEnvelopeWithMetaDto} contains more
     * information than is sent over the wire, this is the "WithMeta" aspect which holds processing metadata - the
     * wire-part is what is contained in {@link MatsSocketEnvelopeDto}.
     * <p />
     * <b>Note wrt. modifications on the <code>MatsSocketEnvelopeWithMetaDto</code>! All fields are public and
     * non-final, so you can modify it before e.g. sending it over Mats (e.g. nulling out the 'msg' field). However,
     * read the JavaDoc comment on the class: There is only one single instance for all listeners and
     * {@link ActiveMatsSocketSession#getLastEnvelopes()} "last envelopes"}, so clone it before modifying!</b>
     * <p />
     * Note: The last messages per {@link ActiveMatsSocketSession} is available via
     * {@link ActiveMatsSocketSession#getLastEnvelopes()}.
     *
     * @see ActiveMatsSocketSession#getLastEnvelopes()
     * @see MatsSocketEnvelopeWithMetaDto
     *
     * @param listener
     *            the {@link MessageEventListener} that will be invoked for every processed incoming and outgoing
     *            envelope for any session.
     */
    void addMessageEventListener(MessageEventListener listener);

    @FunctionalInterface
    interface MessageEventListener {
        void messagesProcessed(MessageEvent messageEvent);
    }

    interface MessageEvent {
        /**
         * @return which {@link LiveMatsSocketSession} that this list of envelopes concerns.
         */
        LiveMatsSocketSession getMatsSocketSession();

        /**
         * @return the list of envelopes that was processed "in one go". Often corresponds to a MatsSocket "pipeline",
         *         but this doesn't quite hold when the pipeline contains "control messages" (like HELLO and WELCOME,
         *         ACK, ACK2 etc), as these are internally processed one-by-one.
         */
        List<MatsSocketEnvelopeWithMetaDto> getEnvelopes();
    }

    /**
     * Closes the specified MatsSocketSession - can be used to forcibly close an active MatsSocketSession (i.e. "kick it
     * off"), and can also used to perform out-of-band closing of Session if the WebSocket is down (this is used in the
     * MatsSocket.js Client, where an "onunload"-listener is attached, so that if the user navigates away, every effort
     * is done to get the MatsSocketSession closed).
     * <p />
     * Note: An invocation of any {@link #addSessionRemovedEventListener(SessionRemovedEventListener) SessionRemoved
     * listeners} with type {@link SessionRemovedEventType#CLOSE CLOSE} will be issued.
     * <p />
     * Note: This can be done on any node of the MatsSocketServer-instance cluster, as the instruction will be forwarded
     * to the active node if the MatsSocketSession is not active on this node. If it is not active on any node, it will
     * nevertheless be closed in the {@link ClusterStoreAndForward data store} (i.e. the session cannot reconnect
     * again).
     *
     * @param sessionId
     *            the id of the Session to close.
     * @param reason
     *            a short descriptive String of why it was closed.
     */
    void closeSession(String sessionId, String reason);

    /**
     * Closes all {@link ActiveMatsSocketSession} on this node, closing the WebSocket with
     * {@link CloseCodes#SERVICE_RESTART} (assuming that a MatsSocket service will never truly go down, thus effectively
     * asking the client to reconnect, hopefully to another instance). Should be invoked at application shutdown.
     */
    void stop(int gracefulShutdownMillis);

    /**
     * RuntimeException raised from methods which directly interfaces with the {@link ClusterStoreAndForward} and which
     * cannot "hide" the situation if the data store doesn't work. These methods are
     * {@link #send(String, String, String, Object) send(..)},
     * {@link #request(String, String, String, Object, String, String, byte[]) request(..)},
     * {@link #getMatsSocketSessions(boolean, String, String, String) getMatsSocketSessions(..)} and
     * {@link #getMatsSocketSessionsCount(boolean, String, String, String)} getMatsSocketSessionsCount(..)}.
     */
    class DataStoreException extends RuntimeException {
        public DataStoreException(String message) {
            super(message);
        }

        public DataStoreException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * This is the entire "Wire transport" DTO of MatsSocket. For each {@link MessageType MessageType}, a certain set of
     * its properties needs to be present, with a couple of optional fields (chiefly 'desc', 'rd' and 'debug'). Note
     * that any message may contain the 'auth' field, thus updating the Server's "cached" Authorization value - it must
     * be present with (or before, but hey) the HELLO message. It may also be present with a dedicated
     * {@link MessageType#AUTH AUTH} message.
     * <p />
     * Note: For information: The {@link #msg}-field (which is defined as <code>Object</code>) is handled rather
     * specially: The serialization/deserialization-mechanism in the MatsSocket implementation is configured to treat it
     * differently depending on whether it is deserialized or serialized: When a message is incoming from the Client,
     * the 'msg' field is not deserialized to a specific type - instead it contains the "raw JSON" of the incoming DTO
     * as a String. This is necessary since we do not know the type of which DTO object is represented there until we've
     * figured out what MatsSocket endpoint is targeted - at which time we can deserialize it to the type that
     * MatsSocket Endpoint expects. On serialization, the message object is serialized separately from the Envelope -
     * and then plugged into the Envelope on the 'msg' field using a "direct JSON" technique.
     * <p />
     * Note: When instances of this the Envelope DTO is exposed via the MatsSocket API via listeners or the
     * {@link LiveMatsSocketSession#getLastEnvelopes() LiveMatsSocketSession.getLastEnvelopes()}, the contents of the
     * 'msg' will always be a String representing the JSON serialized message - it will NOT be an instance of a DTO
     * class. This ensures that the entire Envelope can be sent as a Mats or MatsSocket message, should that be of
     * interest. <b>Also, exposed Envelopes will be as instances of {@link MatsSocketEnvelopeWithMetaDto
     * MatsSocketEnvelopeWithMetaDto}!</b>.
     * <p />
     * Note: Copying out the source of {@link MatsSocketEnvelopeDto MatsSocketEnvelopeDto} PLUS
     * {@link MatsSocketEnvelopeWithMetaDto MatsSocketEnvelopeWithMetaDto} and including it in the source of another
     * service (or just importing the MatsSocket jar into that service), you can pass the entire Envelope as (part of) a
     * Mats DTO.
     *
     * @author Endre Stølsvik 2019-11-28 12:17 - http://stolsvik.com/, endre@stolsvik.com
     */
    class MatsSocketEnvelopeDto implements Cloneable {
        public MessageType t; // Type

        public String clv; // Client Lib and Versions, informative, e.g.
        // "MatsSockLibCsharp,v2.0.3; iOS,v13.2"
        // "MatsSockLibAlternativeJava,v12.3; ASDKAndroid,vKitKat.4.4"
        // Java lib: "MatsSockLibJava,v0.8.9; Java,v11.03:Windows,v2019"
        // browsers/JS: "MatsSocket.js,v0.8.9; User-Agent: <navigator.userAgent string>",
        public String an; // AppName
        public String av; // AppVersion

        public String auth; // Authorization header

        public String sid; // SessionId

        public Integer rd; // Requested debug info (currently only Client-to-Server)

        public String eid; // target MatsSocketEndpointId: Which MatsSocket Endpoint (server/client) this message is for

        public String tid; // TraceId
        public String smid; // ServerMessageId, messageId from Server.
        public String cmid; // ClientMessageId, messageId from Client.
        public List<String> ids; // Either multiple cmid or smid - used for ACKs/NACKs and ACK2s.
        public String x; // PingId - "correlationId" for pings. Small, so as to use little space.

        public Long to; // Timeout, in millis, for Client-to-Server Requests.

        public String desc; // Description when failure (NACK or others), exception message, multiline, may include
        // stacktrace if authz.

        public Object msg; // Message, JSON

        // ::: Debug info

        public DebugDto debug; // Debug info object - enabled if requested ('rd') and principal is allowed.

        @Override
        protected Object clone() {
            try {
                return super.clone();
            }
            catch (CloneNotSupportedException e) {
                throw new AssertionError("MatsSocketEnvelopeDto implements Cloneable, so why..?!", e);
            }
        }

        @Override
        public String toString() {
            return "{" + t
                    + (eid == null ? "" : "->" + eid)
                    + (tid == null ? "" : " tid:" + tid)
                    + (cmid == null ? "" : " cmid:" + cmid)
                    + (smid == null ? "" : " smid:" + smid)
                    + (msg == null
                            ? ""
                            : " msg:" + (msg instanceof String
                                    ? "String[" + ((String) msg).length() + "]"
                                    : msg.getClass().getSimpleName()))
                    + "}";
        }

        public static class DebugDto {
            public String d; // Description

            public int resd; // Resolved DebugOptions

            // :: Timings and Nodenames
            public Long cmrts; // Client Message Received Timestamp (when message was received on Server side, Server
            // timestamp)
            public String cmrnn; // Client Message Received on NodeName (and Mats message is also sent from this)

            public Long mmsts; // Mats Message Sent Timestamp (when the message was sent onto Mats MQ fabric, Server
            // timestamp)
            public Long mmrrts; // Mats Message Reply Received Timestamp (when the message was received from Mats,
            // Server ts)
            public String mmrrnn; // Mats Message Reply Received on NodeName

            public Long smcts; // Server Message Created Timestamp (when message was created on Server side)
            public String smcnn; // Server Message Created NodeName (when message was created on Server side)

            public Long mscts; // Message Sent to Client Timestamp (when the message was replied to Client side, Server
            // timestamp)
            public String mscnn; // Message Sent to Client from NodeName (typically same as cmrnn, unless reconnect in
            // between)

            // The log
            public List<LogLineDto> l; // Log - this will be appended to if debugging is active.
        }

        public static class LogLineDto {
            public long ts; // TimeStamp
            public String s; // System: "MatsSockets", "Mats", "MS SQL" or similar.
            public String hos; // Host OS, e.g. "iOS,v13.2", "Android,vKitKat.4.4", "Chrome,v123:Windows,vXP",
            // "Java,v11.03:Windows,v2019"
            public String an; // AppName
            public String av; // AppVersion
            public String t; // Thread name
            public int level; // 0=TRACE/ALL, 1=DEBUG, 2=INFO, 3=WARN, 4=ERROR
            public String m; // Message
            public String x; // Exception if any, null otherwise.
            public Map<String, String> mdc; // The MDC
        }
    }

    /**
     * Extension of {@link MatsSocketEnvelopeDto} which carries some metadata about the processing of the Envelope. When
     * MatsSocketEnvelopes are exposed from this API, it is via instances of this class. The class if fully serializable
     * via field serialization, and can directly be sent over Mats.
     * <p />
     * Note: When instance of this class is exposed through this API, there will only be a single instance created for
     * any {@link MessageEventListener} and also the {@link ActiveMatsSocketSession#getLastEnvelopes()}. This means that
     * you MUST NOT modify any fields on this single instance. However, this class implements {@link Cloneable} (shallow
     * clone), not throwing on {@link #clone()}. If you in a {@link MessageEventListener} want to modify the message,
     * you should clone it and modify and use the clone.
     *
     * @author Endre Stølsvik 2020-04-19 00:44 - http://stolsvik.com/, endre@stolsvik.com
     */
    class MatsSocketEnvelopeWithMetaDto extends MatsSocketEnvelopeDto implements Cloneable {
        public Direction dir; // Direction: C2S (message received on server) or S2C (message sent from server).
        public long ts; // Timestamp when envelope was received from the WebSocket, or successfully sent on the WS.
        public String nn; // NodeName of receiving or sending node.

        // ===== For all messages that are a reply:
        // -> WELCOME (HELLO), ACK + RESOLVE/REJECT (REQUEST), PONG (PING), ACK2 (ACK/NACK), SUB_OK/.. (SUB)

        public Long icts; // Incoming Timestamp
        public Double rttm; // Round Trip Time Millis. May be fractional.

        // ===== For Client-to-Server SEND / REQUEST

        public IncomingResolution ir; // IncomingResolution
        public String fmeid; // Forwarded to Mats Endpoint Id
        public Double rm; // Resolution Millis, fractional.

        // ===== For Server-to-Client SEND / REQUEST

        public Long ints; // Initiated Timestamp
        public String innn; // Initiated NodeName

        /**
         * <b>Only used for when the MatsSocketEnvelopeDto is exposed via the MatsSocket API - contains "meta-meta"
         * about the processing of the envelope</b>
         * <p />
         * Which direction the message travelled: Client-to-Server, or Server-to-Client.
         */
        public enum Direction {
            /**
             * Client-to-Server.
             */
            C2S,

            /**
             * Server-to-Client.
             */
            S2C
        }

        /**
         * <b>Only used for when the MatsSocketEnvelopeDto is exposed via the MatsSocket API - contains "meta-meta"
         * about the processing of the envelope</b>
         * <p />
         * How a Client-to-Server SEND or REQUEST was handled wrt. MatsSocket actions. For all these actions, the
         * incoming envelope is run through a piece of Java code - so it is possible that it might also have mined a
         * Bitcoin or something else entirely; These resolutions are in terms of response options for the
         * IncomingHandler.
         */
        public enum IncomingResolution {
            /**
             * The IncomingHandler raised an Exception. This results in NACK with the 'desc' field set, containing the
             * Exception class and message.
             */
            EXCEPTION,

            /**
             * IncomingHandler did no action wrt. MatsSocket options - this is allowed for SEND, but not for REQUEST.
             */
            NO_ACTION,

            /**
             * The IncomingHandler sent NACK ("Negative Acknowledgement"), signifying that it did not want to process
             * the message, typically because the authenticated user was not allowed to perform the requested action
             * (i.e. <i>authorization</i> of the action vs. the authenticated user (Principal) failed), and no further
             * action wrt. MatsSocket options.
             * <p />
             * {@link MatsSocketEndpointIncomingContext#deny()} was invoked.
             */
            DENY,

            /**
             * For a REQUEST: The IncomingHandler "insta-settled" the request with a Resolve.
             * <p />
             * {@link MatsSocketEndpointIncomingContext#resolve(Object)} was invoked.
             */
            RESOLVE,

            /**
             * For a REQUEST: The IncomingHandler "insta-settled" the request with a Reject.
             * <p />
             * {@link MatsSocketEndpointIncomingContext#reject(Object)} was invoked.
             */
            REJECT,

            /**
             * The IncomingHandler forwarded the SEND or REQUEST to a Mats Endpoint.
             * <p />
             * {@link MatsSocketEndpointIncomingContext#forwardCustom(Object, InitiateLambda)} or its ilk was invoked.
             */
            FORWARD,
        }
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
         * {@link #SUB} was not known to the server, implying that there are <i>at least one message</i> that has
         * expired, but we don't know whether it was 1 or 1 million - so you won't get <i>any</i> of them "replayed".
         * You must therefore "get up to speed" by other means, e.g. if the subscription in question is some kind of
         * cache delta-update mechanism, you will now have to reload the entire cache.
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
         * <li>From Server: {@link MatsSocketServer#closeSession(String, String)} was invoked, and the WebSocket to that
         * client was still open, so we close it.</li>
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
         * same MatsSocketSessionId as an existing open WebSocket connection. This could happen if the client has
         * realized that a connection is wonky and wants to ditch it, but the server has not realized the same yet. When
         * the server then gets the new connect, it'll see that there is an active WebSocket for this
         * MatsSocketSessionId already existing. This is probably a transitory situation, which would have resolved
         * itself after a while - but the MatsSocketServer will anyway close that existing connection to clean up - and
         * uses this close code. (Note that in the current test suite, this double-connection situation is emulated
         * using two different client instances using the same MatsSocketSessionId, something that shall never happen in
         * actual use. In this case the "original" client being closed with this close code must not start
         * reconnecting!)
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

    /**
     * Implementation of {@link MatsSocketSession}, which is serializable both for MatsSocket and Mats, i.e.
     * serialization mechanisms using field-based serialization.
     */
    class MatsSocketSessionDto implements MatsSocketSession {
        public String id;
        public String uid;
        public long scts;
        public long slts;
        public String clv;
        public String an;
        public String av;
        public String nn;

        @Override
        public String getMatsSocketSessionId() {
            return id;
        }

        @Override
        public String getUserId() {
            return uid;
        }

        @Override
        public Instant getSessionCreatedTimestamp() {
            return Instant.ofEpochMilli(scts);
        }

        @Override
        public Instant getSessionLivelinessTimestamp() {
            return Instant.ofEpochMilli(slts);
        }

        @Override
        public String getClientLibAndVersions() {
            return clv;
        }

        @Override
        public String getAppName() {
            return an;
        }

        @Override
        public String getAppVersion() {
            return av;
        }

        @Override
        public Optional<String> getNodeName() {
            return Optional.ofNullable(nn);
        }
    }

    /**
     * Implementation of {@link ActiveMatsSocketSession}, which is serializable both for MatsSocket and Mats, i.e.
     * serialization mechanisms using field-based serialization.
     */
    class ActiveMatsSocketSessionDto extends MatsSocketSessionDto implements ActiveMatsSocketSession {
        public String auth;
        public String pn;
        public String rip;
        public String ocrip;
        public SortedSet<String> subs;
        public long sets;
        public long lauthts;
        public long lcpts;
        public long lactts;
        public List<MatsSocketEnvelopeWithMetaDto> msgs;

        @Override
        public Optional<String> getAuthorization() {
            return Optional.ofNullable(auth);
        }

        @Override
        public Optional<String> getPrincipalName() {
            return Optional.ofNullable(pn);
        }

        @Override
        public Optional<String> getRemoteAddr() {
            return Optional.ofNullable(rip);
        }

        @Override
        public Optional<String> getOriginatingRemoteAddr() {
            return Optional.ofNullable(ocrip);
        }

        @Override
        public SortedSet<String> getTopicSubscriptions() {
            return subs;
        }

        @Override
        public Instant getSessionEstablishedTimestamp() {
            return Instant.ofEpochMilli(sets);
        }

        @Override
        public Instant getLastAuthenticatedTimestamp() {
            return Instant.ofEpochMilli(lauthts);
        }

        @Override
        public Instant getLastClientPingTimestamp() {
            return Instant.ofEpochMilli(lcpts);
        }

        @Override
        public Instant getLastActivityTimestamp() {
            return Instant.ofEpochMilli(lactts);
        }

        @Override
        public List<MatsSocketEnvelopeWithMetaDto> getLastEnvelopes() {
            return msgs;
        }
    }
}
