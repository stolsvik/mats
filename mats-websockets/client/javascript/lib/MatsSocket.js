// Register as an UMD module - source: https://github.com/umdjs/umd/blob/master/templates/commonjsStrict.js
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        // AMD. Register as an anonymous module.
        define(['exports', 'ws'], factory);
    } else if (typeof exports === 'object' && typeof exports.nodeName !== 'string') {
        // CommonJS
        factory(exports, require('ws'));
    } else {
        // Browser globals
        factory((root.mats = {}), root.WebSocket);
        // Also export these directly (without "mats." prefix)
        root.MatsSocket = root.mats.MatsSocket;

        root.MatsSocketCloseCodes = root.mats.MatsSocketCloseCodes;
        // Not exporting MessageType directly, as that is pretty internal.

        root.ConnectionState = root.mats.ConnectionState;

        root.AuthorizationRequiredEvent = root.mats.AuthorizationRequiredEvent;
        root.AuthorizationRequiredEventType = root.mats.AuthorizationRequiredEventType

        root.ConnectionEvent = root.mats.ConnectionEvent;
        root.ConnectionEventType = root.mats.ConnectionEventType;

        root.ReceivedEvent = root.mats.ReceivedEvent;
        root.ReceivedEventType = root.mats.ReceivedEventType;

        root.MessageEvent = root.mats.MessageEvent;
        root.MessageEventType = root.mats.MessageEventType;
    }
}(typeof self !== 'undefined' ? self : this, function (exports, WebSocket) {

    /**
     * <b>Copied directly from MatsSocketServer.java</b>: All WebSocket close codes used in the MatsSocket protocol.
     *
     * @type {Readonly<{VIOLATED_POLICY: number, SERVICE_RESTART: number, CLOSE_SESSION: number, GOING_AWAY: number, SESSION_LOST: number, RECONNECT: number, nameFor: nameFor, PROTOCOL_ERROR: number, UNEXPECTED_CONDITION: number}>}
     */
    const MatsSocketCloseCodes = Object.freeze({
        /**
         * Standard code 1002 - From Server side, Client should REJECT all outstanding and "crash"/reboot application:
         * used when the client does not observe the protocol.
         */
        PROTOCOL_ERROR: 1002,

        /**
         * Standard code 1008 - From Server side, Client should REJECT all outstanding and "crash"/reboot application:
         * used when the we cannot authenticate.
         */
        VIOLATED_POLICY: 1008,

        /**
         * Standard code 1011 - From Server side, Client should REJECT all outstanding and "crash"/reboot application.
         * This is the default close code if the MatsSocket "onMessage"-handler throws anything, and may also explicitly
         * be used by the implementation if it encounters a situation it cannot recover from.
         */
        UNEXPECTED_CONDITION: 1011,

        /**
         * Standard code 1012 - From Server side, Client should REISSUE all outstanding upon reconnect: used when
         * {@link MatsSocketServer#stop(int)} is invoked. Please reconnect.
         */
        SERVICE_RESTART: 1012,

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
         * considered a CLOSE_SESSION by the neither the client nor the server!</b> At least Jetty's implementation of
         * JSR 356 WebSocket API for Java sends GOING_AWAY upon socket close due to timeout. Since a timeout can happen
         * if we loose connection and thus can't convey PINGs, the MatsSocketServer must not interpret Jetty's
         * timeout-close as Close Session. Likewise, if the client just experienced massive lag on the connection, and
         * thus didn't get the PING over to the server in a timely fashion, but then suddenly gets Jetty's timeout close
         * with GOING_AWAY, this should not be interpreted by the client as the server wants to close the session.
         */
        GOING_AWAY: 1001,

        /**
         * 4000: Both from Server side and Client/Browser side, client should REJECT all outstanding:
         * <ul>
         * <li>From Browser: Used when the browser closes WebSocket "on purpose", wanting to close the session -
         * typically when the user explicitly logs out, or navigates away from web page. All traces of the
         * MatsSocketSession are effectively deleted from the server, including any undelivered replies and messages
         * ("push") from server.</li>
         * <li>From Server: {@link MatsSocketServer#closeSession(String)} was invoked, and the WebSocket to that client
         * was still open, so we close it.</li>
         * </ul>
         */
        CLOSE_SESSION: 4000,

        /**
         * 4001: From Server side, Client should REJECT all outstanding and "crash"/reboot application: A
         * HELLO:RECONNECT was attempted, but the session was gone. A considerable amount of time has probably gone by
         * since it last was connected. The client application must get its state synchronized with the server side's
         * view of the world, thus the suggestion of "reboot".
         */
        SESSION_LOST: 4001,

        /**
         * 4002: Both from Server side and from Client/Browser side: REISSUE all outstanding upon reconnect: From
         * Server: We ask that the client reconnects. This gets us a clean state and in particular new authentication
         * (In case of using OAuth/OIDC tokens, the client is expected to fetch a fresh token from token server). From
         * Client: The client just fancied a little break (just as if lost connection in a tunnel), used form
         * integration tests.
         */
        RECONNECT: 4002,

        /**
         * 4003: From Server side: Currently used in the specific situation where a MatsSocket client connects with
         * the same MatsSocketSessionId as an existing WebSocket connection. This could happen if the client has
         * realized that a connection is wonky and wants to ditch it, but the server has not realized the same yet.
         * When the server then gets the new connect, it'll see that there is an active WebSocket already. It needs
         * to close that. But the client "must not do anything" other than what it already is doing - reconnecting.
         */
        DISCONNECT: 4003,

        nameFor: function (closeCode) {
            let keys = Object.keys(MatsSocketCloseCodes).filter(function (key) {
                return MatsSocketCloseCodes[key] === closeCode;
            });
            if (keys.length === 1) {
                return keys[0];
            }
            return "UNKNOWN(" + closeCode + ")";
        }
    });

    /**
     * <b>Copied directly from MatsSocketServer.java</b>: All MessageTypes used in the wire-protocol of MatsSocket.
     *
     * @type {Readonly<{REJECT: string, REAUTH: string, HELLO: string, WELCOME: string, ACK: string, RETRY: string, SEND: string, REQUEST: string, AUTH: string, ACK2: string, PING: string, RESOLVE: string, PONG: string, NACK: string}>}
     */
    const MessageType = Object.freeze({
        /**
         * A HELLO message must be part of the first Pipeline of messages, preferably alone. One of the messages in the
         * first Pipeline must have the "auth" field set, and it might as well be the HELLO.
         */
        HELLO: "HELLO",

        /**
         * The reply to a {@link #HELLO}, where the MatsSocketSession is established, and the MatsSocketSessionId is
         * returned. If you included a MatsSocketSessionId in the HELLO, signifying that you want to reconnect to an
         * existing session, and you actually get a WELCOME back, it will be the same as what you provided - otherwise
         * the connection is closed with {@link MatsSocketCloseCodes#SESSION_LOST}.
         */
        WELCOME: "WELCOME",

        /**
         * The sender sends a "fire and forget" style message.
         */
        SEND: "SEND",

        /**
         * The sender initiates a request, to which a {@link #RESOLVE} or {@link #REJECT} message is expected.
         */
        REQUEST: "REQUEST",

        /**
         * The sender should retry the message (the receiver could not handle it right now, but a Retry might fix it).
         */
        RETRY: "RETRY",

        /**
         * The message was Received, and acknowledged positively - i.e. it has acted on it.
         * <p/>
         * The sender has now taken over responsibility of this message, put it (at least the reference ClientMessageId)
         * in its Inbox, and possibly acted on it. The reason for the Inbox is so that if it Receives the message again,
         * it may just insta-ACK/NACK it and toss this copy out the window (since it has already handled it).
         * <p/>
         * When the receive gets this, it may safely delete the message from its Outbox.
         */
        ACK: "ACK",

        /**
         * The message was Received, but it did not acknowledge it - i.e. it has not acted on it.
         * <p/>
         * The sender has now taken over responsibility of this message, put it (at least the reference ClientMessageId)
         * in its Inbox, and possibly acted on it. The reason for the Inbox is so that if it Receives the message again,
         * it may just insta-ACK/NACK it and toss this copy out the window (since it has already handled it).
         * <p/>
         * When the receive gets this, it may safely delete the message from its Outbox.
         */
        NACK: "NACK",

        /**
         * A RESOLVE-reply to a previous {@link #REQUEST} - if the Client did the {@code REQUEST}, the Server will
         * answer with either a RESOLVE or {@link #REJECT}.
         */
        RESOLVE: "RESOLVE",

        /**
         * A REJECT-reply to a previous {@link #REQUEST} - if the Client did the {@code REQUEST}, the Server will answer
         * with either a REJECT or {@link #RESOLVE}.
         */
        REJECT: "REJECT",

        /**
         * An "Acknowledge ^ 2", i.e. an acknowledge of the {@link #ACK} or {@link #NACK}. When the receiver gets this,
         * it may safely delete the entry it has for this message from its Inbox.
         */
        ACK2: "ACK2",

        /**
         * The server requests that the client re-authenticates, where the client should immediately get a fresh
         * authentication and back using either any message it has pending, or in a separate {@link #AUTH} message.
         */
        REAUTH: "REAUTH",

        /**
         * From Client: The client can use a separate AUTH message to send over the requested {@link #REAUTH} (it could
         * just as well put the 'auth' in a PING or any other message it had pending).
         */
        AUTH: "AUTH",

        /**
         * A PING, to which a {@link #PONG} is expected.
         */
        PING: "PING",

        /**
         * A Reply to a {@link #PING}.
         */
        PONG: "PONG"
    });

    /**
     * States for MatsSocket's {@link MatsSocket#state state}.
     *
     * @type {Readonly<{NO_SESSION: string, CONNECTING: string, WAITING: string, CONNECTED: string, SESSION_ESTABLISHED: string}>}
     */
    const ConnectionState = Object.freeze({
        /**
         * State only - this is the initial State of a MatsSocket. Also, the MatsSocket is re-set back to this State in a
         * Session-Closed-from-Server situation (which is communicated via listeners registered with
         * {@link MatsSocket#addSessionClosedEventListener(listener)}), OR if you have explicitly performed a
         * matsSocket.close().
         * <p/>
         * Only transition out of this state is into {@link #CONNECTING}.
         */
        NO_SESSION: "nosession",

        /**
         * State, and fires as ConnectionEvent when we transition into this state, which is when the WebSocket is literally trying to connect.
         * This is between <code>new WebSocket(url)</code> and either webSocket.onopen or webSocket.onclose is
         * fired, or countdown reaches 0. If webSocket.onopen, we transition into {@link #CONNECTED}, if webSocket.onclose, we transition into
         * {@link #WAITING}. If we reach countdown 0 while in CONNECTING, we will "re-transition" to the same state, and
         * thus get one more event of CONNECTING.
         * <p/>
         * User Info Tip: Show a info-box, stating "Connecting..", and you may show the countdown number in "grayed out".
         * Each time it transitions into CONNECTING, it will start a new countdown. Let's say it starts from say 4
         * seconds: If this connection attempt fails after 1 second, it will transition into WAITING and continue the
         * countdown with 3 seconds remaining.
         * <p/>
         * User Info Tip: Show a info-box, stating "Connecting! <4 seconds..>", countdown in "grayed out" style, box is
         * some neutral information color, e.g. yellow (fading over to this color if already red or orange due to
         * {@link #CONNECTION_ERROR} or {@link #LOST_CONNECTION}).
         */
        CONNECTING: "connecting",

        /**
         * State, and fires as ConnectionEvent when we transition into this state, which is when {@link #CONNECTING} fails.
         * The only transition out of this state is {@link #CONNECTING}, when the {@link #COUNTDOWN} reaches 0.
         * <p/>
         * Notice that the {@link ConnectionEvent} contains the {@link Event} that came with webSocket.close (while CONNECTING).
         * <p/>
         * User Info Tip: Show a info-box, stating "Waiting for next attempt..", and you may show the countdown number with proper visibility. It will come into this state from {@link #CONNECTING},
         * and have the time remaining from the initial countdown. So if the attempt countdown started from 4 seconds,
         * and it took 1 second before the connection attempt failed, then there will be 3 seconds left in WAITING state.
         * <p/>
         * User Info Tip: Show a info-box, stating "Waiting! <2.9 seconds..>", countdown in normal visibility, box is
         * some neutral information color, e.g. yellow (keeping the box color fading if in progress).
         */
        WAITING: "waiting",

        /**
         * State, and fires as ConnectionEvent when we transition into this state, which is when WebSocket.onopen is fired.
         * Notice that the MatsSocket is still not fully established, as we have not yet exchanged HELLO and WELCOME -
         * the MatsSocket is fully established at {@link #SESSION_ESTABLISHED}.
         * <p/>
         * Notice that the {@link ConnectionEvent} contains the WebSocket 'onopen' {@link Event} that was issued when
         * the WebSocket opened.
         * <p/>
         * User Info Tip: Show a info-box, stating "Connected!", happy-color, e.g. green, with no countdown.
         */
        CONNECTED: "connected",

        /**
         * State, and fires as ConnectionEvent when we transition into this state, which is when when the WELCOME MatsSocket message comes
         * from the Server, also implying that it has been authenticated: The MatsSocket is now fully established, and
         * actual messages can be exchanged.
         * <p/>
         * User Info Tip: Show a info-box, stating "Session OK!", happy-color, e.g. green, with no countdown - and the
         * entire info-box fades away fast, e.g. after 1 second.
         */
        SESSION_ESTABLISHED: "sessionestablished"
    });

    /**
     * The event types of {@link ConnectionEvent} - four of the event types are state-transitions into different states
     * of {@link ConnectionState}.
     *
     * @type {Readonly<{COUNTDOWN: string, CONNECTING: string, WAITING: string, CONNECTION_ERROR: string, CONNECTED: string, SESSION_ESTABLISHED: string, LOST_CONNECTION: string}>}
     */
    const ConnectionEventType = Object.freeze({
        /**
         * Read doc at {@link ConnectionState#CONNECTING}.
         */
        CONNECTING: ConnectionState.CONNECTING,

        /**
         * Read doc at {@link ConnectionState#WAITING}.
         */
        WAITING: ConnectionState.WAITING,

        /**
         * Read doc at {@link ConnectionState#CONNECTED}.
         */
        CONNECTED: ConnectionState.CONNECTED,

        /**
         * Read doc at {@link ConnectionState#SESSION_ESTABLISHED}.
         */
        SESSION_ESTABLISHED: ConnectionState.SESSION_ESTABLISHED,

        /**
         * This is a pretty worthless event. It comes from WebSocket.onerror. It will <i>always</i> be trailed by a
         * WebSocket.onclose, which gives the event {@link #LOST_CONNECTION}.
         * <p/>
         * Notice that the {@link ConnectionEvent} contains the {@link Event} that caused the error.
         * <p/>
         * User Info Tip: Show a info-box, which is some reddish color (no need for text since next event {@link #LOST_CONNECTION}) comes immediately).
         */
        CONNECTION_ERROR: "connectionerror",

        /**
         * This comes when WebSocket.onclose is fired "unexpectedly", <b>and the reason for this close is NOT a SessionClosed Event</b>. The latter will
         * instead invoke the listeners registered with {@link MatsSocket#addSessionClosedEventListener(listener)}.
         * A LOST_CONNECTION will start a reconnection attempt after a very brief delay (couple of hundred milliseconds), and the next state transition and thus event is {@link #CONNECTING}.
         * <p/>
         * Notice that the {@link ConnectionEvent} contains the {@link CloseEvent} that caused the lost connection.
         * <p/>
         * User Info Tip: Show a info-box, stating "Connection broken!", which is some orange color (unless it already is red due to {@link #CONNECTION_ERROR}), fading over to the next color when next event ({@link #CONNECTING} comes in.
         */
        LOST_CONNECTION: "lostconnection",

        /**
         * Notice that you will most probably not get an event with 0 seconds left, as that is when we (re-)transition to
         * {@link #CONNECTING} and the countdown starts over (possibly with a larger timeout). Read more at {@link ConnectionEvent#countdownSeconds}.
         * <p/>
         * User Info Tip: Read more at {@link #CONNECTING} and {@linl #WAITING}.
         */
        COUNTDOWN: "countdown"

    });

    /**
     * Event object for {@link MatsSocket#addConnectionEventListener(function)}.
     *
     * @param {string} type
     * @param {string} webSocketUrl
     * @param {Event} webSocketEvent
     * @param {number} timeoutSeconds
     * @param {number} countdownSeconds
     * @constructor
     */
    function ConnectionEvent(type, webSocketUrl, webSocketEvent, timeoutSeconds, countdownSeconds) {
        /**
         * Return the enum value of {@link ConnectionEventType} - also available as the enum <i>key</i> string on the
         * property {@link ConnectionEvent#eventName}.
         *
         * @type {string}
         */
        this.type = type;

        /**
         * For all of the events this holds the current URL we're either connected to, was connected to, or trying to
         * connect to.
         *
         * @type {string}
         */
        this.webSocketUrl = webSocketUrl;

        /**
         * For several of the events (enumerated in {@link ConnectionEventType}), there is an underlying WebSocket event
         * that caused it. This field holds that.
         * <ul>
         *     <li>{@link ConnectionEventType#WAITING}: WebSocket {@link CloseEvent} that caused this transition.</li>
         *     <li>{@link ConnectionEventType#CONNECTED}: WebSocket {@link Event} that caused this transition.</li>
         *     <li>{@link ConnectionEventType#CONNECTION_ERROR}: WebSocket {@link Event} that caused this transition.</li>
         *     <li>{@link ConnectionEventType#LOST_CONNECTION}: WebSocket {@link CloseEvent} that caused it.</li>
         * </ul>
         *
         * @type {Event}
         */
        this.webSocketEvent = webSocketEvent;

        /**
         * For {@link ConnectionEventType#CONNECTING}, {@link ConnectionEventType#WAITING} and {@link ConnectionEventType#COUNTDOWN},
         * tells how long the timeout for this attempt is, i.e. what the COUNTDOWN events start out with. Together with
         * {@link #countdownSeconds} of the COUNTDOWN events, this can be used to calculate a fraction if you want to
         * make a "progress bar" of sorts.
         * <p/>
         * The timeouts starts at 500 ms (unless there is only 1 URL configured, in which case 5 seconds), and then
         * increases exponentially, but maxes out at 15 seconds.
         *
         * @type {number}
         */
        this.timeoutSeconds = timeoutSeconds;

        /**
         * For {@link ConnectionEventType#CONNECTING}, {@link ConnectionEventType#WAITING} and {@link ConnectionEventType#COUNTDOWN},
         * tells how many seconds there are left for this attempt (of the {@link #timeoutSeconds} it started with),
         * with a tenth of a second as precision. With the COUNTDOWN events, these come in each 100 ms (1/10 second),
         * and show how long time there is left before trying again (if MatsSocket is configured with multiple URLs,
         * the next attempt will be a different URL).
         * <p/>
         * The countdown is started when the state transitions to {@link ConnectionEventType#CONNECTING}, and
         * stops either when {@link ConnectionEventType#CONNECTED} or the timeout reaches zero. If the
         * state is still CONNECTING when the countdown reaches zero, implying that the "new WebSocket(..)" call still
         * has not either opened or closed, the connection attempt is aborted by calling webSocket.close(). It then
         * tries again, possibly with a different URL - and the countdown starts over.
         * <p/>
         * Notice that the countdown is not affected by any state transition into {@link ConnectionEventType#WAITING} -
         * such transition only means that the "new WebSocket(..)" call failed and emitted a close-event, but we will
         * still wait out the countdown before trying again.
         * <p/>
         * Notice that you will most probably not get an event with 0 seconds, as that is when we transition into
         * {@link #CONNECTING} and the countdown starts over (possibly with a larger timeout).
         * <p/>
         * Truncated exponential backoff: The timeouts starts at 500 ms (unless there is only 1 URL configured, in which
         * case 5 seconds), and then increases exponentially, but maxes out at 15 seconds.
         *
         * @type {number}
         */
        this.countdownSeconds = countdownSeconds;
    }

    /**
     * TODO
     *
     * @member {string} codeName
     * @memberOf ConnectionEvent
     * @readonly
     */
    Object.defineProperty(ConnectionEvent.prototype, "eventName", {
        get: function () {
            let keys = Object.keys(ConnectionEventType).filter(function (key) {
                return ConnectionState[key] === this.type;
            });
            if (keys.length === 1) {
                return keys[0];
            }
            return "UNKNOWN(" + this.type + ")";
        }
    });

    /**
     * Hack the built-in CloseEvent to include a "codeName" property resolving to the MatsSocket-relevant close codes.
     * Return the name of the CloseCode, e.g. "SESSION_LOST". Relevant for quick human introspection (i.e. debugger
     * or logging) - programmatically you should use the numeric 'code' property, possibly comparing to the values of
     * the {@link MatsSocketCloseCodes} enum keys, e.g. "<code>if (closeEvent.code ===
     * MatsSocketCloseCodes.SESSION_LOST) {..}</code>".
     *
     * @member {string} codeName
     * @memberOf CloseEvent
     * @readonly
     */
    if (typeof CloseEvent !== 'undefined') {
        // ^ guard against Node.js, which evidently does not have CloseEvent..
        Object.defineProperty(CloseEvent.prototype, "codeName", {
            get: function () {
                return MatsSocketCloseCodes.nameFor(this.code);
            }
        });
    }

    /**
     * Message Received on Server event: "acknowledge" or "negative acknowledge" - these are the events which the
     * returned Promise of a send(..) is settled with (i.e. then() and catch()), and to a {@link MatsSocket#request request}'s receivedCallback function.
     */
    function ReceivedEvent(type, traceId, sentTimestamp, receivedTimestamp, roundTripMillis) {
        /**
         * Values are from {@link ReceivedEventType}: Type of received event, either {@link ReceivedEventType#ACK "ack"},
         * {@link ReceivedEventType#NACK "nack"} - <b>or {@link ReceivedEventType#SESSION_CLOSED "sessionclosed"} if the
         * session was closed with outstanding initiations and MatsSocket therefore "clears out" these initiations.</b>
         *s
         * @type {string}
         */
        this.type = type;

        /**
         * TraceId for this call / message.
         *
         * @type {string}
         */
        this.traceId = traceId;

        /**
         * Millis-since-epoch when the message was sent from the Client.
         *
         * @type {number}
         */
        this.sentTimestamp = sentTimestamp;

        /**
         * Millis-since-epoch when the ACK or NACK was received on the Client, millis-since-epoch.
         *
         * @type {number}
         */
        this.receivedTimestamp = receivedTimestamp;

        /**
         * Round-trip time in milliseconds, basically <code>{@link #receivedTimestamp} -
         * {@link #sentTimestamp}</code>, but depending on the browser/runtime, you might get higher resolution
         * than integer milliseconds (i.e. fractions of milliseconds, a floating point number) - it depends on
         * the resolution of <code>performance.now()</code>.
         * <p/>
         * Notice that Received-events might be de-prioritized on the Server side (batched up, with micro-delays
         * to get multiple into the same batch), so this number should not be taken as the "ping time".
         *
         * @type {number}
         */
        this.roundTripMillis = roundTripMillis;
    }

    const ReceivedEventType = Object.freeze({
        /**
         * If the Server-side MatsSocketEndpoint/Terminator accepted the message for handling (and if relevant,
         * forwarded it to the Mats fabric). The returned Promise of send() is <i>resolved</i> with this type of event.
         * The 'receivedCallback' of a request() will get both "ack" and {@link #NACK "nack"}, thus must check on
         * the type if it makes a difference.
         */
        ACK: "ack",

        /**
         * If the Server-side MatsSocketEndpoint/Terminator dit NOT accept the message, either explicitly with
         * context.deny(), or by failing with Exception. The returned Promise of send() is <i>rejected</i> with this
         * type of event. The 'receivedCallback' of a request() will get both "nack" and {@link #ACK "ack"}, thus must
         * check on the type if it makes a difference.
         * <p/>
         * Notice that a for a Client-initiated Request which is insta-rejected in the incomingHandler by invocation of
         * context.reject(..), this implies <i>acknowledge</i> of the <i>reception</i> of the message, but <i>reject</i>
         * as with regard to the </i>reply</i> (the Promise returned from request(..)).
         */
        NACK: "nack",

        /**
         * "Synthetic" event in that it only happens if the MatsSocketSession is closed with outstanding Initiations
         * not yet Received on Server.
         */
        SESSION_CLOSED: "sessionclosed"
    });

    /**
     * Message Event - the event emitted for a Requests's Promise resolve() and reject() (i.e. then() and catch()), and
     * to a {@link MatsSocket#terminator terminator} resolveCallback and rejectCallback functions for replies due to
     * requestReplyTo, and for Server initiated sends, and for the event to a {@link MatsSocket#endpoint endpoint} upon
     * a Server initiated request.
     */
    function MessageEvent(type, data, traceId, messageId, receivedTimestamp) {
        /**
         * Values are from {@link MessageEventType}: Either {@link MessageEventType#SEND "send"} (for a Client
         * Terminator when targeted for a Server initiated Send); {@link MessageEventType#REQUEST "request"} (for a
         * Client Endpoint when targeted for a Server initiated Request); or {@link MessageEventType#RESOLVE "resolve"}
         * or {@link MessageEventType#REJECT "reject"} (for settling of Promise from a Client-initiated Request, and
         * for a Client Terminator when targeted as the reply-endpoint for a Client initiated Request) - <b>or
         * {@link MessageEventType#SESSION_CLOSED "sessionclosed"} if the session was closed with outstanding Requests
         * and MatsSocket therefore "clears out" these Requests.</b>
         * <p/>
         * Notice: In the face of {@link MessageType#SESSION_CLOSED "sessionclosed"}, the {@link #data} property (i.e.
         * the actual message from the server) will be <code>undefined</code>. This is <i>by definition</i>: The
         * Request was outstanding, meaning that an answer from the Server had yet to come. This is opposed to a normal
         * REJECT settling from the Server-side MatsSocketEndpoint, which may choose to include data with a rejection.
         *
         * @type {string}
         */
        this.type = type;

        /**
         * The actual data from the other peer.
         *
         * @type {object}
         */
        this.data = data;

        /**
         * When a Terminator gets invoked to handle a Reply due to a Client initiated {@link MatsSocket#requestReplyTo},
         * this holds the 'correlationInformation' object that was supplied in the requestReplyTo(..) invocation.
         *
         * @type {object}
         */
        this.correlationInformation = undefined;

        /**
         * The TraceId for this call / message.
         *
         * @type {string}
         */
        this.traceId = traceId;

        /**
         * Either the ClientMessageId if this message is a Reply to a Client-initiated Request (i.e. this message is a
         * RESOLVE or REJECT), or ServerMessageId if this originated from the Server (i.e. SEND or REQUEST);
         *
         * @type {string}
         */
        this.messageId = messageId;

        /**
         * millis-since-epoch when the Request, for which this message is a Reply, was sent from the
         * Client. If this message is not a Reply to a Client-initiated Request, it is undefined.
         *
         * @type {number}
         */
        this.clientRequestTimestamp = undefined;

        /**
         * When the message was received on the Client, millis-since-epoch.
         *
         * @type {number}
         */
        this.receivedTimestamp = receivedTimestamp;

        /**
         * Round-trip time in milliseconds, basically <code>{@link #receivedTimestamp} -
         * {@link #clientRequestTimestamp}</code>, but depending on the browser/runtime, you might get higher resolution
         * than integer milliseconds (i.e. fractions of milliseconds, a floating point number) - it depends on
         * the resolution of <code>performance.now()</code>.
         *
         * @type {number}
         *
         */
        this.roundTripMillis = undefined;

        /**
         * An instance of {@link DebugInformation}, potentially containing interesting meta-information about the
         * call.
         */
        this.debug = undefined;
    }

    const MessageEventType = Object.freeze({
        RESOLVE: "resolve",

        REJECT: "reject",

        SEND: "send",

        REQUEST: "request",

        /**
         * "Synthetic" event in that it only happens if the MatsSocketSession is closed with outstanding Initiations
         * not yet Received on Server.
         */
        SESSION_CLOSED: "sessionclosed"
    });

    /**
     * Meta-information for the call, availability depends on the allowed debug options for the authenticated user,
     * and which information is requested in client.
     *
     * @constructor
     */
    function DebugInformation(sentTimestamp, envelope, receivedTimestamp) {
        this.clientMessageSent = sentTimestamp;

        if (envelope.debug) {
            this.clientMessageReceived = envelope.debug.cmrts;
            this.clientMessageReceivedNodename = envelope.debug.cmrnn;

            this.matsMessageSent = envelope.debug.mmsts;
            this.matsMessageReplyReceived = envelope.debug.mmrrts;
            this.matsMessageReplyReceivedNodename = envelope.debug.mmrrnn;

            this.messageSentToClient = envelope.debug.mscts;
            this.messageSentToClientNodename = envelope.debug.mscnn;
        }

        this.messageReceived = receivedTimestamp;
    }

    /**
     * @param {string} type type of the event, one of {@link AuthorizationRequiredEvent}.
     * @param {number} currentExpirationTimestamp millis-from-epoch when the current Authorization expires.
     * @constructor
     */
    function AuthorizationRequiredEvent(type, currentExpirationTimestamp) {
        /**
         * Type of the event, one of {@link AuthorizationRequiredEvent}.
         *
         * @type {string} the correlationId used by the client lib to correlate outstanding pings with incoming pongs.
         */
        this.type = type;

        /**
         * Millis-from-epoch when the current Authorization expires - note that this might well still be in the future,
         * but the "slack" left before expiration is not long enough.
         *
         * @type {number}
         */
        this.currentExpirationTimestamp = currentExpirationTimestamp;
    }

    /**
     * Type of {@link AuthorizationRequiredEvent}.
     *
     * @type {Readonly<{NOT_PRESENT: string, SERVER_REQUEST: string, EXPIRED: string}>}
     */
    const AuthorizationRequiredEventType = Object.freeze({
        /**
         * Initial state, if auth not already set by app.
         */
        NOT_PRESENT: "notpresent",

        /**
         * The authentication is expired - note that this might well still be in the future,
         * but the "slack" left before expiration is not long enough.
         */
        EXPIRED: "expired",

        /**
         * The server has requested that the app provides fresh auth to proceed - this needs to be fully fresh, even
         * though there might still be "slack" enough left on the current authorization to proceed. (The server side
         * might want the full expiry to proceed, or wants to ensure that the app can still produce new auth - i.e.
         * it might suspect that the current authentication session has been invalidated, and need proof that the app
         * can still produce new authorizations/tokens).
         */
        REAUTHENTICATE: "reauthenticate"
    });


    /**
     * Stats: A "holding struct" for pings - you may get the latest pings (with experienced round-trip times) from the
     * property {@link MatsSocket#pings}.
     *
     * @param {string} correlationId
     * @param {number} sentTimestamp
     * @constructor
     */
    function PingPong(correlationId, sentTimestamp) {
        /**
         * Pretty meaningless information, but if logged server side, you can possibly correlate outliers.
         *
         * @type {string}
         */
        this.correlationId = correlationId;

        /**
         * Millis-from-epoch when this ping was sent.
         *
         * @type {number}
         */
        this.sentTimestamp = sentTimestamp;

        /**
         * The experienced round-trip time for this ping-pong - this is the time back-and-forth.
         *
         * @type {number}
         */
        this.roundTripMillis = undefined;
    }

    /**
     * Creates a MatsSocket, supplying the using Application's name and version, and which URLs to connect to.
     *
     * Note: Public, Private and Privileged modelled after http://crockford.com/javascript/private.html
     *
     * @param {string} appName the name of the application using this MatsSocket.js client library
     * @param {string} appVersion the version of the application using this MatsSocket.js client library
     * @param {array} urls an array of WebSocket URLs speaking 'matssocket' protocol, or a single string URL.
     * @param {function} socketFactory how to make WebSockets, optional in browser setting as it will use window.WebSocket.
     * @constructor
     */
    function MatsSocket(appName, appVersion, urls, socketFactory) {
        let clientLibNameAndVersion = "MatsSocket.js,v0.10.0";

        // :: Validate primary arguments
        if (typeof appName !== "string") {
            throw new Error("appName must be a string, was: [" + appName + "]");
        }
        if (typeof appVersion !== "string") {
            throw new Error("appVersion must be a string, was: [" + appVersion + "]");
        }
        // 'urls' must either be a string, String, or an Array that is not 0 elements.
        let urlsOk = ((typeof urls === 'string') || (urls instanceof String)) || (Array.isArray(urls) && urls.length > 0);
        if (!urlsOk) {
            throw new Error("urls must have at least 1 url set, got: [" + urls + "]");
        }

        // :: Provide default for socket factory if not defined.
        if (socketFactory === undefined) {
            socketFactory = function (url, protocol) {
                return new WebSocket(url, protocol);
            };
        }
        if (typeof socketFactory !== "function") {
            throw new Error("socketFactory should be a function, instead got [" + socketFactory + "]");
        }

        // Polyfill performance.now() for Node.js
        let performance = ((typeof (window) === "object" && window.performance) || {
            now: function now() {
                return Date.now();
            }
        });

        const that = this;
        const userAgent = (typeof (self) === 'object' && typeof (self.navigator) === 'object') ? self.navigator.userAgent : "Unknown";

        // Ensure that 'urls' is an array or 1 or several URLs.
        urls = [].concat(urls);


        // ==============================================================================================
        // PUBLIC:
        // ==============================================================================================

        // NOTE!! There is an "implicit"/"hidden" 'sessionId' field too, but we do not make it explicit.
        // 'sessionId' is set when we get the SessionId from WELCOME, cleared (deleted) upon SessionClose (along with _sessionOpened = false)

        /**
         * Whether to log via console.log. The logging is quite extensive.
         *
         * @type {boolean}
         */
        this.logging = false;

        /**
         * If this is set to a function, it will be invoked if close(..) is invoked and sessionId is present, the
         * single parameter being a object with two keys:
         * <ol>
         *     <li>'currentWsUrl' is the current WebSocket url (the one that the WebSocket was last connected to, and
         *       would have connected to again, e.g. "wss://example.com/matssocket").</li>
         *     <li>'sessionId' is the current MatsSocket SessionId</li>
         * </ol>
         * The default is 'undefined', which effectively results in the invocation of
         * <code>navigator.sendBeacon(currentWsUrl.replace('ws', 'http)+"/close_session?sessionId={sessionId}")<code>.
         * Note that replace is replace-first, and that the extra 's' in 'wss' results in 'https'.
         *
         * @type {Function}
         */
        this.outofbandclose = undefined;

        /**
         * A bit field requesting different types of debug information - it is read each time an information bearing
         * message is added to the pipeline, and if not 'unhandled' or 0, the debug options flags is added to the
         * message.
         * <p/>
         * The debug options on the outgoing Client-to-Server requests are tied to that particular request flow -
         * but to facilitate debug information also on Server-initiated messages, the <i>last set</i> debug options is
         * also stored on the server and used when messages originate there (i.e. Server-to-Client SENDs and REQUESTs).
         * <p/>
         * If you only want debug information on a particular Client-to-Server request, you'll need to first set the
         * debug flags, then do the request (adding the message to the pipeline), and then reset the debug flags back.
         * <p/>
         * The value is a bit field, so you bitwise-or (or add) together the different things you want. Undefined, or 0,
         * means "no debug info".
         * <p/>
         * The value from the client is bitwise-and'ed together with the debug capabilities the authenticated user has
         * gotten by the AuthenticationPlugin on the Server side.
         *
         * @type {number}
         */
        this.debug = undefined;

        /**
         * <b>Note: You <i>should</i> register a SessionClosedEvent listener, as any invocation of this listener by this
         * client library means that you've lost sync with the server, and you should crash or "reboot" the application
         * employing the library to regain sync.</b>
         * <p/>
         * The registered event listener functions are called when the Server kicks us off the socket and the session is
         * closed due to a multitude of reasons, where most should never happen if you use the library correctly, in
         * particular wrt. authentication. <b>It is NOT invoked when you explicitly invoke matsSocket.close() from
         * the client yourself!</b>
         * <p/>
         * The event object is the forwarded WebSocket CloseEvent, albeit it is prototype-extended to include the
         * property 'codeName' (in addition to the existing 'code') for the relevant {@link MatsSocketCloseCodes}.
         * You can use the 'code' to "enum-compare" to {@code MatsSocketCloseCodes}, the enum keys are listed here:
         * <ul>
         *   <li>UNEXPECTED_CONDITION: Error on the Server side, typically that the data store (DB) was unavailable,
         *   and the MatsSocketServer could not reliably recover the processing of your message.</li>
         *   <li>PROTOCOL_ERROR: This client library has a bug!</li>
         *   <li>VIOLATED_POLICY: Authorization was wrong. Always supply a correct and non-expired Authorization header,
         *   which has sufficient 'roomForLatency' wrt. the expiry time.</li>
         *   <li>CLOSE_SESSION: <code>MatsSocketServer.closeSession(sessionId)</code> was invoked Server side for this
         *   MatsSocketSession</li>
         *   <li>SESSION_LOST: A reconnect attempt was performed, but the MatsSocketSession was timed out on the Server.
         *   The Session will never time out if the WebSocket connection is open. Only if the Client has lost connection,
         *   the timer will start. The Session timeout is measured in hours or days. This could conceivably happen
         *   if you close the lid of a laptop, and open it again days later - but one would think that the
         *   Authentication session (the one giving you Authorization headers) had timed out long before.</li>
         * </ul>
         * No such error should happen if this client is used properly (and the server does not get problems with
         * its data store) - the only conceivable situation is that a pipeline of envelopes (MatsSocket messages) took
         * more time to send over to the server than there was left before the authorization expired (i.e.
         * 'roomForLatency' is set too small, compared to the bandwidth available for sending the messages residing in
         * the current pipeline).
         * <p/>
         * Note that when this event listener is invoked, the MatsSocket session is just as closed as if you invoked
         * {@link MatsSocket#close()} on it: All outstanding send/requests are rejected, all request Promises are
         * rejected, and the MatsSocket object is as if just constructed and configured. You may "boot it up again" by
         * sending a new message where you then will get a new MatsSocket Session. However, you should consider
         * restarting the application if this happens, or otherwise "reboot" it as if it just started up (gather all
         * required state and null out any other that uses lazy fetching). Realize that any outstanding "addOrder"
         * request's Promise will now have been rejected - and you don't really know whether the order was placed or
         * not, so you should get the entire order list. On the received event, there will be a number detailing the
         * number of outstanding send/requests and Promises that was rejected: If this is zero, you should actually be
         * in sync, and can consider just "act as if nothing happened" - by sending a new message and thus get a new
         * MatsSocket Session going.
         *
         * @param {function} sessionClosedEventListener a function that is invoked when the library gets the current
         * MatsSocketSession closed from the server. The event object is the WebSocket's {@link CloseEvent}.
         */
        this.addSessionClosedEventListener = function (sessionClosedEventListener) {
            if (!(typeof sessionClosedEventListener === 'function')) {
                throw Error("SessionClosedEvent listener must be a function");
            }
            _sessionClosedEventListeners.push(sessionClosedEventListener);
        };

        /**
         * <b>Note: You <i>could</i> register a ConnectionEvent listener, as these are only informational messages
         * about the state of the Connection.</b> It is nice if the user gets a small notification about <i>"Connection
         * Lost, trying to reconnect in 2 seconds"</i> to keep him in the loop of why the application's data fetching
         * seems to be lagging.
         * <p/>
         * The registered event listener functions are called when this client library performs WebSocket connection
         * operations, including connection closed events that are not "Session Close" style. This includes the simple
         * situation of "lost connection, reconnecting" because you passed through an area with limited or no
         * connectivity.
         * <p/>
         * Read more at {@link ConnectionEvent} and {@link ConnectionState}.
         *
         * @param {function} connectionEventListener a function that is invoked when the library issues
         * {@link ConnectionEvent}s.
         */
        this.addConnectionEventListener = function (connectionEventListener) {
            if (!(typeof connectionEventListener === 'function')) {
                throw Error("SessionClosedEvent listener must be a function");
            }
            _connectionEventListeners.push(connectionEventListener);
        };

        /**
         * If this MatsSockets client realizes that the expiration time (less the room for latency) of the authorization
         * has passed when about to send a message, it will invoke this callback function. A new authorization must then
         * be provided by invoking the 'setCurrentAuthorization' function - only when this is invoked, the MatsSocket
         * will send messages. The MatsSocket will stack up any messages that are sent while waiting for new
         * authorization, and send them all at once when the authorization is in (i.e. it'll pipeline the messages).
         *
         * @param {function} authorizationExpiredCallback function which will be invoked if the current time is more
         * than 'expirationTimeMillisSinceEpoch - roomForLatencyMillis' of the last invocation of
         * 'setCurrentAuthorization' when about to send a new message.
         */
        this.setAuthorizationExpiredCallback = function (authorizationExpiredCallback) {
            _authorizationExpiredCallback = authorizationExpiredCallback;

            // Evaluate whether there are stuff in the pipeline that should be sent now.
            // (Not-yet-sent HELLO does not count..)
            _evaluatePipelineSend();
        };

        /**
         * Sets an authorization String, which for several types of authorization must be invoked on a regular basis with
         * fresh authorization - this holds for a OAuth/OIDC-type system where an access token will expire within a short time
         * frame (e.g. expires within minutes). For an Oauth2-style authorization scheme, this could be "Bearer: ......".
         * This must correspond to what the server side authorization plugin expects.
         * <p />
         * <b>NOTE: This SHALL NOT be used to CHANGE the user!</b> It should only refresh an existing authorization for the
         * initially authenticated user. One MatsSocket (Session) shall only be used by a single user: If changing
         * user, you should ditch the existing MatsSocket (preferably invoking 'shutdown' to close the session properly
         * on the server side too), and make a new MatsSocket thus getting a new Session.
         * <p />
         * Note: If the underlying WebSocket has not been established and HELLO sent, then invoking this method will NOT
         * do that - only the first actual MatsSocket message will start the WebSocket and do HELLO.
         *
         * @param authorizationHeader the authorization String which will be resolved to a Principal on the server side by the
         * authorization plugin (and which potentially also will be forwarded to other resources that requires
         * authorization).
         * @param expirationTimestamp the millis-since-epoch at which this authorization (e.g. JWT access
         * token) expires. -1 means "never expires". <i>Notice that in a JWT token, the expiration time is in
         * seconds, not millis: Multiply by 1000.</i>
         * @param roomForLatencyMillis the number of millis which is subtracted from the 'expirationTimestamp' to
         * find the point in time where the MatsSocket will refuse to use the authorization and instead invoke the
         * 'authorizationExpiredCallback' and wait for a new authorization being set by invocation of the present method.
         * Depending on what the usage of the Authorization string is on server side is, this should probably <b>at least</b> be 10000,
         * i.e. 10 seconds - but if the Mats endpoints uses the Authorization string to do further accesses, both latency
         * and queue time must be taken into account (e.g. for calling into another API that also needs a valid token).
         */
        this.setCurrentAuthorization = function (authorizationHeader, expirationTimestamp, roomForLatencyMillis) {
            if (this.logging) log("Got Authorization which "
                + (expirationTimestamp !== -1 ? "Expires in [" + (expirationTimestamp - Date.now()) + " ms]" : "[Never expires]")
                + ", roomForLatencyMillis: " + roomForLatencyMillis);

            _authorization = authorizationHeader;
            _expirationTimestamp = expirationTimestamp;
            _roomForLatencyMillis = roomForLatencyMillis;
            _sendAuthorizationToServer = true;
            // ?: Should we send it now?
            if (_authExpiredCallbackInvoked === AuthorizationRequiredEventType.REAUTHENTICATE) {
                log("Immediate send of AUTH due to SERVER_REQUEST");
                _addEnvelopeToPipeline({
                    t: MessageType.AUTH
                });
            }
            // We're now back to "normal", i.e. not outstanding authorization request.
            _authExpiredCallbackInvoked = undefined;

            // Evaluate whether there are stuff in the pipeline that should be sent now.
            // (Not-yet-sent HELLO does not count..)
            _evaluatePipelineSend();
        };

        /**
         * This can be used by the mechanism invoking 'setCurrentAuthorization(..)' to decide whether it should keep the
         * authorization fresh (i.e. no latency waiting for new authorization is introduced when a new message is
         * enqueued), or fall back to relying on the 'authorizationExpiredCallback' being invoked when a new message needs
         * it (thus introducing latency while waiting for authorization). One could envision keeping fresh auth for 5
         * minutes, but if the user has not done anything requiring authentication (i.e. sending information bearing
         * messages SEND, REQUEST or Replies) in that timespan, you stop doing continuous authentication refresh, falling
         * back to the "on demand" based logic, where when the user does something, the 'authorizationExpiredCallback'
         * is invoked if the authentication is expired.
         *
         * @member {number} lastMessageEnqueuedTimestamp millis-since-epoch of last message enqueued.
         * @memberOf MatsSocket
         * @readonly
         */
        Object.defineProperty(this, "lastMessageEnqueuedTimestamp", {
            get: function () {
                return _lastMessageEnqueuedTimestamp;
            }
        });


        /**
         * Returns whether this MatsSocket <i>currently</i> have a WebSocket connection open. It can both go down
         * by lost connection (driving through a tunnel), where it will start to do reconnection attempts, or because
         * you (the Client) have closed this MatsSocketSession, or because the <i>Server</i> has closed the
         * MatsSocketSession. In the latter cases, where the MatsSocketSession is closed, the WebSocket connection will
         * stay down - until you open a new MatsSocketSession.
         * <p/>
         * Pretty much the same as <code>({@link #state} === {@link ConnectionState#CONNECTED})
         * || ({@link #state} === {@link ConnectionState#SESSION_ESTABLISHED})</code> - however, in the face of
         * {@link MessageType#DISCONNECT}, the state will not change, but the connection is dead ('connected' returns
         * false).
         *
         * @member {string} connected
         * @memberOf MatsSocket
         * @readonly
         */
        Object.defineProperty(this, "connected", {
            get: function () {
                return _webSocket != null;
            }
        });

        /**
         * Returns which one of the {@link ConnectionState} state enums the MatsSocket is in.
         * <ul>
         *     <li>NO_SESSION - initial state, and after Session Close (both from client and server side)</li>
         *     <li>CONNECTING - when we're actively trying to connect, i.e. "new WebSocket(..)" has been invoked, but not yet either opened or closed.</li>
         *     <li>WAITING - if the "new WebSocket(..)" invocation ended in the socket closing, i.e. connection failed, but we're still counting down to next (re)connection attempt.</li>
         *     <li>CONNECTED - if the "new WebSocket(..)" resulted in the socket opening. We still have not established the MatsSocketSession with the server, though.</li>
         *     <li>SESSION_ESTABLISHED - when we're open for business: Both connected, and established MatsSocketSession with the server.</li>
         * </ul>
         *
         * @member {string} state
         * @memberOf MatsSocket
         * @readonly
         */
        Object.defineProperty(this, "state", {
            get: function () {
                return _state;
            }
        });

        /**
         * Stats: Returns an array of the 100 latest {@link PingPong}s.
         *
         * @member {array} state
         * @memberOf MatsSocket
         * @readonly
         */
        Object.defineProperty(this, "pings", {
            get: function () {
                return _pings;
            }
        });

        // ========== Terminator and Endpoint registration ==========

        /**
         * Registers a Terminator, on the specified terminatorId, and with the specified callbacks. A Terminator is
         * the target for Server-to-Client SENDs, and the Server's REPLYs from invocations of
         * <code>requestReplyTo(terminatorId ..)</code> where the terminatorId points to this Terminator.
         *
         * @param endpointId the id of this client side Terminator.
         * @param messageCallback receives an Event when everything went OK, containing the message on the "data" property.
         * @param errorCallback is relevant if this endpoint is set as the replyTo-target on a requestReplyTo(..) invocation, and will
         * get invoked with the Event if the corresponding Promise-variant would have been rejected.
         */
        this.terminator = function (endpointId, messageCallback, errorCallback) {
            // :: Assert for double-registrations
            if (_terminators[endpointId] !== undefined) {
                throw new Error("Cannot register more than one Terminator to same endpointId [" + endpointId + "], existing: " + _terminators[endpointId]);
            }
            if (_endpoints[endpointId] !== undefined) {
                throw new Error("Cannot register a Terminator to same endpointId [" + endpointId + "] as an Endpoint, existing: " + _endpoints[endpointId]);
            }
            log("Registering Terminator on id [" + endpointId + "]:\n #messageCallback: " + messageCallback + "\n #errorCallback: " + errorCallback);
            _terminators[endpointId] = {
                resolve: messageCallback,
                reject: errorCallback
            };
        };

        /**
         * Registers an Endpoint, on the specified endpointId, with the specified "promiseProducer". An Endpoint is
         * the target for Server-to-Client REQUESTs. The promiseProducer is a function that takes a message event
         * (the incoming REQUEST) and produces a Promise, whose return (resolve or reject) is the return value of the
         * endpoint.
         *
         * @param endpointId the id of this client side Endpoint.
         * @param {function} promiseProducer a function that takes a Message Event and returns a Promise which when
         * later either Resolve or Reject will be the return value of the endpoint call.
         */
        this.endpoint = function (endpointId, promiseProducer) {
            // :: Assert for double-registrations
            if (_endpoints[endpointId] !== undefined) {
                throw new Error("Cannot register more than one Endpoint to same endpointId [" + endpointId + "], existing: " + _endpoints[endpointId]);
            }
            if (_terminators[endpointId] !== undefined) {
                throw new Error("Cannot register an Endpoint to same endpointId [" + endpointId + "] as a Terminator, existing: " + _terminators[endpointId]);
            }
            log("Registering Endpoint on id [" + endpointId + "]:\n #promiseProducer: " + promiseProducer);
            _endpoints[endpointId] = promiseProducer;
        };

        /**
         * "Fire-and-forget"-style send-a-message. The returned promise is Resolved if the server receives and accepts it,
         * while it is Rejected if either the current authorization is not accepted ("authHandle") or it cannot be forwarded
         * to the Mats infrastructure (e.g. MQ or any outbox DB is down).
         *
         * @param endpointId
         * @param traceId
         * @param message
         * @returns {Promise<unknown>}
         */
        this.send = function (endpointId, traceId, message) {
            return new Promise(function (resolve, reject) {
                let envelope = Object.create(null);
                envelope.t = MessageType.SEND;
                envelope.eid = endpointId;
                envelope.msg = message;

                // Make lambda for what happens when it has been RECEIVED on server.
                let outstandingInitiation = Object.create(null);
                // Set the Sends's returned Promise's settle functions for ACK and NACK.
                outstandingInitiation.ack = resolve;
                outstandingInitiation.nack = reject;

                _addInformationBearingEnvelopeToPipeline(envelope, traceId, outstandingInitiation, undefined);
            });
        };


        /**
         * Perform a Request, and have the reply come back via the returned Promise. As opposed to Send, where the
         * returned Promise is resolved when the server accepts the message, the Promise is now resolved by the Reply.
         * To get information of whether the server accepted the message, you can provide a receivedCallback - this is
         * invoked when the server accepts the message.
         *
         * @param endpointId
         * @param traceId
         * @param message
         * @param receivedCallback
         * @returns {Promise<unknown>}
         */
        this.request = function (endpointId, traceId, message, receivedCallback) {
            return new Promise(function (resolve, reject) {
                let envelope = Object.create(null);
                envelope.t = MessageType.REQUEST;
                envelope.eid = endpointId;
                envelope.msg = message;

                // Make lambda for what happens when it has been RECEIVED on server.
                let outstandingInitiation = Object.create(null);
                // Set both ACN and NACK to the 'receivedCallback'
                outstandingInitiation.ack = receivedCallback;
                outstandingInitiation.nack = receivedCallback;

                // Make future for the resolving of Promise
                let future = Object.create(null);
                future.resolve = resolve;
                future.reject = reject;

                _addInformationBearingEnvelopeToPipeline(envelope, traceId, outstandingInitiation, future);
            });
        };

        /**
         * Perform a Request, but send the reply to a specific client endpoint registered on this MatsSocket instance.
         * The returned Promise functions as for Send, since the reply will not go to the Promise now. Notice that you
         * can set a CorrelationId which will be available for the Client endpoint when it receives the reply - this
         * is pretty much free form.
         *
         * @param endpointId
         * @param traceId
         * @param message
         * @param replyToTerminatorId
         * @param correlationInformation
         * @returns {Promise<unknown>}
         */
        this.requestReplyTo = function (endpointId, traceId, message, replyToTerminatorId, correlationInformation) {
            // ?: Do we have the Terminator the client requests reply should go to?
            if (!_terminators[replyToTerminatorId]) {
                // -> No, we do not have this. Programming error from app.
                throw new Error("The Client Terminator [" + replyToTerminatorId + "] is not present, !");
            }

            return new Promise(function (resolve, reject) {
                let envelope = Object.create(null);
                envelope.t = MessageType.REQUEST;
                envelope.eid = endpointId;
                envelope.msg = message;

                // Make lambda for what happens when it has been RECEIVED on server.
                let outstandingInitiation = Object.create(null);
                // Set the RequestReplyTop's returned Promise's settle functions for ACK and NACK.
                outstandingInitiation.ack = resolve;
                outstandingInitiation.nack = reject;

                // Make request for the resolving of Promise
                let request = Object.create(null);
                request.replyToEndpointId = replyToTerminatorId;
                request.correlationInformation = correlationInformation;

                _addInformationBearingEnvelopeToPipeline(envelope, traceId, outstandingInitiation, request);
            });
        };

        /**
         * Synchronously flush any pipelined messages, i.e. when the method exits, webSocket.send(..) has been invoked
         * with the serialized pipelined messages, <i>unless</i> the authorization had expired (read more at
         * {@link #setCurrentAuthorization()} and {@link #setAuthorizationExpiredCallback()}).
         */
        this.flush = function () {
            if (_evaluatePipelineLater_timeoutId) {
                clearTimeout(_evaluatePipelineLater_timeoutId);
                _evaluatePipelineLater_timeoutId = undefined;
            }
            _evaluatePipelineSend();
        };

        /**
         * MatsSocket will always iterate through the URL array starting from 0th element. Therefore, the URL array is
         * shuffled. Invoking this method disables this randomization. Must be invoked before MatsSocket connects.
         * Should only be used for testing, as you in production definitely want randomization.
         */
        this.disableUrlRandomization = function () {
            // Just copy over the original array to the "_useUrls" var.
            _useUrls = urls;
        };

        /**
         * Closes any currently open WebSocket with MatsSocket-specific CloseCode CLOSE_SESSION (4000). It <i>also</i>
         * uses <code>navigator.sendBeacon(..)</code> (if present) to send an out-of-band Close Session HTTP POST,
         * or, if {@link #outofbandclose} is a function, it is invoked instead. The SessionId is made undefined. If
         * there currently is a pipeline, this will be dropped (i.e. messages deleted), any outstanding acknowledge
         * callbacks are dropped, acknowledge Promises are rejected, and outstanding Reply Promises (from Requests)
         * are rejected. The effect is to cleanly shut down the Session and MatsSocket (closing session on server side).
         * <p />
         * Afterwards, the MatsSocket can be started up again by sending a message. As The SessionId on this client
         * MatsSocket was cleared (and the previous Session on the server is gone anyway), this will give a new server
         * side Session. If you want a totally clean MatsSocket, then just ditch this instance and make a new one.
         *
         * <b>Note: An 'onBeforeUnload' event handler is registered on 'window' (if present), which invokes this
         * method.</b>
         *
         * @param {string} reason short descriptive string. Will be returned as part of reason string, must be quite
         * short.
         */
        this.close = function (reason) {
            // Fetch properties we need before clearing state
            let currentWsUrl = _currentWebSocketUrl();
            let existingSessionId = that.sessionId;
            log("close(): Closing MatsSocketSession, id:[" + existingSessionId + "] due to [" + reason
                + "], currently connected: [" + (_webSocket ? _webSocket.url : "not connected") + "]");

            // :: In-band Session Close: Close the WebSocket itself with CLOSE_SESSION Close Code.
            // ?: Do we have _webSocket?
            if (_webSocket) {
                // -> Yes, so close WebSocket with MatsSocket-specific CloseCode CLOSE_SESSION 4000.
                log(" \\-> WebSocket is open, so we perform in-band Session Close by closing the WebSocket with MatsSocketCloseCode.CLOSE_SESSION (4000).");
                // Perform the close
                _webSocket.close(MatsSocketCloseCodes.CLOSE_SESSION, "From client: " + reason);
            }

            // Clear out WebSocket "infrastructure", i.e. state and "pinger thread".
            _clearWebSocketStateAndInfrastructure();
            // Close Session and clear all state of this MatsSocket.
            _closeSessionAndClearStateAndPipelineAndFuturesAndOutstandingMessages("From Client: " + reason);

            // :: Out-of-band Session Close
            // ?: Do we have a sessionId?
            if (existingSessionId) {
                // ?: Is the out-of-band close function defined?
                if (this.outofbandclose !== undefined) {
                    // -> Yes, so invoke it
                    this.outofbandclose({
                        currentWsUrl: currentWsUrl,
                        sessionId: existingSessionId
                    });
                } else {
                    // -> No, so do default logic
                    // ?: Do we have 'navigator'?
                    if ((typeof window !== 'undefined') && (typeof window.navigator !== 'undefined')) {
                        // -> Yes, navigator present, so then we can fire off the out-of-band close too
                        // Fire off a "close" over HTTP using navigator.sendBeacon(), so that even if the socket is closed, it is possible to terminate the MatsSocket SessionId.
                        let closeSesionUrl = currentWsUrl.replace("ws", "http") + "/close_session?session_id=" + existingSessionId;
                        log("  \\- Send an out-of-band (i.e. HTTP) close_session, using navigator.sendBeacon('" + closeSesionUrl + "').");
                        let success = window.navigator.sendBeacon(closeSesionUrl);
                        log("    \\- Result: " + (success ? "Enqueued POST, but do not know whether anyone received it - check Network tab of Dev Tools." : "Did NOT manage to enqueue a POST."));
                    }
                }
            }
        };

        /**
         * Effectively emulates "lost connection". Used in testing.
         *
         * @param reason a string saying why.
         */
        this.reconnect = function (reason) {
            log("reconnect(): Closing WebSocket with CloseCode 'RECONNECT (" + MatsSocketCloseCodes.RECONNECT
                + ")', MatsSocketSessionId:[" + that.sessionId + "] due to [" + reason + "], currently connected: [" + (_webSocket ? _webSocket.url : "not connected") + "]");
            if (!_webSocket) {
                throw new Error("There is no live WebSocket to close with RECONNECT closeCode!");
            }
            // Hack for Node: Node is too fast wrt. handling the reply message, so one of the integration tests fails.
            // This tests reconnect in face of having the test RESOLVE in the incomingHandler, which exercises the
            // double-delivery catching when getting a direct RESOLVE instead of an ACK from the server.
            // However, the following RECONNECT-close is handled /after/ the RESOLVE message comes in, and ok's the test.
            // So then, MatsSocket dutifully starts reconnecting - /after/ the test is finished. Thus, Node sees the
            // outstanding timeout "thread" which pings, and never exits. To better emulate an actual lost connection,
            // we /first/ unset the 'onmessage' handler (so that any pending messages surely will be lost), before we
            // close the socket. Notice that a "_sessionOpened" guard was also added, so that it shall explicitly stop
            // such reconnecting in face of an actual .close(..) invocation.

            // First unset message handler so that we do not receive any more WebSocket messages (but NOT unset 'onclose', nor 'onerror')
            _webSocket.onmessage = undefined;
            // Now closing the WebSocket (thus getting the 'onclose' handler invoked - just as if we'd lost connection, or got this RECONNECT close from Server).
            _webSocket.close(MatsSocketCloseCodes.RECONNECT, reason);
        };

        /**
         * Convenience method for making random strings meant for user reading, e.g. in TraceIds, since this
         * alphabet only consists of lower and upper case letters, and digits. To make a traceId "unique enough" for
         * finding it in a log system, a length of 6 should be plenty.
         *
         * @param {number} length how long the string should be. 6 should be enough to make a TraceId "unique enough"
         * to uniquely find it in a log system. If you want "absolute certainty" that there never will be any collisions,
         * go for 20.
         * @returns {string} a random string consisting of characters from from digits, lower and upper case letters
         * (62 chars).
         */
        this.id = function (length) {
            let result = '';
            for (let i = 0; i < length; i++) {
                result += _alphabet[Math.floor(Math.random() * _alphabet.length)];
            }
            return result;
        };

        /**
         * Convenience method for making random strings for correlationIds, not meant for human reading ever
         * (choose e.g. length=8), as the alphabet consist of all visible ACSII chars that won't be quoted in a JSON
         * string. Should you want to make free-standing SessionIds or similar, you would want to have a longer length,
         * use e.g. length=16.
         *
         * @param {number} length how long the string should be, e.g. 8 chars for a pretty safe correlationId.
         * @returns {string} a random string consisting of characters from all visible and non-JSON-quoted chars of
         * ASCII (92 chars).
         */
        this.jid = function (length) {
            let result = '';
            for (let i = 0; i < length; i++) {
                result += _jsonAlphabet[Math.floor(Math.random() * _jsonAlphabet.length)];
            }
            return result;
        };

        // ==============================================================================================
        // PRIVATE
        // ==============================================================================================

        function log(msg, object) {
            if (that.logging) {
                if (object) {
                    console.log(_instanceId + "/" + that.sessionId + ": " + msg, object);
                } else {
                    console.log(_instanceId + "/" + that.sessionId + ": " + msg);
                }
            }
        }

        function error(type, msg, err) {
            if (err) {
                console.error(type + ": " + msg, err);
            } else {
                console.error(type + ": " + msg);
            }
        }

        // Simple Alphabet: All digits, lower and upper ASCII chars: 10 + 26 x 2 = 62 chars.
        let _alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        // Alphabet of JSON-non-quoted and visible chars: 92 chars.
        let _jsonAlphabet = "!#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{|}~";

        // The URLs to use - will be shuffled. Can be reset to not randomized by this.disableUrlRandomize()
        let _useUrls = [].concat(urls);
        // Shuffle the URLs
        _shuffleArray(_useUrls);

        // fields

        let _instanceId = that.id(5);

        // If true, we're currently already trying to get a WebSocket
        let _webSocketConnecting = false;
        // If not undefined, we have an open WebSocket available.
        let _webSocket = undefined;
        // If false, we should not accidentally try to reconnect or similar
        let _sessionOpened = false; // NOTE: Set to true upon enqueuing of information-bearing message.

        let _pipeline = [];
        let _terminators = Object.create(null);
        let _endpoints = Object.create(null);
        let _inbox = Object.create(null);

        let _sessionClosedEventListeners = [];
        let _connectionEventListeners = [];

        let _state = ConnectionState.NO_SESSION;

        let _helloSent = false;
        let _reconnect_ForceSendHello = false;

        let _authorization = undefined;
        let _currentAuthorizationSentToServer = undefined;
        let _sendAuthorizationToServer = false;
        let _expirationTimestamp = undefined;
        let _roomForLatencyMillis = undefined;
        let _authorizationExpiredCallback = undefined;
        let _lastMessageEnqueuedTimestamp = Date.now(); // Start by assuming that it was just used.

        let _messageSequenceId = 0; // Increases for each SEND, REQUEST and REPLY

        // When we've informed the app that we need auth, we do not need to do it again until it has set it.
        let _authExpiredCallbackInvoked = false;

        // Outstanding Pings
        const _outstandingPings = Object.create(null);
        // Outstanding Request "futures", i.e. the resolve() and reject() functions of the returned Promise.
        const _outstandingRequests = Object.create(null);
        // Outbox for SEND and REQUEST messages waiting for Received ACK/NACK
        const _outboxInitiations = Object.create(null);
        // .. a "guard object" to avoid having to retransmit messages sent /before/ the HELLO/WELCOME handshake
        let _outboxInitiations_RetransmitGuard = this.jid(5);
        // Outbox for REPLYs
        const _outboxReplies = Object.create(null);

        // :: STATS
        // Last 100 PINGs
        const _pings = [];

        function _invokeLater(that) {
            setTimeout(that, 0);
        }

        // https://stackoverflow.com/a/12646864/39334
        function _shuffleArray(array) {
            for (let i = array.length - 1; i > 0; i--) {
                let j = Math.floor(Math.random() * (i + 1));
                let temp = array[i];
                array[i] = array[j];
                array[j] = temp;
            }
        }

        function _beforeunloadHandler() {
            that.close(clientLibNameAndVersion + " window.onbeforeunload close");
        }

        function _registerBeforeunload() {
            // ?: Is the self object an EventTarget (ie. window in a Browser context)
            if (typeof (self) === 'object' && typeof (self.addEventListener) === 'function') {
                // Yes -> Add "onbeforeunload" event listener to shut down the MatsSocket cleanly (closing session) when user navigates away from page.
                self.addEventListener("beforeunload", _beforeunloadHandler);
            }
        }

        function _deregisterBeforeunload() {
            if (typeof (self) === 'object' && typeof (self.addEventListener) === 'function') {
                self.removeEventListener("beforeunload", _beforeunloadHandler);
            }
        }

        function _clearWebSocketStateAndInfrastructure() {
            log("clearWebSocketStateAndInfrastructure(). Current WebSocket:" + _webSocket);
            // Stop pinger
            _stopPinger();
            // Remove beforeunload eventlistener
            _deregisterBeforeunload();
            // Reset Reconnect state vars
            _resetReconnectStateVars();
            // Make new RetransmitGuard - so that any previously "guarded" messages now will be retransmitted.
            _outboxInitiations_RetransmitGuard = that.jid(5);
            // :: Clear out _webSocket;
            if (_webSocket) {
                // We don't want the onclose callback invoked from this event that we initiated ourselves.
                _webSocket.onclose = undefined;
                // We don't want any messages either
                _webSocket.onmessage = undefined;
                // Also drop onerror for good measure.
                _webSocket.onerror = undefined;
            }
            _webSocket = undefined;
        }

        function _closeSessionAndClearStateAndPipelineAndFuturesAndOutstandingMessages(reason) {
            // Clear state
            delete (that.sessionId);
            _state = ConnectionState.NO_SESSION;

            // :: Clear pipeline
            _pipeline.length = 0;

            // Were now also closed (i.e. not Opened) - until a new message is enqueued.
            _sessionOpened = false;

            // :: Reject all outstanding messages
            for (let cmid in _outboxInitiations) {
                // noinspection JSUnfilteredForInLoop: Using Object.create(null)
                let initiation = _outboxInitiations[cmid];
                // noinspection JSUnfilteredForInLoop: Using Object.create(null)
                delete _outboxInitiations[cmid];

                if (that.logging) log("Close Session: Clearing outstanding Initiation [" + initiation.envelope.t + "] to [" + initiation.envelope.eid + "], cmid:[" + initiation.envelope.cmid + "], TraceId:[" + initiation.envelope.tid + "].");
                if (initiation.nack) {
                    try {
                        let nackEvent = _createReceivedEvent(ReceivedEventType.SESSION_CLOSED, initiation.envelope.tid, initiation.sentTimestamp, Date.now(), performance.now() - initiation.performanceNow);
                        nackEvent.desc = "Session Closed: Session was closed due to [" + reason + "]";
                        initiation.nack(nackEvent);
                    } catch (err) {
                        error("Got error while clearing outstanding Initiation [" + initiation.envelope.t + "] to [" + initiation.envelope.eid + "], cmid:[" + initiation.envelope.cmid + "], TraceId:[" + initiation.envelope.tid + "].", err);
                    }
                }
            }

            // :: Reject all Requests
            for (let cmid in _outstandingRequests) {
                // noinspection JSUnfilteredForInLoop: Using Object.create(null)
                let request = _outstandingRequests[cmid];
                // noinspection JSUnfilteredForInLoop: Using Object.create(null)
                delete _outstandingRequests[cmid];

                if (that.logging) log("Close Session: Clearing outstanding REQUEST to [" + request.envelope.eid + "], cmid:[" + request.envelope.cmid + "], TraceId:[" + request.envelope.tid + "].", request);
                if (request.reject) {
                    try {
                        let rejectEvent = _createMessageEvent({}, MessageEventType.SESSION_CLOSED, request.envelope.cmid, request.envelope.tid, request.sentTimestamp, Date.now(), performance.now() - request.performanceNow, undefined);
                        rejectEvent.desc = "Session Closed: Session was closed due to [" + reason + "]";
                        request.reject(rejectEvent);
                    } catch (err) {
                        error("Got error while clearing outstanding REQUEST to [" + request.envelope.eid + "], cmid:[" + request.envelope.cmid + "], TraceId:[" + request.envelope.tid + "].", err);
                    }
                }
            }
        }

        /**
         * <b>YOU SHOULD PROBABLY NOT USE THIS!</b>, instead using the specific prototype methods for generating messages.
         * Add a message to the outgoing pipeline, evaluates whether to send the pipeline afterwards (i.e. if pipelining
         * is active or not).
         */
        function _addInformationBearingEnvelopeToPipeline(envelope, traceId, outstandingInitiation, outstandingRequest) {
            // This is an information-bearing message, so now we are Opened!
            _sessionOpened = true;
            let now = Date.now();
            let performanceNow = performance.now();

            // Add the traceId to the message
            envelope.tid = traceId;
            // Add next message Sequence Id
            let thisMessageSequenceId = _messageSequenceId++;
            envelope.cmid = thisMessageSequenceId;

            // If debug != undefined || 0, then store it in envelope
            if (that.debug && (that.debug !== 0)) {
                envelope.rd = debug;
            }

            // Update timestamp of last "information bearing message" sent.
            _lastMessageEnqueuedTimestamp = now;

            // Make lambda for what happens when it has been RECEIVED on server.
            // Store the outgoing envelope
            outstandingInitiation.envelope = envelope;
            // Store which "retransmitGuard" we were at when sending this (look up usage for why)
            outstandingInitiation.retransmitGuard = _outboxInitiations_RetransmitGuard;
            // Start the attempt counter. We start at 1, as we immediately will enqueue this envelope..
            outstandingInitiation.attempt = 1;
            // Store the sent timestamp.
            outstandingInitiation.sentTimestamp = now;
            // Store TraceId
            outstandingInitiation.traceId = traceId;
            // Store performance.now for round-trip timing
            outstandingInitiation.performanceNow = performanceNow;

            // Then - store the outstanding Send or Request
            _outboxInitiations[thisMessageSequenceId] = outstandingInitiation;

            // ?: Do we have a outstandingRequest?
            if (outstandingRequest) {
                // Store the outgoing envelope
                outstandingRequest.envelope = envelope;
                // Store TraceId
                outstandingRequest.traceId = traceId;
                // Store the sent timestamp.
                outstandingRequest.sentTimestamp = now;
                // Store performance.now for round-trip timing
                outstandingRequest.performanceNow = performanceNow;

                // Then - store this outstanding Request.
                _outstandingRequests[thisMessageSequenceId] = outstandingRequest;
            }

            _addEnvelopeToPipeline(envelope);
        }

        /**
         * Unconditionally adds the supplied envelope to the pipeline, and then evaluates the pipeline,
         * invokeLater-style so as to get "auth-pipelining". Use flush() to get sync send.
         */
        function _addEnvelopeToPipeline(envelope) {
            // ?: Have we sent hello, i.e. session is "active", but the authorization has changed since last we sent over authorization?
            if (_helloSent && (_currentAuthorizationSentToServer !== _authorization)) {
                if (that.logging) log("Authorization has changed, so we add it to the envelope about to be enqueued, of type ["+envelope.t+"].");
                // -> Yes, it has changed, so set new, and add it to envelope of this next message.
                _currentAuthorizationSentToServer = _authorization;
                envelope.auth = _authorization;
            }
            _pipeline.push(envelope);
            if (that.logging) log("Pushed to pipeline: " + JSON.stringify(envelope));
            _evaluatePipelineLater();
        }

        let _evaluatePipelineLater_timeoutId = undefined;

        function _evaluatePipelineLater() {
            if (_evaluatePipelineLater_timeoutId) {
                clearTimeout(_evaluatePipelineLater_timeoutId);
            }
            _evaluatePipelineLater_timeoutId = setTimeout(function () {
                _evaluatePipelineSend();
                _evaluatePipelineLater_timeoutId = undefined;
            }, 2);
        }

        function _requestNewAuthorizationFromApp(what, event) {
            // ?: Have we already asked app for new auth?
            if (_authExpiredCallbackInvoked) {
                // -> Yes, so just return.
                log("Authorization was " + what + ", but we've already asked app for it due to: [" + _authExpiredCallbackInvoked + "].");
                return;
            }
            // E-> No, not asked for auth - so do it.
            log("Authorization was " + what + ". Will not send pipeline until gotten. Invoking 'authorizationExpiredCallback', type:[" + event.type + "].");
            // We will have asked for auth after this.
            _authExpiredCallbackInvoked = event.type;
            // Assert that we have callback
            if (!_authorizationExpiredCallback) {
                error("missingauthcallback", "Was about to ask app for new Authorization header, but the 'authorizationExpiredCallback' is not present.");
                return;
            }
            // Ask app for new auth
            _authorizationExpiredCallback(event);
        }

        /**
         * Sends pipelined messages if pipelining is not engaged.
         */
        function _evaluatePipelineSend() {
            // ?: Are there any messages in pipeline, or should we force processing to get HELLO due to reconnect.
            if ((_pipeline.length === 0) && !_reconnect_ForceSendHello) {
                // -> No, no message in pipeline, and we should not force processing to get HELLO
                // Nothing to do, drop out.
                return;
            }
            // ?: Do we have authorization?!
            if (_authorization === undefined) {
                // -> No, authorization not present.
                _requestNewAuthorizationFromApp("not present", new AuthorizationRequiredEvent(AuthorizationRequiredEventType.NOT_PRESENT, undefined));
                return;
            }
            // ?: Check whether we have expired authorization
            if ((_expirationTimestamp !== undefined) && (_expirationTimestamp !== -1)
                && ((_expirationTimestamp - _roomForLatencyMillis) < Date.now())) {
                // -> Yes, authorization is expired.
                _requestNewAuthorizationFromApp("expired", new AuthorizationRequiredEvent(AuthorizationRequiredEventType.EXPIRED, _expirationTimestamp));
                return;
            }
            // ?: Check that we are not already waiting for new auth
            if (_authExpiredCallbackInvoked) {
                log("We have asked app for new authorization, and still waiting for it.");
                return;
            }

            // ----- Not pipelining, and auth is present.

            // ?: Are we trying to open websocket?
            if (_webSocketConnecting) {
                log("evaluatePipelineSend(): WebSocket is currently connecting. Cannot send yet.");
                // -> Yes, so then the socket is not open yet, but we are in the process.
                // Return now, as opening is async. When the socket opens, it will re-run 'evaluatePipelineSend()'.
                return;
            }

            // ?: Is the WebSocket present?
            if (_webSocket === undefined) {
                log("evaluatePipelineSend(): WebSocket is not present, so initiate creation. Cannot send yet.");
                // -> No, so go get it.
                _initiateWebSocketCreation();
                // Returning now, as opening is async. When the socket opens, it will re-run 'evaluatePipelineSend()'.
                return;
            }

            // ----- We have an open WebSocket!

            let prePipeline = undefined;

            // -> Yes, WebSocket is open, so send any outstanding messages
            // ?: Have we sent HELLO?
            if (!_helloSent) {
                // -> No, HELLO not sent, so we create it now (auth is present, check above)
                let helloMessage = {
                    t: MessageType.HELLO,
                    clv: clientLibNameAndVersion + "; User-Agent: " + userAgent,
                    ts: Date.now(),
                    an: appName,
                    av: appVersion,
                    auth: _authorization,  // This is guaranteed to be in place and valid, see above
                    cid: that.id(10),
                    tid: "MatsSocket_start_" + that.id(6)
                };
                // ?: Have we requested a reconnect?
                if (that.sessionId !== undefined) {
                    log("HELLO not send, adding to pre-pipeline. HELLO (\"Reconnect\") to MatsSocketSessionId:[" + that.sessionId + "]");
                    // -> Evidently yes, so add the requested reconnect-to-sessionId.
                    helloMessage.sid = that.sessionId;
                } else {
                    // -> We want a new session (which is default anyway)
                    log("HELLO not sent, adding to pre-pipeline. HELLO (\"New\"), we will get assigned a MatsSocketSessionId upon WELCOME.");
                }
                // Add the HELLO to the prePipeline
                prePipeline = [];
                prePipeline.unshift(helloMessage);
                // We will now have sent the HELLO, so do not send it again.
                _helloSent = true;
                // .. and again, we will now have sent the HELLO - If this was a RECONNECT, we have now done the immediate reconnect HELLO.
                // (As opposed to initial connection, where we do not send before having an actual information bearing message in pipeline)
                _reconnect_ForceSendHello = false;
                // We've sent the current auth
                _currentAuthorizationSentToServer = _authorization;
            }

            // :: Send pre-pipeline messages, if there are any
            // (Before the HELLO is sent and sessionId is established, the max size of message is low on the server)
            if (prePipeline) {
                if (that.logging) log("Flushing prePipeline of [" + prePipeline.length + "] messages.");
                _webSocket.send(JSON.stringify(prePipeline));
                // NOTE: prePipeline is function scoped, so it "clears" when this function exits.
            }
            // :: Send any pipelined messages.
            if (_pipeline.length > 0) {
                if (that.logging) log("Flushing pipeline of [" + _pipeline.length + "] messages:" + JSON.stringify(_pipeline));
                _webSocket.send(JSON.stringify(_pipeline));
                // Clear pipeline
                _pipeline.length = 0;
            }
        }

        let _urlIndexCurrentlyConnecting = 0; // Cycles through the URLs
        let _connectionAttemptRound = 0; // When cycled one time through URLs, increases.
        let _connectionTimeoutBase = 500; // Base timout, milliseconds. Doubles, up to max defined below.
        let _connectionTimeoutMinIfSingleUrl = 5000; // Min timeout if single-URL configured, milliseconds.
        // Based on whether there is multiple URLs, or just a single one, we choose the short "timeout base", or a longer one, as minimum.
        let _connectionTimeoutMin = _useUrls.length > 1 ? _connectionTimeoutBase : _connectionTimeoutMinIfSingleUrl;
        let _connectionTimeoutMax = 15000; // Milliseconds max between connection attempts.

        function _currentWebSocketUrl() {
            log("## Using urlIndexCurrentlyConnecting [" + _urlIndexCurrentlyConnecting + "]: " + _useUrls[_urlIndexCurrentlyConnecting] + ", round:" + _connectionAttemptRound);
            return _useUrls[_urlIndexCurrentlyConnecting];
        }

        function _secondsTenths(milliseconds) {
            // Rounds to tenth of second, e.g. 2730 -> 2.7.
            return Math.round(milliseconds / 100) / 10;
        }

        function _resetReconnectStateVars() {
            _urlIndexCurrentlyConnecting = 0;
            _connectionAttemptRound = 0;
        }

        function _updateStateAndNotifyConnectionEventListeners(connectionEvent) {
            // ?: Should we log? Logging is on, AND either NOT CountDown, OR CountDown and whole second.
            if (that.logging && ((connectionEvent.type !== ConnectionEventType.COUNTDOWN)
                || (connectionEvent.countdownSeconds === connectionEvent.timeoutSeconds)
                || (Math.round(connectionEvent.countdownSeconds) === connectionEvent.countdownSeconds))) {
                log("Sending ConnectionEvent to listeners", connectionEvent);
            }
            // ?: Is this a state?
            let result = Object.keys(ConnectionState).filter(function (key) {
                return ConnectionState[key] === connectionEvent.type;
            });
            if (result.length === 1) {
                // -> Yes, this is a state - so update the state..!
                _state = ConnectionState[result[0]];
                log("The ConnectionEventType [" + result[0] + "] is also a ConnectionState - setting MatsSocket state [" + _state + "].");
            }

            // :: Notify all ConnectionEvent listeners.
            for (let i = 0; i < _connectionEventListeners.length; i++) {
                try {
                    _connectionEventListeners[i](connectionEvent);
                } catch (err) {
                    error("notify ConnectionEvent listeners", "Caught error when notifying one of the [" + _connectionEventListeners.length + "] ConnectionEvent listeners about [" + connectionEvent.type + "].", err);
                }
            }
        }

        function _initiateWebSocketCreation() {
            // ?: Assert that we do not have the WebSocket already
            if (_webSocket !== undefined) {
                // -> Damn, we did have a WebSocket. Why are we here?!
                throw (new Error("Should not be here, as WebSocket is already in place, or we're trying to connect"));
            }
            // E-> No, WebSocket ain't open..

            // ?: Assert that we aren't actually closed (could conceivably have happened async)
            if (!_sessionOpened) {
                // -> We've been asynchronously closed - bail out from opening WebSocket
                throw (new Error("We're closed, so we should not open WebSocket"));
            }

            // :: We are currently trying to connect!
            _webSocketConnecting = true;

            // Timeout: LESSER of "max" and "timeoutBase * (2^round)", which should lead to timeoutBase x1, x2, x4, x8 - but capped at max.
            // .. but at least 'minTimeout', which handles the special case of longer minimum if just 1 URL.
            let timeout = Math.max(_connectionTimeoutMin,
                Math.min(_connectionTimeoutMax, _connectionTimeoutBase * Math.pow(2, _connectionAttemptRound)));
            let currentCountdownTargetTimestamp = Date.now();
            let targetTimeoutTimestamp = currentCountdownTargetTimestamp + timeout;
            let secondsLeft = function () {
                return Math.round(((targetTimeoutTimestamp - Date.now()) / 100)) / 10;
            };

            // About to create WebSocket, so notify our listeners about this.
            _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionEventType.CONNECTING, _currentWebSocketUrl(), undefined, _secondsTenths(timeout), secondsLeft()));

            // Create the WebSocket
            let websocket = socketFactory(_currentWebSocketUrl(), "matssocket");

            let increaseReconnectStateVars = function () {
                _urlIndexCurrentlyConnecting++;
                if (_urlIndexCurrentlyConnecting >= _useUrls.length) {
                    _urlIndexCurrentlyConnecting = 0;
                    _connectionAttemptRound++;
                }
            };

            // Make a "connection timeout" thingy,
            // This will re-invoke itself every 100 ms to created the COUNTDOWN events - until either cancelled by connect,
            // or it reaches targetTimeoutTimestamp (timeout), where it bumps the state vars, and then re-runs the present method.
            let countdownId;
            let countDownTimer = function () {
                // :: Find next target
                while (currentCountdownTargetTimestamp <= Date.now()) {
                    currentCountdownTargetTimestamp += 100;
                }
                // ?: Have we now hit or overshot the target?
                if (currentCountdownTargetTimestamp >= targetTimeoutTimestamp) {
                    // -> Yes, we've hit target, so this did not work out.
                    // :: Bad attempt, clear this WebSocket out
                    log("Create WebSocket: Timeout exceeded [" + timeout + "], URL [" + _currentWebSocketUrl() + "] is bad so ditch it.");
                    // Clear out the handlers
                    websocket.onopen = undefined;
                    websocket.onerror = undefined;
                    websocket.onclose = undefined;
                    // Close the current WebSocket connection attempt (i.e. abort connect if still trying).
                    websocket.close(4999, "WebSocket connect timeout");
                    // Invoke after a small random number of millis: Bump reconnect state vars, re-run _initiateWebSocketCreation
                    setTimeout(function () {
                        increaseReconnectStateVars();
                        _initiateWebSocketCreation();
                    }, Math.round(Math.random() * 200));
                } else {
                    // -> No, we've NOT hit target, so sleep till next countdown target, where we re-invoke ourselves (this countDownTimer())
                    // Notify ConnectionEvent listeners about this COUNTDOWN event.
                    _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionEventType.COUNTDOWN, _currentWebSocketUrl(), undefined, _secondsTenths(timeout), secondsLeft()));
                    let sleep = Math.max(5, currentCountdownTargetTimestamp - Date.now());
                    countdownId = setTimeout(function () {
                        countDownTimer();
                    }, sleep);
                }
            };
            // Initial invoke of our countdown-timer.
            countDownTimer();

            // :: Add the handlers for this "trying to acquire" procedure.

            // Error: Just log for debugging. NOTICE: Upon Error, Close is also always invoked.
            websocket.onerror = function (event) {
                log("Create WebSocket: error.", event);
            };

            // Close: Log + IF this is the first "round" AND there is multiple URLs, then immediately try the next URL. (Close may happen way before the Connection Timeout)
            websocket.onclose = function (closeEvent) {
                log("Create WebSocket: close. Code:" + closeEvent.code + ", Reason:" + closeEvent.reason, closeEvent);
                _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionEventType.WAITING, _currentWebSocketUrl(), closeEvent, _secondsTenths(timeout), secondsLeft()));

                // ?: If we are on the FIRST (0th) round of trying out the different URLs, then immediately try the next
                // .. But only if there are multiple URLs configured.
                if ((_connectionAttemptRound === 0) && (_useUrls.length > 1)) {
                    // -> YES, we are on the 0th round of connection attempts, and there are multiple URLs, so immediately try the next.
                    // Cancel the "connection timeout" thingy
                    clearTimeout(countdownId);
                    // Invoke on next tick: Bump state vars, re-run _initiateWebSocketCreation
                    _invokeLater(function () {
                        increaseReconnectStateVars();
                        _initiateWebSocketCreation();
                    });
                }
                // E-> NO, we are either not on the 0th round of attempts, OR there is just a single URL.
                // Therefore, let the countdown timer do its stuff.
            };

            // Open: Success! Cancel countdown timer, and set WebSocket in MatsSocket, clear flags, set proper WebSocket event handlers including onMessage.
            websocket.onopen = function (event) {
                log("Create WebSocket: opened!", event);
                // Cancel the "connection timeout" thingy
                clearTimeout(countdownId);

                // Store our brand new, open-for-business WebSocket.
                _webSocket = websocket;
                // We're not /trying/ to connect anymore..
                _webSocketConnecting = false;
                // Since we've just established this WebSocket, we have obviously not sent HELLO yet.
                _helloSent = false;

                // Set our proper handlers
                websocket.onopen = undefined; // No need for 'onopen', it is already open. Also, node.js evidently immediately fires it again, even though it was already fired.
                websocket.onerror = _onerror;
                websocket.onclose = _onclose;
                websocket.onmessage = _onmessage;

                _registerBeforeunload();

                _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionEventType.CONNECTED, _currentWebSocketUrl(), event, undefined, undefined));

                // Fire off any waiting messages, next tick
                _invokeLater(function () {
                    log("We're open for business! Running evaluatePipelineSend()..");
                    _evaluatePipelineSend();
                });
            };
        }

        function _onerror(event) {
            error("websocket.onerror", "Got 'onerror' event from WebSocket.", event);
            // :: Synchronously notify our ConnectionEvent listeners.
            _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionEventType.CONNECTION_ERROR, _currentWebSocketUrl(), event, undefined, undefined));
        }

        function _onclose(closeEvent) {
            log("websocket.onclose", closeEvent);

            // Note: here the WebSocket is already closed, so we don't have to close it..!

            // Clear out WebSocket "infrastructure", i.e. state and "pinger thread".
            _clearWebSocketStateAndInfrastructure();

            // ?: Special codes, that signifies that we should close (terminate) the MatsSocketSession.
            if ((closeEvent.code === MatsSocketCloseCodes.UNEXPECTED_CONDITION)
                || (closeEvent.code === MatsSocketCloseCodes.PROTOCOL_ERROR)
                || (closeEvent.code === MatsSocketCloseCodes.VIOLATED_POLICY)
                || (closeEvent.code === MatsSocketCloseCodes.CLOSE_SESSION)
                || (closeEvent.code === MatsSocketCloseCodes.SESSION_LOST)) {
                // -> One of the specific "Session is closed" CloseCodes -> Reject all outstanding, this MatsSocket is trashed.
                error("session closed from server", "The WebSocket was closed with a CloseCode [" + MatsSocketCloseCodes.nameFor(closeEvent.code) + "] signifying that our MatsSocketSession is closed, reason:[" + closeEvent.reason + "].", closeEvent);

                // Close Session, Clear all state.
                _closeSessionAndClearStateAndPipelineAndFuturesAndOutstandingMessages("From Server: " + closeEvent.reason);

                // :: Synchronously notify our SessionClosedEvent listeners
                // NOTE: This shall only happen if Close Session is from ServerSide (that is, here), otherwise, if the app invoked matsSocket.close(), one would think the app knew about the close itself..!
                for (let i = 0; i < _sessionClosedEventListeners.length; i++) {
                    try {
                        _sessionClosedEventListeners[i](closeEvent);
                    } catch (err) {
                        error("notify SessionClosedEvent listeners", "Caught error when notifying one of the [" + _sessionClosedEventListeners.length + "] SessionClosedEvent listeners.", err);
                    }
                }
            } else {
                // -> NOT one of the specific "Session is closed" CloseCodes -> Reconnect and Reissue all outstanding..
                if (closeEvent.code !== MatsSocketCloseCodes.DISCONNECT) {
                    log("We were closed with a CloseCode [" + MatsSocketCloseCodes.nameFor(closeEvent.code) + "] that does NOT denote that we should close the session. Initiate reconnect and reissue all outstanding.");
                } else {
                    log("We were closed with the special DISCONNECT close code - act as we lost connection, but do NOT start to reconnect.");
                }

                // :: This is a reconnect - so we should do pipeline processing right away, to get the HELLO over.
                _reconnect_ForceSendHello = true;

                // :: Synchronously notify our ConnectionEvent listeners.
                _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionEventType.LOST_CONNECTION, _currentWebSocketUrl(), closeEvent, undefined, undefined));

                // ?: Is this the special DISCONNECT that asks us to NOT start reconnecting?
                if (closeEvent.code !== MatsSocketCloseCodes.DISCONNECT) {
                    // -> No, not special DISCONNECT - so start reconnecting.
                    // :: Start reconnecting, but give the server a little time to settle
                    setTimeout(function () {
                        _initiateWebSocketCreation();
                    }, 200 + Math.round(Math.random() * 200));
                }
            }
        }

        function _onmessage(webSocketEvent) {
            let receivedTimestamp = Date.now();
            let data = webSocketEvent.data;
            let envelopes = JSON.parse(data);

            let numEnvelopes = envelopes.length;
            if (that.logging) log("onmessage: Got " + numEnvelopes + " messages.");

            for (let i = 0; i < numEnvelopes; i++) {
                let envelope = envelopes[i];
                try {
                    if (that.logging) log(" \\- onmessage: handling message " + i + ": " + envelope.t + ", envelope:" + JSON.stringify(envelope));

                    if (envelope.t === MessageType.WELCOME) {
                        // Fetch our assigned MatsSocketSessionId
                        that.sessionId = envelope.sid;
                        if (that.logging) log("We're WELCOME! SessionId:" + that.sessionId + ", there are [" + Object.keys(_outboxInitiations).length + "] outstanding sends-or-requests, and [" + Object.keys(_outboxReplies).length + "] outstanding replies.");
                        // :: Synchronously notify our ConnectionEvent listeners.
                        _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionEventType.SESSION_ESTABLISHED, _currentWebSocketUrl(), undefined, undefined, undefined));
                        // Start pinger (AFTER having set ConnectionState to SESSION_ESTABLISHED, otherwise it'll exit!)
                        _startPinger();

                        // TODO: Test this outstanding-stuff! Both that they are actually sent again, and that server handles the (quite possible) double-delivery.

                        // ::: RETRANSMIT: If we have stuff in our outboxes, we might have to send them again (we send unless "RetransmitGuard" tells otherwise).

                        // :: Outstanding SENDs and REQUESTs
                        for (let key in _outboxInitiations) {
                            // noinspection JSUnfilteredForInLoop: Using Object.create(null)
                            let initiation = _outboxInitiations[key];
                            let initiationEnvelope = initiation.envelope;
                            // ?: Is the RetransmitGuard the same as we currently have?
                            if (initiation.retransmitGuard === _outboxInitiations_RetransmitGuard) {
                                // -> Yes, so it makes little sense in sending these messages again just yet.
                                if (that.logging) log("RetransmitGuard: The outstanding Initiation [" + initiationEnvelope.t + "] with cmid:[" + initiationEnvelope.cmid + "] and TraceId:[" + initiationEnvelope.tid
                                    + "] was created with the same RetransmitGuard as we currently have [" + _outboxInitiations_RetransmitGuard + "] - they were sent directly trailing HELLO, before WELCOME came back in. No use in sending again.");
                                continue;
                            }
                            initiation.attempt++;
                            if (initiation.attempt > 10) {
                                error("toomanyretries", "Upon reconnect: Too many attempts at sending Initiation [" + initiationEnvelope.t + "] with cmid:[" + initiationEnvelope.cmid + "], TraceId[" + initiationEnvelope.tid + "], size:[" + JSON.stringify(initiationEnvelope).length + "].");
                                continue;
                            }
                            // NOTICE: Won't delete it here - that is done when we process the ACK from server
                            _addEnvelopeToPipeline(initiationEnvelope);
                            // Flush for each message, in case the size of the message was of issue why we closed (maybe pipeline was too full).
                            that.flush();
                        }

                        // :: Outstanding Replies
                        // NOTICE: Since we cannot possibly have replied to a Server Request BEFORE we get the WELCOME, we do not need RetransmitGuard for Replies
                        // (Point is that the RetransmitGuard guards against sending again messages that we sent "along with" the HELLO, before we got the WELCOME.
                        // A Request from the Server cannot possibly come in before WELCOME (as that is by protcol definition the first message we get from the Server),
                        // so there will "axiomatically" not be any outstanding Replies with the same RetransmitGuard as we currently have).
                        for (let key in _outboxReplies) {
                            // noinspection JSUnfilteredForInLoop: Using Object.create(null)
                            let reply = _outboxReplies[key];
                            let replyEnvelope = reply.envelope;
                            reply.attempt++;
                            if (reply.attempt > 10) {
                                error("toomanyretries", "Upon reconnect: Too many attempts at sending Reply [" + replyEnvelope.t + "] with smid:[" + replyEnvelope.smid + "], TraceId[" + replyEnvelope.tid + "], size:[" + JSON.stringify(replyEnvelope).length + "].");
                                continue;
                            }
                            // NOTICE: Won't delete it here - that is done when we process the ACK from server
                            _addEnvelopeToPipeline(replyEnvelope);
                            // Flush for each message, in case the size of the message was of issue why we closed (maybe pipeline was too full).
                            that.flush();
                        }

                    } else if (envelope.t === MessageType.REAUTH) {
                        // -> Server asks us to get new Authentication, as the one he has "on hand" is too old to send us outgoing messages
                        _requestNewAuthorizationFromApp("requested new by server", new AuthorizationRequiredEvent(AuthorizationRequiredEventType.REAUTHENTICATE, undefined));


                    } else if (envelope.t === MessageType.RETRY) {
                        // -> Server asks us to RETRY the information-bearing-message

                        // TODO: Test RETRY!

                        // ?: Is it an outstanding Send or Request
                        let initiation = _outboxInitiations[envelope.cmid];
                        if (initiation) {
                            let initiationEnvelope = initiation.envelope;
                            initiation.attempt++;
                            if (initiation.attempt > 10) {
                                error("toomanyretries", "Upon RETRY-request: Too many attempts at sending [" + initiationEnvelope.t + "] with cmid:[" + initiationEnvelope.cmid + "], TraceId[" + initiationEnvelope.tid + "], size:[" + JSON.stringify(initiationEnvelope).length + "].");
                                continue;
                            }
                            // Note: the retry-cycles will start at attempt=2, since we initialize it with 1, and have already increased it by now.
                            let retryDelay = Math.pow(2, (initiation.attempt - 2)) * 500 + Math.round(Math.random() * 1000);
                            setTimeout(function () {
                                _addEnvelopeToPipeline(initiationEnvelope);
                            }, retryDelay);
                            continue;
                        }
                        // E-> Was not outstanding Send or Request

                        // ?: Is it an outstanding Reply, i.e. Resolve or Reject?
                        let reply = _outboxReplies[envelope.cmid];
                        if (reply) {
                            let replyEnvelope = reply.envelope;
                            reply.attempt++;
                            if (reply.attempt > 10) {
                                error("toomanyretries", "Upon RETRY-request: Too many attempts at sending [" + replyEnvelope.t + "] with smid:[" + replyEnvelope.smid + "], TraceId[" + replyEnvelope.tid + "], size:[" + JSON.stringify(replyEnvelope).length + "].");
                                continue;
                            }
                            // Note: the retry-cycles will start at attempt=2, since we initialize it with 1, and have already increased it by now.
                            let retryDelay = Math.pow(2, (initiation.attempt - 2)) * 500 + Math.round(Math.random() * 1000);
                            setTimeout(function () {
                                _addEnvelopeToPipeline(replyEnvelope);
                            }, retryDelay);
                        }
                    } else if ((envelope.t === MessageType.ACK) || (envelope.t === MessageType.NACK)) {
                        // -> Server Acknowledges information-bearing message from Client.

                        // ?: Do server want receipt of the RECEIVED-from-client, indicated by the message having 'cmid' property?
                        if (envelope.cmid) {
                            // -> Yes, so send RECEIVED to server
                            _addEnvelopeToPipeline({
                                t: MessageType.ACK2,
                                cmid: envelope.cmid
                            });
                        }

                        // :: Handling if this was an ACK for outstanding SEND or REQUEST
                        let initiation = _outboxInitiations[envelope.cmid];
                        // ?: Check that we found it.
                        if (initiation === undefined) {
                            // -> No, not initiation. Assume it was a Reply, delete the outbox entry.
                            delete _outboxReplies[envelope.cmid];
                            continue;
                        }

                        // E-> Yes, we had an outstanding Initiation (SEND or REQUEST).
                        // Delete it, as we're handling it now
                        delete _outboxInitiations[envelope.cmid];
                        // ?: Was it a ACK (not NACK)?
                        if (envelope.t === MessageType.ACK) {
                            // -> Yes, it was undefined or "ACK" - so Server was happy.
                            if (initiation.ack) {
                                initiation.ack(_createReceivedEvent(ReceivedEventType.ACK, initiation.traceId, initiation.sentTimestamp, receivedTimestamp, performance.now() - initiation.performanceNow));
                            }
                        } else {
                            // -> No, it was "NACK", so message has not been forwarded to Mats
                            if (initiation.nack) {
                                initiation.nack(_createReceivedEvent(ReceivedEventType.NACK, initiation.traceId, initiation.sentTimestamp, receivedTimestamp, performance.now() - initiation.performanceNow));
                            }
                            // ?: ALSO check if it was a REQUEST?
                            let request = _outstandingRequests[envelope.cmid];
                            if (request) {
                                // -> Yes, this was a REQUEST
                                // Then we have to reject the REQUEST too - it was never sent to Mats, and will thus never get a Reply
                                // (Note: This is either a reject for a Promise, or errorCallback on Endpoint).
                                _completeFuture(request, envelope, receivedTimestamp);
                            }
                        }
                    } else if (envelope.t === MessageType.ACK2) {
                        // -> ACKNOWLEDGE of the RECEIVED: We can delete from our inbox
                        if (!envelope.smid) {
                            // -> No, we do not have this. Programming error from app.
                            error("missing smid", "The ACK2 envelope is missing 'smid'", envelope);
                            continue;
                        }
                        // Delete it from inbox - that is what ACK2 means: Other side has now deleted it from outbox,
                        // and can thus not ever deliver it again (so we can delete the guard against double delivery).
                        delete _inbox[envelope.smid];

                    } else if (envelope.t === MessageType.SEND) {
                        // -> SEND: Sever-to-Client Send a message to client terminator

                        if (!envelope.smid) {
                            // -> No, we do not have this. Programming error from app.
                            error("SEND: missing smid", "The SEND envelope is missing 'smid'", envelope);
                            continue;
                        }

                        // Find the (client) Terminator which the Send should go to
                        let terminator = _terminators[envelope.eid];

                        // :: Send receipt unconditionally
                        let ackEnvelope = {
                            t: (terminator ? MessageType.ACK : MessageType.NACK),
                            smid: envelope.smid
                        };
                        if (!terminator) {
                            ackEnvelope.desc = "The Client Terminator [" + envelope.eid + "] does not exist!";
                        }
                        _addEnvelopeToPipeline(ackEnvelope);

                        // ?: Do we have the desired Terminator?
                        if (terminator === undefined) {
                            // -> No, we do not have this. Programming error from app.
                            error("missing client Terminator", "The Client Terminator [" + envelope.eid + "] does not exist!!", envelope);
                            continue;
                        }
                        // E-> We found the Terminator to tell

                        // ?: Have we already gotten this message? (Double delivery)
                        if (_inbox[envelope.smid]) {
                            // -> Yes, so this was a double delivery. Drop processing, we've already done it.
                            log("Caught double delivery of SEND with smid:[" + envelope.smid + "], have sent RECEIVED, but won't process again.", envelope);
                            continue;
                        }

                        // Add the message to inbox
                        _inbox[envelope.smid] = envelope;

                        // Actually invoke the Terminator
                        terminator.resolve(_createMessageEvent(envelope, envelope.t, envelope.smid, envelope.tid, undefined, receivedTimestamp, undefined, envelope.msg));

                    } else if (envelope.t === MessageType.REQUEST) {
                        // -> REQUEST: Server-to-Client Request a REPLY from a client endpoint

                        if (!envelope.smid) {
                            // -> No, we do not have this. Programming error from app.
                            error("REQUEST: missing smid", "The REQUEST envelope is missing 'smid'.", envelope);
                            continue;
                        }

                        // Find the (client) Endpoint which the Request should go to
                        let endpoint = _endpoints[envelope.eid];

                        // :: Send receipt unconditionally
                        let ackEnvelope = {
                            t: (endpoint ? MessageType.ACK : MessageType.NACK),
                            smid: envelope.smid
                        };
                        if (!endpoint) {
                            ackEnvelope.desc = "The Client Endpoint [" + envelope.eid + "] does not exist!";
                        }
                        _addEnvelopeToPipeline(ackEnvelope);

                        // ?: Do we have the desired Terminator?
                        if (endpoint === undefined) {
                            // -> No, we do not have this. Programming error from app.
                            error("missing client Endpoint", "The Client Endpoint [" + envelope.eid + "] does not exist!!", envelope);
                            continue;
                        }
                        // E-> We found the Endpoint to request!

                        // ?: Have we already gotten this message? (Double delivery)
                        if (_inbox[envelope.smid]) {
                            // -> Yes, so this was a double delivery. Drop processing, we've already done it.
                            log("Caught double delivery of REQUEST with smid:[" + envelope.smid + "], have sent RECEIVED, but won't process again.", envelope);
                            continue;
                        }

                        // Add the message to inbox
                        _inbox[envelope.smid] = envelope;

                        // :: Create a Resolve and Reject handler
                        let fulfilled = function (resolveReject, msg) {
                            // Update timestamp of last "information bearing message" sent.
                            _lastMessageEnqueuedTimestamp = Date.now();
                            // Create the Reply message
                            let replyEnvelope = {
                                t: resolveReject,
                                eid: envelope.reid,
                                smid: envelope.smid,
                                tid: envelope.tid,
                                msg: msg
                            };
                            // Add the message Sequence Id
                            replyEnvelope.cmid = _messageSequenceId++;
                            // Add it to outbox
                            _outboxReplies[replyEnvelope.cmid] = {
                                attempt: 1,
                                envelope: replyEnvelope
                            };
                            // Send it over the wire
                            _addEnvelopeToPipeline(replyEnvelope);
                        };

                        // Invoke the Endpoint, getting a Promise back.
                        let promise = endpoint(_createMessageEvent(envelope, envelope.t, envelope.smid, envelope.tid, undefined, receivedTimestamp, undefined, envelope.msg));

                        // Finally attach the Resolve and Reject handler
                        promise.then(function (resolveMessage) {
                            fulfilled(MessageType.RESOLVE, resolveMessage);
                        }).catch(function (rejectMessage) {
                            fulfilled(MessageType.REJECT, rejectMessage);
                        });

                    } else if ((envelope.t === MessageType.RESOLVE) || (envelope.t === MessageType.REJECT)) {
                        // -> Reply to REQUEST
                        // ?: Do server want receipt, indicated by the message having 'smid' property?
                        if (envelope.smid) {
                            // -> Yes, so send RECEIVED to server
                            _addEnvelopeToPipeline({
                                t: MessageType.ACK,
                                smid: envelope.smid
                            });
                        }
                        // It is physically possible that the REPLY comes before the RECEIVED (I've observed it!).
                        // .. Such a situation could potentially be annoying for the using application (Reply before Received)..
                        // ALSO, for REPLYs that are produced in the incomingHandler, there will be no RECEIVED.
                        // Handle this by checking whether the initiation is still in place, and resolve it if so.
                        let initiation = _outboxInitiations[envelope.cmid];
                        // ?: Was the initiation still present?
                        if (initiation) {
                            // -> Yes, still present - so we delete and resolve it
                            delete _outboxInitiations[envelope.cmid];
                            if (initiation.ack) {
                                initiation.ack(_createReceivedEvent(ReceivedEventType.ACK, initiation.traceId, initiation.sentTimestamp, receivedTimestamp, performance.now() - initiation.performanceNow));
                            }
                        }

                        let request = _outstandingRequests[envelope.cmid];
                        if (!request) {
                            if (that.logging) log("Double delivery: Evidently we've already completed the Request for cmid:[" + envelope.cmid + "], traiceId: [" + envelope.tid + "], ignoring.");
                        }

                        // Complete the Promise on a REQUEST-with-Promise, or messageCallback/errorCallback on Endpoint for REQUEST-with-ReplyTo
                        _completeFuture(request, envelope, receivedTimestamp);

                    } else if (envelope.t === MessageType.PING) {
                        // -> PING request, respond with a PONG
                        // Add the PONG reply to pipeline
                        _addEnvelopeToPipeline({
                            t: MessageType.PONG,
                            x: envelope.x
                        });
                        // Send it immediately
                        that.flush();

                    } else if (envelope.t === MessageType.PONG) {
                        // -> Response to a PING
                        let pingPong = _outstandingPings[envelope.x];
                        delete _outstandingPings[envelope.x];
                        pingPong.roundTripMillis = receivedTimestamp - pingPong.sentTimestamp;
                    }
                } catch (err) {
                    error("message", "Got error while handling incoming envelope.cmid [" + envelope.cmid + "], type '" + envelope.t + "': " + JSON.stringify(envelope), err);
                }
            }
        }

        function _createMessageEvent(incomingEnvelope, type, messageId, traceId, sentTimestamp, receivedTimestamp, roundTripMillis, msg) {
            let debug = new DebugInformation(sentTimestamp, incomingEnvelope, receivedTimestamp);

            let messageEvent = new MessageEvent(type.toLowerCase(), msg, traceId, messageId, receivedTimestamp);
            messageEvent.clientRequestTimestamp = sentTimestamp;
            messageEvent.roundTripMillis = roundTripMillis;
            messageEvent.debug = debug;

            return messageEvent;
        }

        function _createReceivedEvent(type, traceId, sentTimestamp, receivedTimestamp, roundTripMillis) {
            return new ReceivedEvent(type, traceId, sentTimestamp, receivedTimestamp, roundTripMillis);
        }

        function _completeFuture(request, incomingEnvelope, receivedTimestamp) {
            // ?: Is this a RequestReplyTo, as indicated by the request having a replyToEndpoint?
            if (request.replyToEndpointId) {
                // -> Yes, this is a REQUEST-with-ReplyTo
                // Find the (client) Terminator which the Reply should go to
                let terminator = _terminators[request.replyToEndpointId];
                // Create the event
                let event = _createMessageEvent(incomingEnvelope, incomingEnvelope.t, incomingEnvelope.cmid, request.envelope.tid, request.sentTimestamp, receivedTimestamp, performance.now() - request.performanceNow, incomingEnvelope.msg);
                // .. add CorrelationInformation from request
                event.correlationInformation = request.correlationInformation;
                if (incomingEnvelope.t === MessageType.RESOLVE) {
                    terminator.resolve(event);
                } else {
                    // ?: Do we actually have a Reject-function (not necessarily, app decides whether to register it)
                    if (terminator.reject) {
                        // -> Yes, so reject it.
                        terminator.reject(event);
                    }
                }
            } else {
                // -> No, this is a REQUEST-with-Promise (missing (client) EndpointId)
                // Delete the outstanding request, as we will complete it now.
                delete _outstandingRequests[incomingEnvelope.cmid];
                // Create the event
                let event = _createMessageEvent(incomingEnvelope, incomingEnvelope.t, incomingEnvelope.cmid, request.envelope.tid, request.sentTimestamp, receivedTimestamp, performance.now() - request.performanceNow, incomingEnvelope.msg);
                // Was it RESOLVE or REJECT?
                if (incomingEnvelope.t === MessageType.RESOLVE) {
                    request.resolve(event);
                } else {
                    request.reject(event);
                }
            }
        }

        function _startPinger() {
            log("Starting PING'er!");
            _pingLater();
        }

        function _stopPinger() {
            log("Cancelling/stopping PINGer");
            if (_pinger_TimeoutId) {
                clearTimeout(_pinger_TimeoutId);
                _pinger_TimeoutId = undefined;
            }
        }

        let _pinger_TimeoutId;

        function _pingLater() {
            _pinger_TimeoutId = setTimeout(function () {
                if (that.logging) log("Ping-'thread': About to send ping. ConnectionState:[" + that.state + "], sessionOpened:[" + _sessionOpened + "].");
                if ((that.state === ConnectionState.SESSION_ESTABLISHED) && _sessionOpened) {
                    let pingId = that.jid(3);
                    if (that.logging) log("Sending PING! PingId:" + pingId);
                    let pingPong = new PingPong(pingId, Date.now());
                    _pings.push(pingPong);
                    if (_pings.length > 100) {
                        _pings.shift();
                    }
                    _outstandingPings[pingId] = pingPong;
                    _webSocket.send("[{\"t\":\"" + MessageType.PING + "\",\"x\":\"" + pingId + "\"}]");
                    // Reschedule
                    _pingLater();
                } else {
                    log("Ping-'thread': NOT Rescheduling due to wrong state or not connected, 'exiting thread'.");
                }
            }, 15000);
        }
    }

    exports.MatsSocket = MatsSocket;

    exports.MessageType = MessageType;
    exports.MatsSocketCloseCodes = MatsSocketCloseCodes;

    exports.ConnectionState = ConnectionState;

    exports.AuthorizationRequiredEvent = AuthorizationRequiredEvent;
    exports.AuthorizationRequiredEventType = AuthorizationRequiredEventType;

    exports.ConnectionEvent = ConnectionEvent;
    exports.ConnectionEventType = ConnectionEventType;

    exports.ReceivedEvent = ReceivedEvent;
    exports.ReceivedEventType = ReceivedEventType;

    exports.MessageEvent = MessageEvent;
    exports.MessageEventType = MessageEventType;
}));