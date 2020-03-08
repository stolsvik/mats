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
        root.MatsSocket = root.mats.MatsSocket;
        root.MatsSocketCloseCodes = root.mats.MatsSocketCloseCodes;
        root.ConnectionState = root.mats.ConnectionState;
        root.ConnectionEvent = root.mats.ConnectionEvent;
    }
}(typeof self !== 'undefined' ? self : this, function (exports, WebSocket) {

    /**
     * MatsSocketCloseCodes enum, directly from MatsSocketServer.java
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

        nameFor: function (closeCode) {
            let keys = Object.keys(MatsSocketCloseCodes).filter(function (key) {
                return MatsSocketCloseCodes[key] === closeCode
            });
            if (keys.length === 1) {
                return keys[0];
            }
            return "UNKNOWN(" + closeCode + ")";
        }
    });

    /**
     * State and Event names for {@link MatsSocket#state} and {@link MatsSocket#addConnectionEventListener(function)}.
     *
     * @type {Readonly<{COUNTDOWN: string, NO_SESSION: string, CONNECTING: string, WAITING: string, CONNECTION_ERROR: string, CONNECTED: string, SESSION_ESTABLISHED: string, LOST_CONNECTION: string}>}
     */
    const ConnectionState = Object.freeze({
        /**
         * State only - this is the initial state, unless we've gotten a CloseSession situation (which is communicated
         * via listeners registered with {@link MatsSocket#addSessionClosedEventListener(listener)}), where the state
         * is reset back to NO_SESSION.
         * <p/>
         * Only transition out of this state is into {@link #CONNECTING}.
         */
        NO_SESSION: "Not Connected",

        /**
         * State, and fires as ConnectionEvent when we transition into this state, which is when the WebSocket is literally trying to connect.
         * This is between <code>new WebSocket(url)</code> and either webSocket.onopen or webSocket.onclose is
         * fired. If webSocket.onopen, we transition into {@link #CONNECTED}, if webSocket.onclose, we transition into
         * {@link #WAITING}.
         * <p/>
         * User Info Tip: Show a info-box, stating "Connecting..", and you may show the countdown number if "grayed out". Each time it transitions into CONNECTING, it will
         * start a new countdown, from say 4 seconds. If the connection attempt fails after 1 second, it will transition
         * into WAITING and continue the countdown with 3 seconds reminging.
         * <p/>
         * User Info Tip: Show a info-box, stating "Connecting! <4 seconds..>", countdown in "grayed out" style, box is some neutral color, e.g. yellow (fading over to this color if already red or orange due to {@link #CONNECTION_ERROR} or {@link #LOST_CONNECTION}).
         */
        CONNECTING: "Connecting",

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
         * User Info Tip: Show a info-box, stating "Waiting! <2.9 seconds..>", countdown in normal visibility, box is some neutral color, e.g. yellow (keeping the box color fading if in progress).
         */
        WAITING: "Waiting",

        /**
         * State, and fires as ConnectionEvent when we transition into this state, which is when WebSocket.onopen is fired.
         * <p/>
         * Notice that the {@link ConnectionEvent} contains the {@link Event} that came with webSocket.open.
         * <p/>
         * User Info Tip: Show a info-box, stating "Connected!", happy-color, e.g. green, with no countdown. Notice that the {@link ConnectionEvent} contains the WebSocket 'onopen' {@link Event} that was issued when
         * the WebSocket opened.
         */
        CONNECTED: "Connected",

        /**
         * State, and fires as ConnectionEvent when we transition into this state, which is when when the WELCOME MatsSocket message comes
         * from the Server, also implying that it has been authenticated - now actual messages can be exchanged.
         * <p/>
         * User Info Tip: Show a info-box, stating "Session OK!", happy-color, e.g. green, with no countdown - and the info-box fades out fast after 1 second.
         */
        SESSION_ESTABLISHED: "Session Established",

        /**
         * Event only. This is a pretty worthless event. It comes from WebSocket.onerror. It will <i>always</i> be trailed by a
         * WebSocket.onclose, which for MatsSocket ends up as {@link #LOST_CONNECTION}.
         * <p/>
         * Notice that the {@link ConnectionEvent} contains the {@link Event} that caused the error.
         * <p/>
         * User Info Tip: Show a info-box, which is some reddish color (no need for text since next event {@link #LOST_CONNECTION}) comes immediately).
         */
        CONNECTION_ERROR: "Connection Error",

        /**
         * Event only. This comes when WebSocket.onclose is fired - <b>and it is NOT a SessionClosed Event</b>. The latter will
         * instead invoke the listeners registered with {@link MatsSocket#addSessionClosedEventListener(listener)}.
         * <p/>
         * Notice that the {@link ConnectionEvent} contains the {@link CloseEvent} that caused the lost connection.
         * <p/>
         * User Info Tip: Show a info-box, stating "Connection broken!", which is some orange color (unless it already is red due to {@link #CONNECTION_ERROR}), fading over to the next color when next event ({@link #CONNECTING} comes in.
         */
        LOST_CONNECTION: "Lost Connection",

        /**
         * Event only. Notice that you will most probably not get an event with 0 seconds, as that is when we transition into
         * {@link #CONNECTING} and the countdown starts over (possibly with a larger timeout). Read more at {@link ConnectionEvent#countdownSeconds}.
         * <p/>
         * User Info Tip: Read more at {@link #CONNECTING} and {@linl #WAITING}.
         */
        COUNTDOWN: "Countdown"
    });


    /**
     * Event object for {@link MatsSocket#addConnectionEventListener(function)}.
     *
     * @param {string} connectionState
     * @param {string} webSocketUrl
     * @param {Event} webSocketEvent
     * @param {number} timeoutSeconds
     * @param {number} countdownSeconds
     * @constructor
     */
    function ConnectionEvent(connectionState, webSocketUrl, webSocketEvent, timeoutSeconds, countdownSeconds) {
        /**
         * Return the enum value of {@link ConnectionState}.
         *
         * @type {string}
         */
        this.state = connectionState;

        /**
         * For all of the events this holds the current URL we're either connected to, was connected to, or trying to
         * connect to.
         *
         * @type {string}
         */
        this.webSocketUrl = webSocketUrl;

        /**
         * For several of the types (enumerated in {@link ConnectionState}), there is an underlying WebSocket event
         * that caused it. This field holds that.
         * <ul>
         *     <li>{@link ConnectionState#WAITING}: WebSocket {@link CloseEvent} that caused this transition.</li>
         *     <li>{@link ConnectionState#CONNECTED}: WebSocket {@link Event} that caused this transition.</li>
         *     <li>{@link ConnectionState#CONNECTION_ERROR}: WebSocket {@link Event} that caused this transition.</li>
         *     <li>{@link ConnectionState#LOST_CONNECTION}: WebSocket {@link CloseEvent} that caused it.</li>
         * </ul>
         *
         * @type {Event}
         */
        this.webSocketEvent = webSocketEvent;

        /**
         * For {@link ConnectionState#CONNECTING}, {@link ConnectionState#WAITING} and {@link ConnectionState#COUNTDOWN},
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
         * For {@link ConnectionState#CONNECTING}, {@link ConnectionState#WAITING} and {@link ConnectionState#COUNTDOWN},
         * tells how many seconds there are left for this attempt (of the {@link #timeoutSeconds} it started with),
         * with a tenth of a second as precision. With the COUNTDOWN events, these come in each 100 ms (1/10 second),
         * and show how long time there is left before trying again (if MatsSocket is configured with multiple URLs,
         * the next attempt will be a different URL).
         * <p/>
         * The countdown is started when the state transitions to {@link ConnectionState#CONNECTING}, and
         * stops either when {@link ConnectionState#CONNECTED} or the timeout reaches zero. If the
         * state is still CONNECTING when the countdown reaches zero, implying that the "new WebSocket(..)" call still
         * has not either opened or closed, the connection attempt is aborted by calling webSocket.close(). It then
         * tries again, possibly with a different URL - and the countdown starts over.
         * <p/>
         * Notice that the countdown is not affected by any state transition into {@link ConnectionState#WAITING} -
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
     * Synonym for {@link ConnectionEvent#state}, to kinda adhere to the "Event" contract. Returns the enum value of
     * {@link ConnectionState}.
     *
     * @member {string} codeName
     * @memberOf ConnectionEvent
     * @readonly
     */
    Object.defineProperty(ConnectionEvent.prototype, "type", {
        get: function () {
            return this.state;
        }
    });

    /**
     * Hack the built-in CloseEvent to include a "codeName" property resolving to the MatsSocket-relevant close codes.
     * Return the name of the CloseCode, e.g. "SESSION_LOST".
     *
     * @member {string} codeName
     * @memberOf CloseEvent
     * @readonly
     */
    if (typeof CloseEvent !== 'undefined') {
        // ^ guard against node, which evidently does not have CloseEvent..
        Object.defineProperty(CloseEvent.prototype, "codeName", {
            get: function () {
                return MatsSocketCloseCodes.nameFor(this.code);
            }
        });
    }

    /**
     * A "holding struct" for pings - you may get the latest pings (with experienced latency) from the property
     * {@link MatsSocket#pings}.
     *
     * @param {string} correlationId
     * @param {number} sentTimestamp
     * @constructor
     */
    function PingPong(correlationId, sentTimestamp) {
        /**
         * Pretty meaningless information, if logged server side, you can possibly correlate outliers.
         *
         * @type {string} the correlationId used by the client lib to correlate outstanding pings with incoming pongs.
         */
        this.correlationId = correlationId;

        /**
         * Millis-from-epoch when this ping was sent.
         *
         * @type {number} which this ping was sent, millis-from-epoch.
         */
        this.sentTimestamp = sentTimestamp;

        /**
         * The experienced latency for this ping-pong, remember that this is the time back-and-forth.
         *
         * @type {number} the number of milliseconds from ping sent to pong received.
         */
        this.latency = undefined;
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
            }
        }
        if (typeof socketFactory !== "function") {
            throw new Error("socketFactory should be a function, instead got [" + socketFactory + "]");
        }

        const that = this;
        const userAgent = (typeof (self) === 'object' && typeof (self.navigator) === 'object') ? self.navigator.userAgent : "Unknown";

        // Ensure that 'urls' is an array or 1 or several URLs.
        urls = [].concat(urls);


        // ==============================================================================================
        // PUBLIC:
        // ==============================================================================================

        this.logging = false; // Whether to log via console.log

        /**
         * If this is set to a function, it will be invoked if close(..) is invoked and sessionId is present, the
         * argument being a object with two keys:
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
         * <b>Note: You <i>should</i> register a SessionClosedEvent listener, as it means that you've lost sync with the
         * server, and you should crash or "reboot" the application employing to library to regain sync.</b>
         * <p/>
         * The registered event listener functions are called when the server kicks us off the socket and the session is
         * closed due to a multitude of reasons, where most should never happen if you use the library correctly, in
         * particular wrt. authentication.
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
         * Note that when this event listener is invoked, the MatsSocket session is as closed as if you invoked
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
         * @param expirationTimeMillisSinceEpoch the millis-since-epoch at which this authorization (e.g. JWT access
         * token) expires. -1 means "never expires". <i>Notice that in a JWT token, the expiration time is in
         * seconds, not millis: Multiply by 1000.</i>
         * @param roomForLatencyMillis the number of millis which is subtracted from the 'expirationTimeMillisSinceEpoch' to
         * find the point in time where the MatsSocket will refuse to use the authorization and instead invoke the
         * 'authorizationExpiredCallback' and wait for a new authorization being set by invocation of the present method.
         * Depending on what the usage of the Authorization string is on server side is, this should probably <b>at least</b> be 10000,
         * i.e. 10 seconds - but if the Mats endpoints uses the Authorization string to do further accesses, both latency
         * and queue time must be taken into account (e.g. for calling into another API that also needs a valid token).
         */
        this.setCurrentAuthorization = function (authorizationHeader, expirationTimeMillisSinceEpoch, roomForLatencyMillis) {
            _authorization = authorizationHeader;
            _expirationTimeMillisSinceEpoch = expirationTimeMillisSinceEpoch;
            _roomForLatencyMillis = roomForLatencyMillis;
            _sendAuthorizationToServer = true;
            _authExpiredCallbackInvoked = false;

            if (this.logging) log("Got Authorization which "
                + (expirationTimeMillisSinceEpoch !== -1 ? "Expires in [" + (expirationTimeMillisSinceEpoch - Date.now()) + " ms]" : "[Never expires]")
                + ", roomForLatencyMillis: " + roomForLatencyMillis);

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
         * Effectively the same as <code>({@link #state} === {@link ConnectionState#CONNECTED}) || ({@link #state} === {@link ConnectionState#SESSION_ESTABLISHED})</code>.
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
         * <b>YOU SHOULD PROBABLY NOT USE THIS!</b>, instead using the specific prototype methods for generating messages.
         * Add a message to the outgoing pipeline, evaluates whether to send the pipeline afterwards (i.e. if pipelining
         * is active or not).
         */
        this.addSendOrRequestEnvelopeToPipeline = function (envelope, ack, nack, requestResolve, requestReject) {
            let type = envelope.t;

            if ((type !== "SEND") && (type !== "REQUEST")) {
                throw Error("Only SEND and REQUEST messages can be enqueued via this method.")
            }
            // Update timestamp of last "information bearing message" sent.
            _lastMessageEnqueuedTimestamp = Date.now();

            // Add client timestamp.
            // TODO: How necessary is this?
            envelope.cmcts = Date.now();

            // Make lambda for what happens when it has been RECEIVED on server.
            let outstandingSendOrRequest = Object.create(null);
            // Store the outgoing message
            outstandingSendOrRequest.envelope = envelope;
            // Store acn/nack callbacks
            outstandingSendOrRequest.ack = ack;
            outstandingSendOrRequest.nack = nack;
            outstandingSendOrRequest.requestResolve = requestResolve;
            outstandingSendOrRequest.requestReject = requestReject;
            outstandingSendOrRequest.retransmitGuard = _outstandingSendAndRequest_RetransmitGuard;

            // Add the message Sequence Id
            let thisMessageSequenceId = _messageSequenceId++;
            envelope.cmid = thisMessageSequenceId;

            // Store the outstanding Send or Request
            _outstandingSendsAndRequests[thisMessageSequenceId] = outstandingSendOrRequest;

            // If it is a REQUEST /without/ a specific Reply (client) Endpoint defined, then it is a ...
            let requestWithPromiseCompletion = (type === "REQUEST") && (envelope.reid === undefined);
            // ?: If this is a requestWithPromiseCompletion, we'll have a future to look forward to.
            if (requestWithPromiseCompletion) {
                _futures[thisMessageSequenceId] = {
                    resolve: requestResolve,
                    reject: requestReject,
                    envelope: envelope
                };
            }

            _addEnvelopeToPipeline(envelope);
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
            let existingSessionId = _sessionId;
            log("close(): Closing MatsSocketSession, id:[" + existingSessionId + "] due to [" + reason
                + "], currently connected: [" + (_webSocket ? _webSocket.url : "not connected") + "]");

            // :: Close WebSocket itself.
            // ?: Do we have _webSocket?
            if (_webSocket) {
                // -> Yes, so close WebSocket with MatsSocket-specific CloseCode CLOSE_SESSION 4000.
                log(" \\-> WebSocket is open, so we close it with MatsSocketCloseCode CLOSE_SESSION (4000).");
                // Perform the close
                _webSocket.close(MatsSocketCloseCodes.CLOSE_SESSION, "From client: " + reason);
            }

            // Clear out WebSocket "infrastructure", i.e. state and "pinger thread".
            _clearWebSocketStateAndInfrastructure();
            // Clear Session and all state of this MatsSocket.
            _clearSessionAndStateAndPipelineAndFuturesAndOutstandingMessages("From client: " + reason);

            // :: Out-of-band session close
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
                    } else {
                        log("  \\- Was about to do out-of-band (i.e. HTTP) close_session, but we had no sessionId (HELLO/WELCOME not yet performed).");
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
                + ")', MatsSocketSessionId:[" + _sessionId + "] due to [" + reason + "], currently connected: [" + (_webSocket ? _webSocket.url : "not connected") + "]");
            if (!_webSocket) {
                throw new Error("There is no live WebSocket to close with RECONNECT closeCode!");
            }
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

        function log() {
            if (that.logging) {
                console.log.apply(console, arguments);
            }
        }

        function error(type, msg, err) {
            if (err) {
                console.error(type + ": " + msg, err);
            } else {
                console.error(type + ": " + msg);
            }
        }

        // alphabet length: 10 + 26 x 2 = 62 chars.
        let _alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        // JSON-non-quoted and visible Alphabet: 92 chars.
        let _jsonAlphabet = "!#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{|}~";

        // The URLs to use - will be shuffled. Can be reset to not randomized by this.disableUrlRandomize()
        let _useUrls = [].concat(urls);
        // Shuffle the URLs
        _shuffleArray(_useUrls);

        // fields
        let _sessionId = undefined;
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
        let _sendAuthorizationToServer = false;
        let _expirationTimeMillisSinceEpoch = undefined;
        let _roomForLatencyMillis = undefined;
        let _authorizationExpiredCallback = undefined;
        let _lastMessageEnqueuedTimestamp = Date.now(); // Start by assuming that it was just used.

        let _messageSequenceId = 0; // Increases for each SEND, REQUEST and REPLY

        // When we've informed the app that we need auth, we do not need to do it again until it has set it.
        let _authExpiredCallbackInvoked = false;

        // Outstanding Futures resolving the Promises
        const _futures = Object.create(null);
        // Outstanding SEND and RECEIVE messages waiting for Received ACK/ERROR/NACK
        const _outstandingSendsAndRequests = Object.create(null);
        // .. a "guard object" to avoid having to retransmit messages sent /before/ the HELLO/WELCOME handshake
        let _outstandingSendAndRequest_RetransmitGuard = this.jid(5);
        // Outstanding REPLYs
        const _outstandingReplies = Object.create(null);
        // Outstanding Pings
        const _outstandingPings = Object.create(null);

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
            // Stop pinger
            _stopPinger();
            // Remove beforeunload eventlistener
            _deregisterBeforeunload();
            // Reset Reconnect state vars
            _resetReconnectStateVars();
            // Make new RetransmitGuard - so that any previously "guarded" messages now will be retransmitted.
            _outstandingSendAndRequest_RetransmitGuard = that.jid(5);
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

        function _clearSessionAndStateAndPipelineAndFuturesAndOutstandingMessages(reason) {
            // Clear state
            _sessionId = undefined;
            _state = ConnectionState.NO_SESSION;

            // :: Clear pipeline
            _pipeline.length = 0;

            // :: Reject all outstanding messages
            for (let cmid in _outstandingSendsAndRequests) {
                // noinspection JSUnfilteredForInLoop: Using Object.create(null)
                let outstandingMessage = _outstandingSendsAndRequests[cmid];
                // noinspection JSUnfilteredForInLoop: Using Object.create(null)
                delete _outstandingSendsAndRequests[cmid];

                log("Clearing outstanding [" + outstandingMessage.envelope.t + "] to [" + outstandingMessage.envelope.eid + "] with traceId [" + outstandingMessage.envelope.tid + "].");
                if (outstandingMessage.nack) {
                    try {
                        // TODO: Make better event object
                        outstandingMessage.nack({type: "CLEARED", description: reason});
                    } catch (err) {
                        error("Got error while clearing outstanding [" + outstandingMessage.envelope.t + "] to [" + outstandingMessage.envelope.eid + "].", err);
                    }
                }
            }

            // :: Reject all futures
            for (let cmid in _futures) {
                // noinspection JSUnfilteredForInLoop: Using Object.create(null)
                let future = _futures[cmid];
                // noinspection JSUnfilteredForInLoop: Using Object.create(null)
                delete _futures[cmid];

                log("Clearing REQUEST future [" + future.envelope.t + "] to [" + future.envelope.eid + "] with traceId [" + future.envelope.tid + "].");
                if (future.reject) {
                    try {
                        // TODO: Make better event object
                        future.reject({type: "CLEARED", description: reason});
                    } catch (err) {
                        error("Got error while clearing REQUEST future to [" + future.envelope.eid + "].", err);
                    }
                }
            }
        }

        /**
         * Unconditionally adds the supplied envelope to the pipeline, and then evaluates the pipeline,
         * invokeLater-style so as to get "auth-pipelining". Use flush() to get sync send.
         */
        function _addEnvelopeToPipeline(envelope) {
            _pipeline.push(envelope);
            if (that.logging) log("Pushed to pipeline: " + JSON.stringify(envelope));
            _evaluatePipelineLater()
        }

        let _evaluatePipelineLater_timeoutId = undefined;

        function _evaluatePipelineLater() {
            if (_evaluatePipelineLater_timeoutId) {
                clearTimeout(_evaluatePipelineLater_timeoutId);
            }
            _evaluatePipelineLater_timeoutId = setTimeout(function () {
                _evaluatePipelineSend();
                _evaluatePipelineLater_timeoutId = undefined;
            }, 2)
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
                // -> Yes, authorization is expired.
                // ?: Have we already asked app for new auth?
                if (_authExpiredCallbackInvoked) {
                    // -> Yes, so just return.
                    log("Authorization was not present, but we've already asked app for it.");
                    return;
                }
                // E-> No, not asked for auth - so do it.
                log("Authorization was not present. Need this to continue. Invoking callback..");
                let event = {
                    type: "NOT_PRESENT"
                };
                // We will have asked for auth after next line.
                _authExpiredCallbackInvoked = true;
                // Ask for auth
                _authorizationExpiredCallback(event);
                return;
            }
            // ?: Check whether we have expired authorization
            if ((_expirationTimeMillisSinceEpoch !== undefined) && (_expirationTimeMillisSinceEpoch !== -1)
                && ((_expirationTimeMillisSinceEpoch - _roomForLatencyMillis) < Date.now())) {
                // -> Yes, authorization is expired.
                // ?: Have we already asked app for new auth?
                if (_authExpiredCallbackInvoked) {
                    // -> Yes, so just return.
                    log("Authorization was expired, but we've already asked app for new.");
                    return;
                }
                // E-> No, not asked for new - so do it.
                log("Authorization was expired. Need new to continue. Invoking callback..");
                let event = {
                    type: "EXPIRED",
                    currentAuthorizationExpirationTime: _expirationTimeMillisSinceEpoch
                };
                // We will have asked for auth after next line.
                _authExpiredCallbackInvoked = true;
                // Ask for auth
                _authorizationExpiredCallback(event);
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
                    t: "HELLO",
                    clv: clientLibNameAndVersion + "; User-Agent: " + userAgent,
                    ts: Date.now(),
                    an: appName,
                    av: appVersion,
                    auth: _authorization,  // This is guaranteed to be in place and valid, see above
                    cid: that.id(10),
                    tid: "MatsSocket_start_" + that.id(6)
                };
                // ?: Have we requested a reconnect?
                if (_sessionId !== undefined) {
                    log("HELLO not send, adding to pre-pipeline. HELLO:RECONNECT to MatsSocketSessionId:[" + _sessionId + "]");
                    // -> Evidently yes, so add the requested reconnect-to-sessionId.
                    helloMessage.sid = _sessionId;
                    // This implementation of MatsSocket client lib expects existing session
                    // when reconnecting, thus wants pipelined messages to be ditched if
                    // the assumption about existing session fails.
                    helloMessage.st = "RECONNECT";
                } else {
                    // -> We want a new session (which is default anyway)
                    log("HELLO not sent, adding to pre-pipeline. HELLO:NEW, we will get assigned a MatsSocketSessionId upon WELCOME.");
                    helloMessage.st = "NEW";
                }
                // Add the HELLO to the prePipeline
                prePipeline = [];
                prePipeline.unshift(helloMessage);
                // We will now have sent the HELLO, so do not send it again.
                _helloSent = true;
                // .. and again, we will now have sent the HELLO - If this was a RECONNECT, we have now done the immediate reconnect HELLO.
                // (As opposed to initial connection, where we do not send before having an actual information bearing message in pipeline)
                _reconnect_ForceSendHello = false;
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

        // Two variables for state of socket opening, used by 'evaluatePipelineSend()'
        // If true, we're currently already trying to get a WebSocket
        let _webSocketConnecting = false;
        // If not undefined, we have an open WebSocket available.
        let _webSocket = undefined;

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
            if (that.logging && ((connectionEvent.state !== ConnectionState.COUNTDOWN)
                || (connectionEvent.countdownSeconds === connectionEvent.timeoutSeconds)
                || (Math.round(connectionEvent.countdownSeconds) === connectionEvent.countdownSeconds))) {
                log("Sending ConnectionEvent to listeners", connectionEvent);
            }
            // ?: Is it one of the states we're notifying about?
            if (connectionEvent.state === ConnectionState.CONNECTING
                || connectionEvent.state === ConnectionState.WAITING
                || connectionEvent.state === ConnectionState.CONNECTED
                || connectionEvent.state === ConnectionState.SESSION_ESTABLISHED) {
                // -> Yes, this is a state - so update the state..!
                _state = connectionEvent.state;
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

            // E-> No, WebSocket ain't open, so fire it up

            // We are trying to connect
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
            _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionState.CONNECTING, _currentWebSocketUrl(), undefined, _secondsTenths(timeout), secondsLeft()));

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
                        _initiateWebSocketCreation()
                    }, Math.round(Math.random() * 200));
                } else {
                    // -> No, we've NOT hit target, so sleep till next countdown target, where we re-invoke ourselves (this countDownTimer())
                    // Notify ConnectionEvent listeners about this COUNTDOWN event.
                    _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionState.COUNTDOWN, _currentWebSocketUrl(), undefined, _secondsTenths(timeout), secondsLeft()));
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
                _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionState.WAITING, _currentWebSocketUrl(), closeEvent, _secondsTenths(timeout), secondsLeft()));

                // ?: If we are on the FIRST (0th) round of trying out the different URLs, then immediately try the next
                // .. But only if there are multiple URLs configured.
                if ((_connectionAttemptRound === 0) && (_useUrls.length > 1)) {
                    // -> YES, we are on the 0th round of connection attempts, and there are multiple URLs, so immediately try the next.
                    // Cancel the "connection timeout" thingy
                    clearTimeout(countdownId);
                    // Invoke on next tick: Bump state vars, re-run _initiateWebSocketCreation
                    _invokeLater(function () {
                        increaseReconnectStateVars();
                        _initiateWebSocketCreation()
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

                _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionState.CONNECTED, _currentWebSocketUrl(), event, undefined, undefined));

                // Fire off any waiting messages, next tick
                _invokeLater(function () {
                    log("We're open for business! Running evaluatePipelineSend()..");
                    _evaluatePipelineSend();
                });
            };
        }

        function _onerror(event) {
            error("websocket.onerror", "Got 'onerror' error", event);
            // :: Synchronously notify our ConnectionEvent listeners.
            _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionState.CONNECTION_ERROR, _currentWebSocketUrl(), event, undefined, undefined));
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
                _clearSessionAndStateAndPipelineAndFuturesAndOutstandingMessages("From Server: " + closeEvent.reason);

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
                log("We were closed with a CloseCode [" + MatsSocketCloseCodes.nameFor(closeEvent.code) + "] that does not denote that we should close the session. Initiate reconnect and reissue all outstanding.");

                // :: This is a reconnect - so we should do pipeline processing right away, to get the HELLO over.
                _reconnect_ForceSendHello = true;

                // :: Synchronously notify our ConnectionEvent listeners.
                _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionState.LOST_CONNECTION, _currentWebSocketUrl(), closeEvent, undefined, undefined));

                // :: Start reconnecting, but give the server a little time to settle
                setTimeout(function () {
                    _initiateWebSocketCreation();
                }, 200 + Math.round(Math.random() * 200));
            }
        }

        function _onmessage(webSocketEvent) {
            // // ?: Is this message received on the current '_webSocket' instance (i.e. different (reconnect), or cleared/closed)?
            // if (this !== _webSocket) {
            //     // -> NO! This received-message is not on the current _webSocket instance.
            //     // We just drop the messages on the floor, as nobody is waiting for them anymore:
            //     // If we closed with outstanding messages or futures, they were rejected.
            //     // NOTE: This happens all the time on the node.js integration tests, triggering the if-missing-
            //     // outstandingSendOrRequest error-output below.
            //     return;
            // }
            let receivedTimestamp = Date.now();
            let data = webSocketEvent.data;
            let envelopes = JSON.parse(data);

            let numEnvelopes = envelopes.length;
            if (that.logging) log("onmessage: Got " + numEnvelopes + " messages.");

            for (let i = 0; i < numEnvelopes; i++) {
                let envelope = envelopes[i];
                try {
                    if (that.logging) log(" \\- onmessage: handling message " + i + ": " + envelope.t + ", envelope:" + JSON.stringify(envelope));

                    // TODO: Handle RETRY!

                    if (envelope.t === "WELCOME") {
                        _sessionId = envelope.sid;
                        // :: Synchronously notify our ConnectionEvent listeners.
                        _updateStateAndNotifyConnectionEventListeners(new ConnectionEvent(ConnectionState.SESSION_ESTABLISHED, _currentWebSocketUrl(), undefined, undefined, undefined));
                        // Start pinger
                        _startPinger();
                        if (that.logging) log("We're WELCOME! Session:" + envelope.st + ", SessionId:" + _sessionId + ", there are [" + Object.keys(_outstandingSendsAndRequests).length + "] outstanding sends-or-requests, and [" + Object.keys(_outstandingReplies).length + "] outstanding replies.");

                        // TODO: Test this outstanding-stuff! Both that they are actually sent again, and that server handles the (quite possible) double-delivery.

                        // ::: If we have stuff in our outboxes, we need to send them again

                        // :: Outstanding Replies - these are just stored as the plain envelope
                        // NOTICE: On initial connect: Since we cannot possibly have replied to anything BEFORE we get the WELCOME, we do not need RetransmitGuard for Replies
                        // Loop over them
                        for (let key in _outstandingReplies) {
                            // noinspection JSUnfilteredForInLoop: Using Object.create(null)
                            let replyEnvelope = _outstandingReplies[key];
                            // NOTICE: Won't delete it here - that is done when we process the ACK from server
                            _addEnvelopeToPipeline(replyEnvelope);
                            // Flush for each message, in case the size of the message was of issue why we closed (maybe pipeline was too full).
                            that.flush();
                        }

                        // :: Outstanding SENDs and REQUESTs - these are container objects, having the envelope as a key
                        for (let key in _outstandingSendsAndRequests) {
                            // noinspection JSUnfilteredForInLoop: Using Object.create(null)
                            let outstandingSendOrRequest = _outstandingSendsAndRequests[key];
                            let sendOrRequestEnvelope = outstandingSendOrRequest.envelope;
                            // ?: Is the RetransmitGuard the same as we currently have?
                            if (outstandingSendOrRequest.retransmitGuard === _outstandingSendAndRequest_RetransmitGuard) {
                                // -> Yes, so it makes little sense in sending these messages again just yet.
                                if (that.logging) log("The outstandingSendOrRequest with cmid:["+sendOrRequestEnvelope.cmid+"] and TraceId:["+sendOrRequestEnvelope.tid
                                    +"] was created with the same RetransmitGuard as we currently have ["+_outstandingSendAndRequest_RetransmitGuard+"], thus we don't have to retransmit (at least not yet - not until broken connection).");
                                continue;
                            }
                            // NOTICE: Won't delete it here - that is done when we process the ACK from server
                            _addEnvelopeToPipeline(sendOrRequestEnvelope);
                            // Flush for each message, in case the size of the message was of issue why we closed (maybe pipeline was too full).
                            that.flush();
                        }

                    } else if (envelope.t === "RECEIVED") {
                        // -> RECEIVED-message-from-client (ack/nack/retry/error)

                        // ?: Do server want receipt of the RECEIVED-from-client, indicated by the message having 'cmid' property?
                        if (envelope.cmid && (envelope.st !== "ERROR")) {
                            // -> Yes, so send RECEIVED to server
                            _addEnvelopeToPipeline({
                                t: "ACKACK",
                                cmid: envelope.cmid,
                            });
                        }

                        // If this was an ACK for a REPLY from us, delete the outbox:
                        delete _outstandingReplies[envelope.cmid];

                        // :: Handling if this was an ACK for outstanding SEND or REQUEST
                        let outstandingSendOrRequest = _outstandingSendsAndRequests[envelope.cmid];
                        // ?: Check that we found it.
                        if (outstandingSendOrRequest !== undefined) {
                            // -> Yes, the OutstandingSendOrRequest was present
                            // (Either already handled by fast REPLY, or was an insta-settling in IncomingAuthorizationAndAdapter, which won't send RECEIVED)
                            // E-> Yes, we had OutstandingSendOrRequest
                            // Delete it, as we're handling it now
                            delete _outstandingSendsAndRequests[envelope.cmid];
                            // ?: Was it a "good" RECEIVED?
                            if ((envelope.st === undefined) || (envelope.st === "ACK")) {
                                // -> Yes, it was undefined or "ACK" - so Server was happy.
                                if (outstandingSendOrRequest.ack) {
                                    outstandingSendOrRequest.ack(_eventFromEnvelope(envelope, receivedTimestamp));
                                }
                            } else {
                                // -> No, it was "ERROR" or "NACK", so message has not been forwarded to Mats
                                if (outstandingSendOrRequest.nack) {
                                    outstandingSendOrRequest.nack(_eventFromEnvelope(envelope, receivedTimestamp));
                                }
                                // ?: Check if it was a REQUEST
                                if (outstandingSendOrRequest.envelope.t === "REQUEST") {
                                    // -> Yes, this was a REQUEST
                                    // Then we have to reject the REQUEST too - it was never sent to Mats, and will thus never get a Reply
                                    // (Note: This is either a reject for a Promise, or errorCallback on Endpoint).
                                    let requestEnvelope = outstandingSendOrRequest.envelope;
                                    _completeFuture(requestEnvelope.reid, "REJECT", envelope, receivedTimestamp);
                                }
                            }
                        }
                    } else if (envelope.t === "ACKACK") {
                        // -> ACKNOWLEDGE of the RECEIVED: We can delete from our inbox
                        if (!envelope.smid) {
                            // -> No, we do not have this. Programming error from app.
                            error("ACKACK: missing smid", "The ACKACK envelope is missing 'smid'", envelope);
                            continue;
                        }
                        // Delete it from inbox - that is what ACKACK means: Other side has now deleted it from outbox,
                        // and can thus not ever deliver it again (so we can delete the guard against double delivery).
                        delete _inbox[envelope.smid];

                    } else if (envelope.t === "PONG") {
                        // -> Response to a PING
                        let pingPong = _outstandingPings[envelope.cid];
                        delete _outstandingPings[envelope.cid];
                        pingPong.latency = receivedTimestamp - pingPong.sentTimestamp;

                    } else if (envelope.t === "SEND") {
                        // -> SEND: Send message to terminator

                        if (!envelope.smid) {
                            // -> No, we do not have this. Programming error from app.
                            error("SEND: missing smid", "The SEND envelope is missing 'smid'", envelope);
                            continue;
                        }

                        // Find the (client) Terminator which the Send should go to
                        let terminator = _terminators[envelope.eid];

                        // :: Send receipt unconditionally
                        let ackEnvelope = {
                            t: "RECEIVED",
                            st: (terminator !== undefined ? "ACK" : "NACK"),
                            smid: envelope.smid,
                        };
                        if (!terminator) {
                            ackEnvelope.desc = "The Client Terminator [" + envelope.eid + "] does not exist!"
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
                        terminator.resolve(_eventFromEnvelope(envelope, receivedTimestamp));

                    } else if (envelope.t === "REQUEST") {
                        // -> REQUEST: Request a REPLY from an endpoint

                        if (!envelope.smid) {
                            // -> No, we do not have this. Programming error from app.
                            error("REQUEST: missing smid", "The REQUEST envelope is missing 'smid'.", envelope);
                            continue;
                        }

                        // Find the (client) Endpoint which the Request should go to
                        let endpoint = _endpoints[envelope.eid];

                        // :: Send receipt unconditionally
                        let ackEnvelope = {
                            t: "RECEIVED",
                            st: (endpoint !== undefined ? "ACK" : "NACK"),
                            smid: envelope.smid,
                        };
                        if (!endpoint) {
                            ackEnvelope.desc = "The Client Endpoint [" + envelope.eid + "] does not exist!"
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
                                t: "REPLY",
                                st: resolveReject,
                                eid: envelope.reid,
                                smid: envelope.smid,
                                tid: envelope.tid,
                                msg: msg
                            };
                            // Add the message Sequence Id
                            replyEnvelope.cmid = _messageSequenceId++;
                            // Add it to outbox
                            _outstandingReplies[replyEnvelope.cmid] = replyEnvelope;
                            // Send it over the wire
                            _addEnvelopeToPipeline(replyEnvelope);
                        };

                        // Invoke the Endpoint, getting a Promise back.
                        let promise = endpoint(_eventFromEnvelope(envelope, receivedTimestamp));

                        // Finally attach the Resolve and Reject handler
                        promise.then(function (resolveMessage) {
                            fulfilled("RESOLVE", resolveMessage);
                        }).catch(function (rejectMessage) {
                            fulfilled("REJECT", rejectMessage);
                        });

                    } else if (envelope.t === "REPLY") {
                        // -> Reply to REQUEST
                        // ?: Do server want receipt, indicated by the message having 'smid' property?
                        if (envelope.smid) {
                            // -> Yes, so send RECEIVED to server
                            _addEnvelopeToPipeline({
                                t: "RECEIVED",
                                st: "ACK",
                                smid: envelope.smid
                            });
                        }
                        // It is physically possible that the REPLY comes before the RECEIVED (I've observed it!).
                        // .. Such a situation could potentially be annoying for the using application (Reply before Received)..
                        // ALSO, for REPLYs that are produced in the incomingHandler, there will be no RECEIVED.
                        // Handle this by checking whether the outstandingSendOrRequest is still in place, and resolve it if so.
                        let outstandingSendOrRequest = _outstandingSendsAndRequests[envelope.cmid];
                        // ?: Was the outstandingSendOrRequest still present?
                        if (outstandingSendOrRequest) {
                            // -> Yes, still present - so we delete and resolve it
                            delete _outstandingSendsAndRequests[envelope.cmid];
                            if (outstandingSendOrRequest.ack) {
                                outstandingSendOrRequest.ack(_eventFromEnvelope(envelope, receivedTimestamp));
                            }
                        }

                        // Complete the Promise on a REQUEST-with-Promise, or messageCallback/errorCallback on Endpoint for REQUEST-with-ReplyTo
                        _completeFuture(envelope.eid, envelope.st, envelope, receivedTimestamp);
                    }
                } catch (err) {
                    error("message", "Got error while handling incoming envelope.cmid [" + envelope.cmid + "], type '" + envelope.t + (envelope.st ? ":" + envelope.st : "") + ": " + JSON.stringify(envelope), err);
                }
            }
        }

        function _eventFromEnvelope(envelope, receivedTimestamp) {
            return {
                data: envelope.msg,
                type: envelope.t,
                subType: envelope.st,
                traceId: envelope.tid,
                correlationId: envelope.cid,
                messageSequenceId: envelope.cmid,

                // Timestamps and handling nodenames (pretty much debug information).
                clientMessageCreated: envelope.cmcts,
                clientMessageReceived: envelope.cmrts,
                clientMessageReceivedNodename: envelope.cmrnn,
                matsMessageSent: envelope.mmsts,
                matsMessageReplyReceived: envelope.mmrrts,
                matsMessageReplyReceivedNodename: envelope.mmrrnn,
                replyMessageToClient: envelope.mscts,
                replyMessageToClientNodename: envelope.mscnn,
                messageReceivedOnClient: receivedTimestamp
            };
        }

        function _completeFuture(endpointId, resolveOrReject, envelope, receivedTimestamp) {
            // ?: Is this a REQUEST-with-Promise?
            if (endpointId === undefined) {
                // -> Yes, REQUEST-with-Promise (missing (client) EndpointId)
                // Get the outstanding future
                let future = _futures[envelope.cmid];
                // ?: Did we have a future?
                if (future === undefined) {
                    // -> No, missing future, no Promise. Pretty strange, really (error in this code..)
                    error("missing future", "When getting a reply to a Promise, we did not find the future for message sequence [" + webSocketEvent.messageSequenceId + "].", webSocketEvent);
                    return;
                }
                // E-> We found the future
                // Delete the outstanding future, as we will complete it now.
                delete _futures[envelope.cmid];
                // Was it RESOLVE or REJECT?
                if (resolveOrReject === "RESOLVE") {
                    future.resolve(_eventFromEnvelope(envelope, receivedTimestamp));
                } else {
                    future.reject(_eventFromEnvelope(envelope, receivedTimestamp));
                }
            } else {
                // -> No, this is a REQUEST-with-ReplyTo
                // Find the (client) Endpoint which the Reply should go to
                let terminator = _terminators[endpointId];
                // ?: Do we not have it?
                if (terminator === undefined) {
                    // -> No, we do not have this. Programming error from app.
                    // TODO: Should catch this upon the requestReplyTo(...) invocation.
                    error("missing client terminator", "The Client Endpoint [" + envelope.eid + "] is not present!", envelope);
                    return;
                }
                // E-> We found the terminator to tell
                if (envelope.st === "RESOLVE") {
                    terminator.resolve(_eventFromEnvelope(envelope, receivedTimestamp));
                } else if (terminator.reject) {
                    terminator.reject(_eventFromEnvelope(envelope, receivedTimestamp));
                }
            }
        }

        function _startPinger() {
            _pingLater();
        }

        function _stopPinger() {
            if (_pinger_TimeoutId) {
                clearTimeout(_pinger_TimeoutId);
                _pinger_TimeoutId = undefined;
            }
        }

        let _pinger_TimeoutId;

        function _pingLater() {
            _pinger_TimeoutId = setTimeout(function () {
                if ((that.state === ConnectionState.SESSION_ESTABLISHED) && that.connected) {
                    let correlationId = that.jid(3);
                    if (that.logging) log("Sending PING! CorrelationId:" + correlationId);
                    let pingPong = new PingPong(correlationId, Date.now());
                    _pings.push(pingPong);
                    if (_pings.length > 100) {
                        _pings.shift();
                    }
                    _outstandingPings[correlationId] = pingPong;
                    _webSocket.send("[{\"t\":\"PING\",\"cid\":\"" + correlationId + "\"}]");
                    _pingLater();
                }
            }, 15000);
        }
    }

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
    MatsSocket.prototype.send = function (endpointId, traceId, message) {
        let that = this;
        return new Promise(function (resolve, reject) {
            that.addSendOrRequestEnvelopeToPipeline({
                t: "SEND",
                eid: endpointId,
                tid: traceId,
                msg: message
            }, resolve, reject);
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
    MatsSocket.prototype.request = function (endpointId, traceId, message, receivedCallback) {
        let that = this;
        return new Promise(function (resolve, reject) {
            that.addSendOrRequestEnvelopeToPipeline({
                t: "REQUEST",
                eid: endpointId,
                tid: traceId,
                msg: message
            }, receivedCallback, receivedCallback, resolve, reject);
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
     * @param replyToEndpointId
     * @param correlationId
     * @returns {Promise<unknown>}
     */
    MatsSocket.prototype.requestReplyTo = function (endpointId, traceId, message, replyToEndpointId, correlationId) {
        let that = this;
        return new Promise(function (resolve, reject) {
            that.addSendOrRequestEnvelopeToPipeline({
                t: "REQUEST",
                eid: endpointId,
                reid: replyToEndpointId,
                cid: correlationId,
                tid: traceId,
                msg: message
            }, resolve, reject);
        });
    };

    exports.MatsSocket = MatsSocket;
    exports.MatsSocketCloseCodes = MatsSocketCloseCodes;
    exports.ConnectionState = ConnectionState;
    exports.ConnectionEvent = ConnectionEvent;
}));