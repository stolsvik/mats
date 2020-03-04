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
    }
}(typeof self !== 'undefined' ? self : this, function (exports, WebSocket) {

    // MatsSocketCloseCodes enum, directly from MatsSocketServer.java
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
         * 4001: From Server side, Client should REJECT all outstanding and should consider "rebooting" the application,
         * in particular if if there was any outstanding requests as their state is now indeterminate: A HELLO:RECONNECT
         * was attempted, but the session was gone. A new session was provided instead. The client application must get
         * its state synchronized with the server side's view of the world, thus the suggestion of "reboot".
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

        // ==============================================================================================
        // PUBLIC:
        // ==============================================================================================

        this.appName = appName;
        this.appVersion = appVersion;
        this.urls = [].concat(urls); // Ensure array

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
         * Invoked when the server kicks us off the socket and the session is closed due to "policy violations", of which there is a multitude, most
         * revolving about protocol not being followed, or the authentication is invalid when being evaluated on the server. No such policy violations
         * should happen if this client is used properly - the only conceivable is that a pipeline took more time to send over to the server
         * than there was left before the authorization expired (i.e. 'roomForLatency' is set too small, compared to the available bandwidth
         * to send the messages in the pipeline).
         * <p/>
         * Note that the MatsSocket session is as closed as if you
         * invoked {@link MatsSocket#close()} on it: Before the supplied event listener function is invoked, all outstanding
         * send/requests are rejected, all request Promises are rejected, and the MatsSocket object is as if just constructed and configured.
         * You may "boot it up again" by sending a new message where you then will get a new MatsSocket Session. However, you should consider restarting the application if this happens, or otherwise "reboot" it
         * by gathering data. Remember that any outstanding "addOrder" request's Promise will now have been rejected - and you don't really know whether
         * the order was placed or not. On the received event, there will be a number detailing the number of outstanding send/requests and Promises that
         * was rejected - if this is zero, you should actually be in sync, and can consider just "act as if nothing happened" - by sending
         * a new message and thus get a new MatsSocket Session going.
         *
         * @param authorizationRevokedCallback
         */
        this.addSessionClosedListener = function (authorizationRevokedCallback) {
            // TODO: implement;
        };

        /**
         * If a re-connect results in a "NEW" Session - not "RECONNECTED" - the registered function will be invoked. This
         * could happen if you e.g. close the lid of a laptop with a webapp running. When you wake it up again,
         * it will start reconnecting to the MatsSocket. Depending on the time slept, you will then get a
         * RECONNECTED if it was within the Session timeout on the server, or NEW if the Session has expired.
         * If NEW, you might want to basically reload the entire webapp - or at least reset the state to as if just booted.
         *
         * TODO: Just special case of ConnectionEventListener?
         *
         * TODO: The SessionLost mechanism on the server side is wrong! We should not reject the NEW requests, only inform that the old ones are dead.
         *
         * @param {function} sessionLostCallback function that will be invoked if we lost session on server side.
         */
        this.addSessionLostListener = function (sessionLostListener) {
            // TODO: Implement
        };

        /**
         * If this MatsSockets client realizes that the 'expirationTime' of the authorization has passed when about to send
         * a message, it will invoke this callback function. A new authorization must then be provided by invoking the
         * 'setCurrentAuthorization' function - only when this is invoked, the MatsSocket will send messages. The
         * MatsSocket will stack up any messages that are sent while waiting for new authorization, and send them all at
         * once when the authorization is in (i.e. it'll pipeline the messages).
         *
         * @param {function} authorizationExpiredCallback function which will be invoked if the current time is more than
         * 'expirationTimeMillisSinceEpoch - roomForLatencyMillis' of the last invocation of 'setCurrentAuthorization' when
         * about to send a new message.
         */
        this.setAuthorizationExpiredCallback = function (authorizationExpiredCallback) {
            _authorizationExpiredCallback = authorizationExpiredCallback;

            // Evaluate whether there are stuff in the pipeline that should be sent now.
            // (Not-yet-sent HELLO does not count..)
            evaluatePipelineSend();
        };

        /**
         * Sets an authorization String, which for several types of authorization must be invoked on a regular basis with
         * fresh authorization - this holds for a OAuth/OIDC-type system where an access token will expire within a short time
         * frame (e.g. expires within minutes). For an Oauth2-style authorization scheme, this could be "Bearer: ......".
         * This must correspond to what the server side authorization plugin expects.
         * <p />
         * <b>NOTE: This SHOULD NOT be used to CHANGE the user!</b> It should only refresh an existing authorization for the
         * initially authenticated user. One MatsSocket (Session) should only be used by a single user: If changing
         * user, you should ditch the existing MatsSocket (preferably invoking 'shutdown' to close the session properly
         * on the server side too), and make a new MatsSocket thus getting a new Session.
         * <p />
         * Note: If the underlying WebSocket has not been established and HELLO sent, then invoking this method will NOT
         * do that - only the first actual MatsSocket message will start the WebSocket and do HELLO.
         *
         * @param authorization the authorization String which will be resolved to a Principal on the server side by the
         * authorization plugin (and which potentially also will be forwarded to other resources that requires
         * authorization).
         * @param expirationTimeMillisSinceEpoch the millis-since-epoch at which this authorization (e.g. JWT access
         * token) expires. Undefined or -1 means "never expires". <i>Notice that in a JWT token, the expiration time is in
         * seconds, not millis: Multiply by 1000.</i>
         * @param roomForLatencyMillis the number of millis which is subtracted from the 'expirationTimeMillisSinceEpoch' to
         * find the point in time where the MatsSocket will "refuse to use" the authorization and instead invoke the
         * 'authorizationExpiredCallback' and wait for a new authorization being set by invocation of this method.
         * Depending on what the usage of the Authorization string is on server side is, this should probably <b>at least</b> be 10000,
         * i.e. 10 seconds - but if the Mats endpoints uses the Authorization string do further accesses, latency and
         * queueing must be taken into account (e.g. for calling into another API that also needs a valid token).
         */
        this.setCurrentAuthorization = function (authorization, expirationTimeMillisSinceEpoch, roomForLatencyMillis) {
            _authorization = authorization;
            _expirationTimeMillisSinceEpoch = expirationTimeMillisSinceEpoch;
            _roomForLatencyMillis = roomForLatencyMillis;
            _sendAuthorizationToServer = true;
            _authExpiredCallbackInvoked = false;

            log("Got Authorization with expirationTimeMillisSinceEpoch: " + expirationTimeMillisSinceEpoch + "," +
                "roomForLatencyMillis: " + roomForLatencyMillis, authorization);

            // Evaluate whether there are stuff in the pipeline that should be sent now.
            // (Not-yet-sent HELLO does not count..)
            evaluatePipelineSend();
        };

        /**
         * The listener is informed about connection events.
         *
         * TODO: Implement.
         *
         * @param connectionEventListener
         */
        this.addConnectionEventListener = function (connectionEventListener) {

        };

        /**
         * This can be used by the mechanism invoking 'setCurrentAuthorization(..)' to decide whether it should keep the
         * authorization fresh (i.e. no latency waiting for new authorization is introduced when a new message is
         * enqueued), or fall back to relying on the 'authorizationExpiredCallback' being invoked when a new message needs
         * it (thus introducing latency while waiting for authorization). One could envision keeping fresh auth for 5
         * minutes, but if the user has not done anything requiring server comms in that timespan, you stop doing auth
         * refresh until he again does something and the 'authorizationExpiredCallback' is invoked.
         *
         * @returns {number} millis-since-epoch of last message enqueued.
         */
        this.getLastMessageEnqueuedTimestamp = function () {
            return _lastMessageEnqueuedMillis;
        };

        /**
         * @param endpointId the id of this client side endpoint.
         * @param messageCallback receives an Event when everything went OK, containing the message on the "data" property.
         * @param errorCallback is relevant if this endpoint is set as the replyTo-target on a requestReplyTo(..) invocation, and will
         * get invoked with the Event if the corresponding Promise-variant would have been rejected.
         */
        this.endpoint = function (endpointId, messageCallback, errorCallback) {
            // :: Assert for double-registrations
            if (_endpoints[endpointId] !== undefined) {
                throw new Error("Cannot register more than one endpoint to same endpointId [" + endpointId + "], existing: " + _endpoints[endpointId]);
            }
            log("Registering endpoint on id [" + endpointId + "]:\n #messageCallback: " + messageCallback + "\n #errorCallback: " + errorCallback);
            _endpoints[endpointId] = {
                resolve: messageCallback,
                reject: errorCallback
            };
        };

        /**
         * <b>YOU SHOULD PROBABLY NOT USE THIS!</b>, instead using the specific prototype methods for generating messages.
         * Add a message to the outgoing pipeline, evaluates whether to send the pipeline afterwards (i.e. if pipelining
         * is active or not).
         */
        this.addEnvelopeToPipeline = function (envelope) {
            _lastMessageEnqueuedMillis = Date.now();
            _pipeline.push(envelope);
            if (this.logging) log("Pushed to pipeline: " + JSON.stringify(envelope));
            evaluatePipelineLater()
        };

        this.addSendOrRequestEnvelopeToPipeline = function (envelope, ack, nack, requestResolve, requestReject) {
            let type = envelope.t;

            if ((type !== "SEND") && (type !== "REQUEST")) {
                throw Error("Only SEND and REQUEST messages can be enqueued via this method.")
            }

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

            this.addEnvelopeToPipeline(envelope);
        };

        let _evaluatePipelineLater_timeout = undefined;

        function evaluatePipelineLater() {
            if (_evaluatePipelineLater_timeout) {
                clearTimeout(_evaluatePipelineLater_timeout);
            }
            _evaluatePipelineLater_timeout = setTimeout(function () {
                evaluatePipelineSend();
                _evaluatePipelineLater_timeout = undefined;
            }, 2)
        }


        /**
         * Flush any pipelined messages, "synchronously" in that it does not wait for next 'tick'.
         */
        this.flush = function () {
            if (_evaluatePipelineLater_timeout) {
                clearTimeout(_evaluatePipelineLater_timeout);
                _evaluatePipelineLater_timeout = undefined;
            }
            evaluatePipelineSend();
        };

        /**
         * Do not randomize the provided WebSocket URLs. Should only be used for testing, as you definitely want
         * randomization of which URL to connect to - it will always go for the 0th element first.
         */
        this.disableUrlRandomization = function () {
            _useUrls = this.urls;
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
            let currentWsUrl = currentWebSocketUrl();
            let existingSessionId = _sessionId;
            let webSocket = _webSocket;
            log("close(): Closing MatsSocketSession, id:[" + existingSessionId + "] due to [" + reason
                + "], currently connected: [" + (_webSocket ? _webSocket.url : "not connected") + "]");

            // :: Remove beforeunload eventlistener
            deregisterBeforeunload();

            // :: Clear all state of this MatsSocket.
            clearStateAndPipelineAndFuturesAndOutstandingMessages("client close session");

            // :: In-band session close
            closeWebSocket(webSocket, existingSessionId, MatsSocketCloseCodes.CLOSE_SESSION, reason);

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
            closeWebSocket(_webSocket, _sessionId, MatsSocketCloseCodes.RECONNECT, "client close session");
        };

        function closeWebSocket(webSocket, sessionId, closeCodeNumber, reason) {
            // ?: Do we have WebSocket?
            let closeCodeName = MatsSocketCloseCodes.nameFor(closeCodeNumber);
            if (webSocket) {
                // -> Yes, so close WebSocket with MatsSocket-specific CloseCode 4000.
                log(" \\-> WebSocket is open, so we close it with MatsSocketCloseCode " + closeCodeName + "(" + closeCodeNumber + ").");
                // Perform the close
                webSocket.close(closeCodeNumber, reason);
            } else if (sessionId) {
                log(" \\-> WebSocket NOT open, so CANNOT close it with MatsSocketCloseCode " + closeCodeName + "(" + closeCodeNumber + ").");
            } else {
                log(" \\-> Missing both WebSocket and MatsSocketSessionId, so CANNOT close it  with MatsSocketCloseCode " + closeCodeName + "(" + closeCodeNumber + ").");
            }
        }

        /**
         * Convenience method for making random strings for correlationIds (choose e.g. length=10) and
         * "add-on to traceId to make it pretty unique" (choose length=6).
         *
         * @param length how long the string should be: 10 for correlationId, 6 for "added to traceId".
         * @returns {string} from digits, lower and upper case letters - 62 entries.
         */
        this.id = function (length) {
            let result = '';
            for (let i = 0; i < length; i++) {
                result += _alphabet[Math.floor(Math.random() * _alphabet.length)];
            }
            return result;
        };

        // ==============================================================================================
        // PRIVATE
        // ==============================================================================================

        function error(type, msg, err) {
            if (err) {
                console.error(type + ": " + msg, err);
            } else {
                console.error(type + ": " + msg);
            }
        }

        function log() {
            if (that.logging) {
                console.log.apply(console, arguments);
            }
        }

        function invokeLater(that) {
            setTimeout(that, 0);
        }

        // https://stackoverflow.com/a/12646864/39334
        function shuffleArray(array) {
            for (let i = array.length - 1; i > 0; i--) {
                let j = Math.floor(Math.random() * (i + 1));
                let temp = array[i];
                array[i] = array[j];
                array[j] = temp;
            }
        }

        // The URLs to use - will be shuffled. Can be reset to not randomized by this.disableUrlRandomize()
        let _useUrls = [].concat(this.urls);
        // Shuffle the URLs
        shuffleArray(_useUrls);

        // alphabet length: 10 + 26 x 2 = 62.
        let _alphabet = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

        // fields
        let _sessionId = undefined;
        let _pipeline = [];
        let _endpoints = {};

        let _helloSent = false;
        let _reconnect_ForceSendHello = false;

        let _authorization = undefined;
        let _sendAuthorizationToServer = false;
        let _expirationTimeMillisSinceEpoch = undefined;
        let _roomForLatencyMillis = undefined;
        let _authorizationExpiredCallback = undefined;
        let _lastMessageEnqueuedMillis = Date.now(); // Start by assuming that it was just used.

        let _messageSequenceId = 0; // Increases for each SEND or REQUEST

        // When we've informed the app that we need auth, we do not need to do it again until it has set it.
        let _authExpiredCallbackInvoked = false;

        // Outstanding Futures resolving the Promises
        const _futures = {};
        // Outstanding SEND and RECEIVE messages waiting for Received ACK/ERROR/NACK
        const _outstandingSendsAndRequests = {};

        // "That" reference
        const that = this;

        function beforeunloadHandler() {
            that.close(clientLibNameAndVersion + " window.onbeforeunload close");
        }

        function registerBeforeunload() {
            // ?: Is the self object an EventTarget (ie. window in a Browser context)
            if (typeof (self) === 'object' && typeof (self.addEventListener) === 'function') {
                // Yes -> Add "onbeforeunload" event listener to shut down the MatsSocket cleanly (closing session) when user navigates away from page.
                self.addEventListener("beforeunload", beforeunloadHandler);
            }
        }

        function deregisterBeforeunload() {
            if (typeof (self) === 'object' && typeof (self.addEventListener) === 'function') {
                self.removeEventListener("beforeunload", beforeunloadHandler);
            }
        }

        const userAgent = (typeof (self) === 'object' && typeof (self.navigator) === 'object') ? self.navigator.userAgent : "Unknown";

        function clearStateAndPipelineAndFuturesAndOutstandingMessages(reason) {
            if (_webSocket) {
                // We don't want the onclose callback invoked from this event that we initiated ourselves.
                _webSocket.onclose = undefined;
                // We don't want any messages either, as we'll now be clearing out futures and outstanding messages ("acks")
                _webSocket.onmessage = undefined;
                // Also drop onerror for good measure.
                _webSocket.onerror = undefined;
            }
            _webSocket = undefined;
            _sessionId = undefined;
            _urlIndexCurrentlyConnecting = 0;

            // :: Clear pipeline
            _pipeline.length = 0;

            // :: Reject all outstanding messages
            for (let cmid in _outstandingSendsAndRequests) {
                if (!_outstandingSendsAndRequests.hasOwnProperty(cmid)) continue;

                let outstandingMessage = _outstandingSendsAndRequests[cmid];
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
                if (!_futures.hasOwnProperty(cmid)) continue;

                let future = _futures[cmid];
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
         * Sends pipelined messages if pipelining is not engaged.
         */
        function evaluatePipelineSend() {
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
                let e1 = {
                    type: "NOT_PRESENT"
                };
                // We will have asked for auth after next line.
                _authExpiredCallbackInvoked = true;
                // Ask for auth
                _authorizationExpiredCallback(e1);
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
                let e2 = {
                    type: "EXPIRED",
                    currentAuthorizationExpirationTime: _expirationTimeMillisSinceEpoch
                };
                // We will have asked for auth after next line.
                _authExpiredCallbackInvoked = true;
                // Ask for auth
                _authorizationExpiredCallback(e2);
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
                initiateWebSocketCreation();
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
                    an: that.appName,
                    av: that.appVersion,
                    auth: _authorization,
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
                // (As opposed to initial connection, where we do not send before having an actual message in pipeline)
                _reconnect_ForceSendHello = false;
            }

            // :: Send pre-pipeline messages, if there are any
            // (Before the HELLO is sent and sessionId is established, the max size of message is low on the server)
            if (prePipeline) {
                if (that.logging) log("Flushing prePipeline of [" + prePipeline.length + "] messages.");
                _webSocket.send(JSON.stringify(prePipeline));
                prePipeline.length = 0;
            }
            // :: Send any pipelined messages.
            if (_pipeline.length > 0) {
                if (that.logging) log("Flushing pipeline of [" + _pipeline.length + "] messages.");
                _webSocket.send(JSON.stringify(_pipeline));
                // Clear pipeline
                _pipeline.length = 0;
            }
        }

        function eventFromEnvelope(envelope, receivedTimestamp) {
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

        // Two variables for state of socket opening, used by 'evaluatePipelineSend()'
        // If true, we're currently already trying to get a WebSocket
        let _webSocketConnecting = false;
        // If not undefined, we have an open WebSocket available.
        let _webSocket = undefined;

        let _urlIndexCurrentlyConnecting = 0; // Cycles through the URLs
        let _connectionAttemptRound = 0; // When cycled one time through URLs, increases.
        let _connectionTimeoutBase = 500; // Milliseconds for this fallback level. Doubles, up to max defined below.
        let _connectionTimeoutMax = 10000; // Milliseconds max between connection attempts.

        function currentWebSocketUrl() {
            log("## Using urlIndexCurrentlyConnecting [" + _urlIndexCurrentlyConnecting + "]: " + _useUrls[_urlIndexCurrentlyConnecting] + ", round:" + _connectionAttemptRound);
            return _useUrls[_urlIndexCurrentlyConnecting];
        }

        function initiateWebSocketCreation() {
            // ?: Assert that we do not have the WebSocket already
            if (_webSocket !== undefined) {
                // -> Damn, we did have a WebSocket. Why are we here?!
                throw (new Error("Should not be here, as WebSocket is already in place, or we're trying to connect"));
            }

            // E-> No, WebSocket ain't open, so fire it up

            // We are trying to connect
            _webSocketConnecting = true;

            let increaseStateVars = function () {
                _urlIndexCurrentlyConnecting++;
                if (_urlIndexCurrentlyConnecting >= _useUrls.length) {
                    _urlIndexCurrentlyConnecting = 0;
                    _connectionAttemptRound++;
                }
            };

            // Based on whether there is multiple URLs, or just a single one, we choose the short "timeout base", or a longer one (max/2), as minimum.
            let minTimeout = _useUrls.length > 1 ? _connectionTimeoutBase : _connectionTimeoutMax / 2;
            // Timeout: LESSER of "max" and "timeoutBase * (2^round)", which should lead to timeoutBase x1, x2, x4, x8 - but capped at max.
            // .. but at least 'minTimeout'
            let timeout = Math.max(minTimeout,
                Math.min(_connectionTimeoutMax, _connectionTimeoutBase * Math.pow(2, _connectionAttemptRound)));

            // Create the WebSocket
            let websocket = socketFactory(currentWebSocketUrl(), "matssocket");

            // Make a "connection timeout" thingy, which bumps the state vars, and then re-runs this method.
            let timeoutId = setTimeout(function (event) {
                log("Create WebSocket: Timeout exceeded [" + timeout + "], this WebSocket is bad so ditch it.");
                // :: Bad attempt, clear this WebSocket out
                // Clear out the handlers
                websocket.onopen = undefined;
                websocket.onerror = undefined;
                websocket.onclose = undefined;
                // Close it
                websocket.close(4999, "timeout hit");
                // Invoke on next tick: Bump state vars, re-run initiateWebSocketCreation
                invokeLater(function () {
                    increaseStateVars();
                    initiateWebSocketCreation()
                });
            }, timeout);

            // :: Add the handlers for this "trying to acquire" procedure.

            // Error: Just log for debugging. NOTICE: Upon Error, Close is also always invoked.
            websocket.onerror = function (event) {
                log("Create WebSocket: error.", event);
            };

            // Close: Log + IF this is the first "round" AND there is multiple URLs, then immediately try the next URL. (Close may happen way before the Connection Timeout)
            websocket.onclose = function (closeEvent) {
                log("Create WebSocket: close. Code:" + closeEvent.code + ", Reason:" + closeEvent.reason, closeEvent);
                // ?: If we are on the FIRST (0th) round of trying out the different URLs, then immediately try the next
                // But only if there are more than one
                if ((_connectionAttemptRound === 0) && (_useUrls.length > 1)) {
                    // Drop the "connection timeout" thingy
                    clearTimeout(timeoutId);
                    // Invoke on next tick: Bump state vars, re-run initiateWebSocketCreation
                    invokeLater(function () {
                        increaseStateVars();
                        initiateWebSocketCreation()
                    });
                }
            };

            // Open: Success! Cancel timeout, and set WebSocket in MatsSocket, clear flags, set proper handlers
            websocket.onopen = function (event) {
                log("Create WebSocket: opened!", event);
                // Drop the "connection timeout" thingy
                clearTimeout(timeoutId);

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

                registerBeforeunload();

                // Fire off any waiting messages, next tick
                invokeLater(function () {
                    log("Running evaluatePipelineSend()..");
                    evaluatePipelineSend();
                });
            };
        }

        function _onerror(event) {
            error("websocket.onerror", "Got 'onerror' error", event);
        }

        function _onclose(event) {
            log("websocket.onclose", event);
            // Ditch this WebSocket
            _webSocket = undefined;

            // ?: Special codes, that signifies that we should close (terminate) the MatsSocketSession.
            if ((event.code === MatsSocketCloseCodes.PROTOCOL_ERROR)
                || (event.code === MatsSocketCloseCodes.VIOLATED_POLICY)
                || (event.code === MatsSocketCloseCodes.UNEXPECTED_CONDITION)
                || (event.code === MatsSocketCloseCodes.CLOSE_SESSION)
                || (event.code === MatsSocketCloseCodes.SESSION_LOST)) {
                // -> One of the specific "Session is closed" CloseCodes -> Reject all outstanding, this MatsSocket is trashed.
                log("We were closed with one of the MatsSocketCloseCode:[" + MatsSocketCloseCodes.nameFor(event.code) + "] that denotes that we should close the MatsSocketSession, reason:[" + event.reason + "].");
                clearStateAndPipelineAndFuturesAndOutstandingMessages(event.reason);
                // TODO: Pingback application!
            } else {
                // -> NOT one of the specific "Session is closed" CloseCodes -> Reconnect and Reissue all outstanding..
                log("We were closed with a CloseCode [" + MatsSocketCloseCodes.nameFor(event.code) + "] that does not denote that we should close the session. Initiate reconnect and reissue all outstanding.");

                // :: This is a reconnect - so we should do pipeline processing right away, to get the HELLO over.
                _reconnect_ForceSendHello = true;

                // :: Start reconnecting, but give the server a little time to settle
                setTimeout(function () {
                    initiateWebSocketCreation();

                    // TODO: Implement reissue!

                }, 200);
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
                        // TODO: Handle WELCOME message better. At least notify if sessionLost..
                        _sessionId = envelope.sid;
                        if (that.logging) log("We're WELCOME! Session:" + envelope.st + ", SessionId:" + _sessionId);
                    } else if (envelope.t === "RECEIVED") {
                        // -> RECEIVED-message-from-client (ack/nack/retry/error)

                        // ?: Do server want receipt of the RECEIVED-from-client, indicated by the message having 'cmid' property?
                        if (envelope.cmid && (envelope.st !== "ERROR")) {
                            // -> Yes, so send RECEIVED to server
                            that.addEnvelopeToPipeline({
                                t: "ACKACK",
                                cmid: envelope.cmid,
                            });
                        }

                        let outstandingSendOrRequest = _outstandingSendsAndRequests[envelope.cmid];
                        // ?: Check that we found it.
                        if (outstandingSendOrRequest === undefined) {
                            // -> No, the OutstandingSendOrRequest was not present.
                            // (Either already handled by fast REPLY, or was an insta-settling in IncomingAuthorizationAndAdapter, which won't send RECEIVED)
                            continue;
                        }
                        // E-> Yes, we had OutstandingSendOrRequest
                        // Delete it, as we're handling it now
                        delete _outstandingSendsAndRequests[envelope.cmid];
                        // ?: Was it a "good" RECEIVED?
                        if ((envelope.st === undefined) || (envelope.st === "ACK")) {
                            // -> Yes, it was undefined or "ACK" - so Server was happy.
                            if (outstandingSendOrRequest.ack) {
                                outstandingSendOrRequest.ack(eventFromEnvelope(envelope, receivedTimestamp));
                            }
                        } else {
                            // -> No, it was "ERROR" or "NACK", so message has not been forwarded to Mats
                            if (outstandingSendOrRequest.nack) {
                                outstandingSendOrRequest.nack(eventFromEnvelope(envelope, receivedTimestamp));
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
                    } else if (envelope.t === "ACKACK") {
                        // -> ACKNOWLEDGE of the RECEIVED: We can delete from our inbox
                        // NOTICE: Comes into play with Server-side SEND and REQUEST. Not yet.
                    } else if (envelope.t === "SEND") {
                        // -> SEND: Send message to terminator
                        // ?: Do server want receipt, indicated by the message having 'smid' property?
                        if (envelope.smid) {
                            // -> Yes, so send RECEIVED to server
                            that.addEnvelopeToPipeline({
                                t: "RECEIVED",
                                st: "ACK",
                                smid: envelope.smid,
                            });
                        }
                        // TODO: Implement SEND

                    } else if (envelope.t === "REQUEST") {
                        // -> REQUEST: Request a REPLY from an endpoint
                        // ?: Do server want receipt, indicated by the message having 'smid' property?
                        if (envelope.smid) {
                            // -> Yes, so send RECEIVED to server
                            that.addEnvelopeToPipeline({
                                t: "RECEIVED",
                                st: "ACK",
                                smid: envelope.smid,
                            });
                        }
                        // TODO: Implement REQUEST

                    } else if (envelope.t === "REPLY") {
                        // -> Reply to REQUEST
                        // ?: Do server want receipt, indicated by the message having 'smid' property?
                        if (envelope.smid) {
                            // -> Yes, so send RECEIVED to server
                            that.addEnvelopeToPipeline({
                                t: "RECEIVED",
                                st: "ACK",
                                smid: envelope.smid,
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
                                outstandingSendOrRequest.ack(eventFromEnvelope(envelope, receivedTimestamp));
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
                    future.resolve(eventFromEnvelope(envelope, receivedTimestamp));
                } else {
                    future.reject(eventFromEnvelope(envelope, receivedTimestamp));
                }
            } else {
                // -> No, this is a REQUEST-with-ReplyTo
                // Find the (client) Endpoint which the Reply should go to
                let endpoint = _endpoints[endpointId];
                // ?: Do we not have it?
                if (endpoint === undefined) {
                    // -> No, we do not have this. Programming error from app.
                    // TODO: Should catch this upon the requestReplyTo(...) invocation.
                    error("missing client endpoint", "The Client Endpoint [" + envelope.eid + "] is not present!", envelope);
                    return;
                }
                // E-> We found the endpoint to tell
                if (envelope.st === "RESOLVE") {
                    endpoint.resolve(eventFromEnvelope(envelope, receivedTimestamp));
                } else if (endpoint.reject) {
                    endpoint.reject(eventFromEnvelope(envelope, receivedTimestamp));
                }
            }
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
}));