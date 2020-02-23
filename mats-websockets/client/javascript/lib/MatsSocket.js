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

        // PUBLIC:

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
         * Note that replace is replace-first, and that any 's' in 'wss' results in 'https'.
         *
         * @type {Function}
         */
        this.outofbandclose = undefined;

        /**
         * Only set this if you explicitly want to continue a previous Session. In an SPA where the MatsSocket is
         * instantiated upon "boot" of the SPA, you probably do not want to set this, as you rather want the new Session
         * provided by the server. You cannot invent a SessionId, it will always originate from the server - this facility
         * is to reconnect to an existing Session that was lost. Note: In case of connection failures, the MatsSocket will
         * reconnect on its own.
         *
         * @param sessionId the SessionId of the Session that you expect to be still living on the server.
         */
        this.setSessionId = function (sessionId) {
            _sessionId = sessionId;
        };

        /**
         * Sets an authorization String, which for several types of authorization must be invoked on a regular basis with
         * fresh authorization - this holds for a OIDC-type system where an access token will expire within a short time
         * frame (e.g. expires within minutes). For an Oauth2-style authorization scheme, this could be "Bearer: ......".
         * This must correspond to what the server side authorization plugin expects.
         * <p />
         * <b>NOTE: This SHOULD NOT be used to CHANGE the user!</b> It should only refresh an existing authorization for the
         * initially authenticated user. One MatsSocket (Session) should only be used by a single user: If changing
         * user, you should ditch the existing MatsSocket (preferably invoking 'shutdown' to close the session properly
         * on the server side too), and make a new MatsSocket thus getting a new Session.
         * <p />
         * Note: If the underlying WebSocket has not been established and HELLO sent, then invoking this method will do
         * that.
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
         * Depending on what the usage of the authorization is on server side is, this should probably at least be 10000,
         * i.e. 10 seconds.
         */
        this.setCurrentAuthorization = function (authorization, expirationTimeMillisSinceEpoch, roomForLatencyMillis) {
            _authorization = authorization;
            _expirationTimeMillisSinceEpoch = expirationTimeMillisSinceEpoch;
            _roomForLatencyMillis = roomForLatencyMillis;
            _sendAuthorizationToServer = true;
            _authExpiredCallbackInvoked = false;

            log("Got Authorization with expirationTimeMillisSinceEpoch: " + expirationTimeMillisSinceEpoch + "," +
                "roomForLatencyMillis: " + roomForLatencyMillis, authorization);

            evaluatePipelineSend();
        };

        /**
         * If this MatsSockets client realizes that the 'expirationTime' of the authorization has passed when about to send
         * a message (except PINGs, which do not need fresh auth), it will invoke this callback function. A new
         * authorization must then be provided by invoking the 'setCurrentAuthorization' function - only when this is
         * invoked, the MatsSocket will send messages. The MatsSocket will stack up any messages that are sent while waiting
         * for new authorization, and send them all at once once the authorization is in (i.e. it'll "pipeline" the
         * messages).
         *
         * @param {function} authorizationExpiredCallback function which will be invoked if the current time is more than
         * 'expirationTimeMillisSinceEpoch - roomForLatencyMillis' of the last invocation of 'setCurrentAuthorization' when
         * about to send a new message.
         */
        this.setAuthorizationExpiredCallback = function (authorizationExpiredCallback) {
            _authorizationExpiredCallback = authorizationExpiredCallback;
        };

        /**
         * Invoked when the server kicks us off the socket due to authorization no longer being valid (e.g. logged out via
         * different channel, e.g. HTTP, or otherwise doing revocation). The Socket will then be closed. Invoking
         * setCurrentAuthorization will again open it, sending all pipelined messages.
         *
         * TODO: Just a special case of AuthorizationExpiredCallback?! "AuthorizationInvalidCallback" as generic term?
         *
         * @param authorizationRevokedCallback
         */
        this.setAuthorizationRevokedCallback = function (authorizationRevokedCallback) {
            // TODO: implement;
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
         * If a reconnect results in a "NEW" Session - not "RECONNECTED" - the registered function will be invoked. This
         * could happen if you e.g. close the lid of a laptop with a webapp running. When you wake it up again,
         * it will start reconnecting to the MatsSocket. Depending on the time slept, you will then get a
         * RECONNECTED if it was within the Session timeout on the server, or NEW if the Session has expired.
         * If NEW, you might want to basically reload the entire webapp - or at least reset the state to as if just booted.
         *
         * TODO: Just special case of ConnectionEventListener?
         *
         * @param {function} sessionLostCallback function that will be invoked if we lost session on server side.
         */
        this.addSessionLostListener = function (sessionLostListener) {
            // TODO: Implement
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
        this.addMessageToPipeline = function (type, envelope, resolve, reject, receiveCallback) {
            let now = Date.now();
            _lastMessageEnqueuedMillis = now;

            // Add common params to envelope
            envelope.t = type;
            envelope.cmcts = now;

            // ?: Send or Request?
            if ((type === "SEND") || (type === "REQUEST")) {
                // If it is a REQUEST /without/ a specific Reply (client) Endpoint defined, then it is a ...
                let requestWithPromiseCompletion = (type === "REQUEST") && (envelope.reid === undefined);
                // Make lambda for what happens when it has been RECEIVED on server.
                let outstandingSendOrRequest = {
                    // Store the outgoing message
                    envelope: envelope,
                    // Common for RECEIVED->ERROR or ->NACK is that the Promise will be rejected.
                    reject: reject
                };
                // ?: Is it an ordinary REQUEST with Promise-completion?
                if (requestWithPromiseCompletion) {
                    // -> Yes, ordinary REQUEST with Promise-completion
                    // Upon RECEIVED->ACK, invoke receiveCallback
                    outstandingSendOrRequest.resolve = receiveCallback;
                } else {
                    // -> No, it is SEND or REQUEST-with-ReplyTo.
                    // Upon RECEIVED->ACK, resolve the Promise with the Received-acknowledgement
                    outstandingSendOrRequest.resolve = resolve;
                }

                outstandingSendOrRequest.reject = reject;

                // Add the message Sequence Id
                let thisMessageSequenceId = _messageSequenceId++;
                envelope.cmseq = thisMessageSequenceId;
                // Store the outstanding Send or Request
                _outstandingSendsAndRequests[thisMessageSequenceId] = outstandingSendOrRequest;

                // ?: If this is a requestWithPromiseCompletion, we'll have a future to look forward to.
                if (requestWithPromiseCompletion) {
                    _futures[thisMessageSequenceId] = {
                        resolve: resolve,
                        reject: reject,
                        envelope: envelope
                    };
                }
            }

            _pipeline.push(envelope);
            if (this.logging) log("Pushed to pipeline: " + JSON.stringify(envelope));
            evaluatePipelineLater()
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
            let existingSessionId = _sessionId;
            log("close(): Closing MatsSocketSession, id:[" + existingSessionId + "] due to [" + reason + "], currently connected: [" + (_websocket ? _websocket.url : "not connected") + "]");

            // :: In-band session close
            // ?: Do we have WebSocket?
            if (_websocket) {
                // -> Yes, so close WebSocket with MatsSocket-specific CloseCode 4000.
                log(" \\-> WebSocket is open, so we close it with MatsSocket-specific CloseCode CLOSE_SESSION (4000).");
                // Perform the close
                _websocket.close(4000, reason);
            } else {
                log(" \\-> WebSocket NOT open, so CANNOT close it with MatsSocket-specific CloseCode CLOSE_SESSION (4000).");
            }

            // Get the current websocket URL before clearing state
            let currentWsUrl = currentWebSocketUrl();

            // :: Clear all state of this MatsSocket.
            clearStateAndPipelineAndFuturesAndOutstandingMessages("client close session");

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

        // ?: Is the self object an EventTarget (ie. window in a Browser context)
        if (typeof (self) === 'object' && typeof (self.addEventListener) === 'function') {
            // Yes -> Add "onbeforeunload" event listener to shut down the MatsSocket cleanly (closing session) when
            //        user navigates away from page.
            self.addEventListener("beforeunload", function (event) {
                that.close("window.onbeforeunload");
            });
        }
        const userAgent = (typeof (self) === 'object' && typeof (self.navigator) === 'object') ? self.navigator.userAgent : "Unknown";

        function clearStateAndPipelineAndFuturesAndOutstandingMessages(reason) {
            if (_websocket) {
                // We don't want the onclose callback invoked from this event that we initiated ourselves.
                _websocket.onclose = undefined;
                // We don't want any messages either, as we'll now be clearing out futures and outstanding messages ("acks")
                _websocket.onmessage = undefined;
                // Also drop onerror for good measure.
                _websocket.onerror = undefined;
            }
            _websocket = undefined;
            _sessionId = undefined;
            _urlIndexCurrentlyConnecting = 0;

            // :: Clear pipeline
            _pipeline.length = 0;

            // :: Reject all outstanding messages
            for (let cmseq in _outstandingSendsAndRequests) {
                if (!_outstandingSendsAndRequests.hasOwnProperty(cmseq)) continue;

                let outstandingMessage = _outstandingSendsAndRequests[cmseq];
                delete _outstandingSendsAndRequests[cmseq];

                log("Clearing outstanding [" + outstandingMessage.envelope.t + "] to [" + outstandingMessage.envelope.eid + "] with traceId ["+outstandingMessage.envelope.tid+"].");
                if (outstandingMessage.reject) {
                    try {
                        // TODO: Make better event object
                        outstandingMessage.reject({type: "CLEARED", description: reason});
                    } catch (err) {
                        error("Got error while clearing outstanding [" + outstandingMessage.envelope.t + "] to [" + outstandingMessage.envelope.eid + "].", err);
                    }
                }
            }

            // :: Reject all futures
            for (let cmseq in _futures) {
                if (!_futures.hasOwnProperty(cmseq)) continue;

                let future = _futures[cmseq];
                delete _futures[cmseq];

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
            // ?: Is the pipeline empty?
            if (_pipeline.length === 0) {
                // -> Yes, so nothing to do.
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
                // -> Yes, so then the socket is not open yet, but we are in the process.
                // Return now, as opening is async. When the socket opens, it will re-run 'evaluatePipelineSend()'.
                return;
            }

            // ?: Is the WebSocket present?
            if (_websocket === undefined) {
                // -> No, so go get it.
                initiateWebSocketCreation();
                // Returning now, as opening is async. When the socket opens, it will re-run 'evaluatePipelineSend()'.
                return;
            }

            // ----- We have an open WebSocket!

            let prePipeline = [];

            // -> Yes, WebSocket is open, so send any outstanding messages
            // ?: Have we sent HELLO?
            if (!_helloSent) {
                log("HELLO not sent, adding it to the pre-pipeline now.");
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
                    log("We expect session to be there [" + _sessionId + "]");
                    // -> Evidently yes, so add the requested reconnect-to-sessionId.
                    helloMessage.sid = _sessionId;
                    // This implementation of MatsSocket client lib expects existing session
                    // when reconnecting, thus wants pipelined messages to be ditched if
                    // the assumption about existing session fails.
                    helloMessage.st = "EXPECT_EXISTING";
                } else {
                    // -> We want a new session (which is default anyway)
                    helloMessage.st = "NEW";
                }
                // Add the HELLO to the prePipeline
                prePipeline.unshift(helloMessage);
                // We will now have sent the HELLO.
                _helloSent = true;
            }

            // :: Send pre-pipeline messages, if there are any
            // (Before the HELLO is sent and sessionId is established, the max size of message is low on the server)
            if (prePipeline.length > 0) {
                if (that.logging) log("Flushing prePipeline of [" + prePipeline.length + "] messages.");
                _websocket.send(JSON.stringify(prePipeline));
                prePipeline.length = 0;
            }
            // :: Send any pipelined messages.
            if (_pipeline.length > 0) {
                if (that.logging) log("Flushing pipeline of [" + _pipeline.length + "] messages.");
                _websocket.send(JSON.stringify(_pipeline));
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
                messageSequenceId: envelope.cmseq,

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
        let _websocket = undefined;

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
            if (_websocket !== undefined) {
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
                _websocket = websocket;
                // We're not /trying/ to connect anymore..
                _webSocketConnecting = false;
                // Since we've just established this WebSocket, we have obviously not sent HELLO yet.
                _helloSent = false;

                // Set our proper handlers
                websocket.onopen = undefined; // No need for 'onopen', it is already open. Also, node.js evidently immediately fires it again, even though it was already fired.
                websocket.onerror = _onerror;
                websocket.onclose = _onclose;
                websocket.onmessage = _onmessage;

                // Fire off any waiting messages, next tick
                invokeLater(function () {
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
            _websocket = undefined;

            // ?: Special codes: UNEXPECTED_CONDITION || SERVICE_RESTART || RECONNECT
            if ((event.code === 1011)
                || (event.code === 1012)
                || (event.code === 4002)) {
                // -> One of the special "reissue" close codes -> Reissue all outstanding..
                log("Special 'reissue' close code, reissue and reconnect.")
                // TODO: Implement reissue.
                // TODO: Implement reconnect.

            } else {
                // -> NOT one of the special "reissue" close codes -> Reject all outstanding
                log("We were closed with close code:[" + event.code + "] and reason:[" + event.reason + "] - closing.");
                clearStateAndPipelineAndFuturesAndOutstandingMessages("server closed session");
            }
        }

        function _onmessage(webSocketEvent) {
            // // ?: Is this message received on the current '_websocket' instance (i.e. different (reconnect), or cleared/closed)?
            // if (this !== _websocket) {
            //     // -> NO! This received-message is not on the current _websocket instance.
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

                    if (envelope.t === "WELCOME") {
                        // TODO: Handle WELCOME message better. At least notify if sessionLost..
                        _sessionId = envelope.sid;
                        if (that.logging) log("We're WELCOME! Session:" + envelope.st + ", SessionId:" + _sessionId);
                    } else if (envelope.t === "RECEIVED") {
                        // -> RECEIVED ack/server_error/nack
                        let outstandingSendOrRequest = _outstandingSendsAndRequests[envelope.cmseq];
                        // ?: Check that we found it.
                        if (outstandingSendOrRequest === undefined) {
                            // -> No, the OutstandingSendOrRequest was not present.
                            // (Either already handled by fast REPLY, or was an insta-settling in IncomingAuthorizationAndAdapter, which won't send RECEIVED)
                            continue;
                        }
                        // E-> Yes, we had OutstandingSendOrRequest
                        // Delete it, as we're handling it now
                        delete _outstandingSendsAndRequests[envelope.cmseq];
                        // ?: Was it a "good" RECEIVED?
                        if (envelope.st === "ACK") {
                            // -> Yes, it was "ACK" - so Server was happy.
                            if (outstandingSendOrRequest.resolve)
                                outstandingSendOrRequest.resolve(eventFromEnvelope(envelope, receivedTimestamp));
                        } else {
                            // -> No, it was "ERROR" or "NACK", so message has not been forwarded to Mats
                            if (outstandingSendOrRequest.reject)
                                outstandingSendOrRequest.reject(eventFromEnvelope(envelope, receivedTimestamp));
                            // ?: Check if it was a REQUEST
                            if (outstandingSendOrRequest.envelope.t === "REQUEST") {
                                // -> Yes, this was a REQUEST
                                // Then we have to reject the REQUEST too - it was never sent to Mats, and will thus never get a Reply
                                // (Note: This is either a reject for a Promise, or errorCallback on Endpoint).
                                let requestEnvelope = outstandingSendOrRequest.envelope;
                                _completeFuture(requestEnvelope.reid, "REJECT", envelope, receivedTimestamp);
                            }
                        }
                    } else if (envelope.t === "REPLY") {
                        // -> Reply to REQUEST
                        // It is physically possible that the REPLY comes before the RECEIVED (I've observed it!)
                        // That could potentially be annoying for the using application (Reply before Received)..
                        // Handle this by checking whether the outstandingSendOrRequest is still in place, and resolve it if so.
                        let outstandingSendOrRequest = _outstandingSendsAndRequests[envelope.cmseq];
                        // ?: Was the outstandingSendOrRequest still present?
                        if (outstandingSendOrRequest) {
                            // -> Yes, still present - so we delete and resolve it
                            delete _outstandingSendsAndRequests[envelope.cmseq];
                            if (outstandingSendOrRequest.resolve)
                                outstandingSendOrRequest.resolve(eventFromEnvelope(envelope, receivedTimestamp));
                            // .. and then mark it as resolved, so that when the received comes, it won't be resolved again
                            outstandingSendOrRequest.handled = true;
                        }
                        // Complete the Promise on a REQUEST-with-Promise, or messageCallback/errorCallback on Endpoint for REQUEST-with-ReplyTo
                        _completeFuture(envelope.eid, envelope.st, envelope, receivedTimestamp);
                    }
                } catch (err) {
                    error("message", "Got error while handling incoming envelope.cmseq [" + envelope.cmseq + "], type '" + envelope.t + (envelope.st ? ":" + envelope.st : "") + ": " + JSON.stringify(envelope), err);
                }
            }
        }

        function _completeFuture(endpointId, resolveOrReject, envelope, receivedTimestamp) {
            // ?: Is this a REQUEST-with-Promise?
            if (endpointId === undefined) {
                // -> Yes, REQUEST-with-Promise (missing (client) EndpointId)
                // Get the outstanding future
                let future = _futures[envelope.cmseq];
                // ?: Did we have a future?
                if (future === undefined) {
                    // -> No, missing future, no Promise. Pretty strange, really (error in this code..)
                    error("missing future", "When getting a reply to a Promise, we did not find the future for message sequence [" + webSocketEvent.messageSequenceId + "].", webSocketEvent);
                    return;
                }
                // E-> We found the future
                // Delete the outstanding future, as we will complete it now.
                delete _futures[envelope.cmseq];
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
            that.addMessageToPipeline("SEND", {
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
            that.addMessageToPipeline("REQUEST", {
                eid: endpointId,
                tid: traceId,
                msg: message
            }, resolve, reject, receivedCallback);
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
            that.addMessageToPipeline("REQUEST", {
                eid: endpointId,
                reid: replyToEndpointId,
                cid: correlationId,
                tid: traceId,
                msg: message
            }, resolve, reject);
        });
    };

    exports.MatsSocket = MatsSocket;
}));