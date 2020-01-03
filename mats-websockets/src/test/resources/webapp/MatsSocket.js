function MatsSocket(appName, appVersion, urls) {

    // PUBLIC:

    this.appName = appName;
    this.appVersion = appVersion;
    this.urls = [].concat(urls); // Ensure array
    this.logging = false; // Whether to log

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
     * <b>NOTE: This SHALL NEVER be used to CHANGE the user! It should only refresh an existing authorization for the
     * initially authenticated user. One MatsSocket (Session) shall only ever be used by a single user: If changing
     * user, you should ditch the existing MatsSocket (preferably invoking 'shutdown' to close the session properly on
     * the server side too), and make a new MatsSocket thus getting a new Session.
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
        // TODO: If no message pending, then make an AUTH message.

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
        return _lastMessageEnqueuedMillisSinceEpoch;
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
     * @param callback takes two args: message, and optional correlationId.
     */
    this.endpoint = function (endpointId, callback) {
        // :: Assert for double-registrations
        if (_endpoints[endpointId] !== undefined) {
            throw new Error("Cannot register more than one endpoint to same endpointId [" + endpointId
                + "], existing: " + _endpoints[endpointId]);
        }
        log("Registering endpoint on id [" + endpointId + "]: " + callback);
        _endpoints[endpointId] = callback;
    };

    /**
     * <b>YOU SHOULD PROBABLY NOT USE THIS!</b>, instead using the specific prototype methods for generating messages.
     * Add a message to the outgoing pipeline, evaluates whether to send the pipeline afterwards (i.e. if pipelining
     * is active or not).
     */
    this.addMessageToPipeline = function (type, msg, callback) {
        // TODO: if third arg is function, set 'reid' and 'correlationId'.
        var now = Date.now();
        _lastMessageEnqueuedMillisSinceEpoch = now;
        // Add common params
        msg.t = type;
        msg.cmcts = now;

        // ?: Is this a close session message?
        if (type === "CLOSE_SESSION") {
            // -> Yes, so drop our session. Server will "reply" by closing WebSocket.
            // :: Try best-effort to pass this message, but if WebSocket is not open, just forget about it.
            // Any new message will be under new Session.
            _sessionId = undefined;
            // Drop any pipelined messages (why invoke shutdown() if you are pipelining?!).
            _pipeline = [];
            // Keep a temp ref to WebSocket, while we clear it out
            var tempSocket = _websocket;
            var tempOpen = _socketOpen;
            // We do not own this WebSocket object anymore
            _websocket = undefined;
            _socketOpen = false;
            // ?: Was it open?
            if (tempOpen) {
                // -> Yes, so off it goes.
                tempSocket.send(JSON.stringify([msg]))
            }
            // We're done. This MatsSocket should be as good as new.
            return;
        }

        // E-> Not special messages.

        // If we got a callback function, then register this
        if (typeof (callback) == 'function') {
            msg.reid = "MS.CBR";
            msg.cid = this.id(10);
            _callbacks[msg.cid] = callback;
        }

        _pipeline.push(msg);
        log("Pushed to pipeline: " + JSON.stringify(msg))
        evaluatePipelineSend();
    };

    /**
     * Send multiple messages in one go, e.g. several sends, requests or requestReplyTo's. When the "lambda block"
     * exits, all the messages will be sent in one WebSocket message. This can reduce latency a tiny bit, and if
     * compression is enabled, the combined message might become a bit smaller than several individual messages.
     *
     * @param {function} lambda will be invoked with this MatsSocket as sole argument.
     */
    this.pipeline = function (lambda) {
        // Stash away the current pipeline, and make an empty one.
        // (The issue is that the same pipeline is used for pipelining, and to hold messages while getting auth)
        var existingMessages = _pipeline;
        _pipeline = [];
        // Enable pipelining
        _pipelining = true;
        try {
            // Run the lambda, which should add new messages in the pipeline
            lambda(this);
        } catch (err) {
            // The lambda raised some error, thus we log this and clear the pipeline.
            error("Caught error while executing pipeline()-lambda: Dropping any added messages.")
            _pipeline = [];
            throw err;
        } finally {
            // Turn off pipelining
            _pipelining = false;
            // Hold the new messages added by the lambda
            var newMessages = _pipeline;
            // Concat the existing messages with the new messages
            _pipeline = [].concat(existingMessages, newMessages);
            // Evaluate the pipeline.
            evaluatePipelineSend();
        }
    };

    /**
     * Do not randomize the provided WebSocket URLs. Should only be used for testing, as you definitely want
     * randomization of which URL to connect to - it will always go for the 0th element first.
     */
    this.disableUrlRandomization = function () {
        _useUrls = this.urls;
    };

    /**
     * Convenience method for making random strings for correlationIds (choose e.g. length=10) and
     * "add-on to traceId to make it pretty unique" (choose length=6).
     *
     * @param length how long the string should be: 10 for correlationId, 6 for "added to traceId".
     * @returns {string} from digits, lower and upper case letters - 62 entries.
     */
    this.id = function (length) {
        var result = '';
        for (var i = 0; i < length; i++) {
            result += _alphabet[Math.floor(Math.random() * _alphabet.length)];
        }
        return result;
    };

    // ==============================================================================================

    // PRIVATE

    // https://stackoverflow.com/a/12646864/39334
    function shuffleArray(array) {
        for (var i = array.length - 1; i > 0; i--) {
            var j = Math.floor(Math.random() * (i + 1));
            var temp = array[i];
            array[i] = array[j];
            array[j] = temp;
        }
    }

    // The URLs to use - will be shuffled. Can be reset to not randomized by this.disableUrlRandomize()
    var _useUrls = [].concat(this.urls);
    // Shuffle the URLs
    shuffleArray(_useUrls);

    // alphabet length: 10 + 26 x 2 = 62.
    var _alphabet = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

    // fields
    var _sessionId = undefined;
    var _pipeline = [];
    var _endpoints = {};

    var _websocket = undefined;
    var _helloSent = false;

    var _pipelining = false;
    var _socketOpen = false;

    var _authorization = undefined;
    var _sendAuthorizationToServer = false;
    var _expirationTimeMillisSinceEpoch = undefined;
    var _roomForLatencyMillis = undefined;
    var _authorizationExpiredCallback = undefined;
    var _lastMessageEnqueuedMillisSinceEpoch = Date.now(); // Start by assuming that it was just used.

    // When we've informed the app that we need auth, we do not need to do it again until it has set it.
    var _authExpiredCallbackInvoked = false;

    // Outstanding callbacks
    var _callbacks = {};

    // "That" reference
    var that = this;

    function log(msg, object) {
        if (that.logging) {
            if (object !== undefined) {
                console.log(msg, object);
            }
            else {
                console.log(msg);
            }
        }
    }


    // Add "onbeforeunload" event listener to shut down the MatsSocket cleanly (closing session) when user navigates
    // away from page.
    window.addEventListener("beforeunload", function (event) {
        log("OnBeforeUnload: Shutting down MatsSocket [" + that.url + "] due to [" + event.type + "]");
        that.closeSession("'window.onbeforeunload'");
    });

    // Register the MatsSocket's system Callback Reply Endpoint
    this.endpoint("MS.CBR", function (event) {
        // Get the outstanding callback
        var callback = _callbacks[event.correlationId];
        // Delete the outstanding callback (we will complete it now)
        delete _callbacks[event.correlationId];
        // Invoke the callback if it was registered
        if (callback !== undefined) {
            callback(event);
        }
    });
    
    /**
     * Sends pipelined messages if pipelining is not engaged.
     */
    function evaluatePipelineSend() {
        // ?: Are we currently pipelining?
        if (_pipelining) {
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
            var e1 = {
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
            var e2 = {
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

        // :: Get WebSocket open. NOTE: Opening WebSocket is async...
        ensureWebSocket();
        // ...so WebSocket might not be open right afterwards.

        // ?: Is the WebSocket open? (Otherwise, the opening of the WebSocket will re-invoke this function)
        if (_socketOpen) {
            // -> Yes, WebSocket is open, so send any outstanding messages
            // ?: Have we sent HELLO?
            if (!_helloSent) {
                log("HELLO not sent, prepending it to the pipeline now.");
                // -> No, HELLO not sent, so we create it now (auth is present, check above)
                var connectMsg = {
                    t: "HELLO",
                    clv: "MatsSocketJs; User-Agent: " + navigator.userAgent,
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
                    connectMsg.sid = _sessionId;
                    // This implementation of MatsSocket client lib expects existing session
                    // when reconnecting, thus wants pipelined messages to be ditched if
                    // the assumption about existing session fails.
                    connectMsg.st = "EXPECT_EXISTING";
                } else {
                    // -> We want a new session (which is default anyway)
                    connectMsg.st = "NEW";
                }
                // Add HELLO msg to front of pipeline
                _pipeline.unshift(connectMsg);
                // We will now have sent the HELLO.
                _helloSent = true;
            }

            // Send messages, if there are any
            if (_pipeline.length > 0) {
                log("Flushing pipeline of [" + _pipeline.length + "] messages.");
                _websocket.send(JSON.stringify(_pipeline));
                // Clear pipeline
                _pipeline = [];
            }
        }
    }

    function eventFromEnvelope(envelope, receivedTimestamp) {
        var eventToCallback = {
            data: envelope.msg,
            type: envelope.t,
            subType: envelope.ts,
            traceId: envelope.tid,
            correlationId: envelope.cid,

            // Timestamps and handling nodenames
            clientMessageCreated: envelope.cmcts,
            clientMessageReceived: envelope.cmrts,
            clientMessageReceivedNodename: envelope.cmrnn,
            matsMessageSent: envelope.mmsts,
            matsMessageReplyReceived: envelope.mmrrts,
            matsMessageReplyReceivedNodename: envelope.mmrrnn,
            replyMessageToClient: envelope.rmcts,
            replyMessageToClientNodename: envelope.rmcnn,
            messageReceivedOnClient: receivedTimestamp
        };
        return eventToCallback;
    }

    var _tryingToConnect = false;
    var _urlIndexCurrentlyConnecting = 0; // Cycles through the URLs
    var _connectionFallbackLevel = 0; // When cycled one time through, increases.
    var _connectionTimeout = 250; // Milliseconds for this fallback level. Doubles, up to 10 seconds where stays.

    function ensureWebSocket() {
        // ?: Do we have the WebSocket object in place?
        if (_websocket !== undefined) {
            // -> Yes, WebSocket is in place, so either open or opening.
            return;
        }

        // E-> No, WebSocket ain't open, so fire it up
        // We've thus not sent HELLO message as the first message ever.
        _helloSent = false;
        // The WebSocket is definitely not open yet, since we've not created it yet.
        _socketOpen = false;
        log(_useUrls);
        log("Using urlIndexCurrentlyConnecting [" + _urlIndexCurrentlyConnecting + "]: " + _useUrls[_urlIndexCurrentlyConnecting]);
        _websocket = new WebSocket(_useUrls[_urlIndexCurrentlyConnecting], "matssocket");
        _websocket.onopen = function (event) {
            log("onopen");
            log(event);
            // Socket is now ready for business

            // TODO: Clear all reconnection settings
            // TODO:
            _urlIndexCurrentlyConnecting = 0;

            _socketOpen = true;
            // Fire off any waiting messages
            evaluatePipelineSend();
        };
        _websocket.onmessage = function (event) {
            var receivedTimestamp = Date.now();
            var data = event.data;
            var envelopes = JSON.parse(data);

            var numEnvelopes = envelopes.length;
            log("Got " + numEnvelopes + " messages.");

            for (var i = 0; i < numEnvelopes; i++) {
                var envelope = envelopes[i];

                if (envelope.t === "WELCOME") {
                    // TODO: Handle WELCOME message better.
                    _sessionId = envelope.sid;
                    log("We're WELCOME! Session:" + envelope.st + ", SessionId:" + _sessionId);
                } else {
                    // -> Assume message that contains EndpointId
                    try {
                        var endpoint = _endpoints[envelope.eid];
                        if (endpoint !== undefined) {
                            endpoint(eventFromEnvelope(envelope, receivedTimestamp));
                        }
                    } catch (error) {
                        error("Got error while trying to invoke endpoint for message: " + error, error);
                    }
                }
            }
        };
        _websocket.onclose = function (event) {
            log("onclose");
            log(event);
            // Ditch this WebSocket
            _websocket = undefined;
            // .. and thus it is most definitely not open anymore.
            _socketOpen = false;
            _helloSent = false;
        };
        _websocket.onerror = function (event) {
            log("onerror");
            log(event);
        };
    }
}

MatsSocket.prototype.send = function (endpointId, traceId, message, callback) {
    this.addMessageToPipeline("SEND", {
        eid: endpointId,
        tid: traceId,
        msg: message
    }, callback);
};

MatsSocket.prototype.request = function (endpointId, traceId, message, callback) {
    if (typeof (callback) !== 'function') {
        throw new Error("When using 'MatsSocket.request(...)', you shall provide a callback function.");
    }
    this.addMessageToPipeline("REQUEST", {
        eid: endpointId,
        tid: traceId,
        msg: message
    }, callback);
};

MatsSocket.prototype.requestReplyTo = function (endpointId, traceId, message, replyToEndpointId, correlationId) {
    this.addMessageToPipeline("REQUEST", {
        eid: endpointId,
        reid: replyToEndpointId,
        cid: correlationId,
        tid: traceId,
        msg: message
    });
};

/**
 * Sends a 'CLOSE_SESSION' message - authorization expiration check is not performed (the server does not evaluate
 * auth for CLOSE_SESSION). If there currently is a pipeline, this will be dropped (i.e. messages deleted). The effect
 * is to cleanly shut down the Session and MatsSocket (closing session on server side), which will reply by shutting
 * down the underlying WebSocket.
 * <p />
 * // TODO: Is this true? Notice: Afterwards, the MatsSocket is as clean as if it was newly instantiated and all initializations was run
 * (i.e. auth callback set, endpoints defined etc), and can be started up again by sending a message. The SessionId on
 * this client MatsSocket is cleared, and the previous Session on the server is gone anyway, so this will give a new
 * server side Session. If you want a totally clean MatsSocket, then just ditch this instance and make a new one.
 *
 * <b>Note: An 'onBeforeUnload' event handler is registered on 'window', which invokes this method.</b>
 *
 * @param {string} reason short descriptive string. Will be returned as part of reason string, must be quite short.
 */
MatsSocket.prototype.closeSession = function (reason) {
    this.addMessageToPipeline("CLOSE_SESSION", {
        tid: "MatsSocket_shutdown[" + reason + "]" + this.id(6),
        desc: reason
    });
};

