function MatsSocket(url, appName, appVersion) {

    // PUBLIC:

    this.url = url;
    this.appName = appName;
    this.appVersion = appVersion;

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
        evaluatePipelineSend();
    };

    /**
     * If this MatsSockets realizes that the 'expirationTime' of the authorization has passed when about to send a
     * message (except PINGs, which do not need fresh auth), it will invoke this callback function. A new authorization
     * must then be provided by invoking the 'setCurrentAuthorization' function - only when this is invoked, the
     * MatsSocket will send messages. The MatsSocket will stack up any messages that are sent while waiting for new
     * authorization, and send them all at once once the authorization is in (i.e. it'll "pipeline" the messages).
     *
     * @param {function} authorizationExpiredCallback function which will be invoked if the current time is more than
     * 'expirationTimeMillisSinceEpoch' - 'roomForLatencyMillis' of the last invocation of 'setCurrentAuthorization'.
     */
    this.setAuthorizationExpiredCallback = function (authorizationExpiredCallback) {
        _authorizationExpiredCallback = authorizationExpiredCallback;
    };

    /**
     * This can be used by the mechanism invoking 'setCurrentAuthorization' to decide whether it should keep the
     * authorization fresh (i.e. no latency waiting for new authorization is introduced when a new message is
     * enqueued), or fall back to relying on the 'authorizationExpiredCallback' being invoked when a new message needs
     * it (thus introducing latency while waiting for authorization). One could envision keeping fresh auth for 5
     * minutes, but if the user has not done anything requiring server comms in that timespan, you stop doing auth
     * refresh until he again does something and the 'authorizationExpiredCallback' is invoked.
     *
     * @returns {number} millis-since-epoch of last message enqueued.
     */
    this.getLastMessageEnqueued = function () {
        return _lastMessageEnqueuedMillisSinceEpoch;
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
        console.log("Registering endpoint on id [" + endpointId + "]: " + callback);
        _endpoints[endpointId] = callback;
    };

    /**
     * <b>YOU SHOULD PROBABLY NOT USE THIS!</b>, instead using the specific prototype methods for generating messages.
     * Add a message to the outgoing pipeline, evaluates whether to send the pipeline afterwards (i.e. if pipelining
     * is active or not).
     */
    this.addMessageToPipeline = function (type, msg, correlationOrReplyId) {
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

        _pipeline.push(msg);
        console.log("Pushed to pipeline: " + JSON.stringify(msg))
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
            console.error("Caught error while executing pipeline()-lambda: Dropping any added messages.")
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

    // PRIVATE

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

    // "That" reference
    var that = this;

    // Add "onbeforeunload" event listener to shut down the MatsSocket cleanly (closing session) when user navigates
    // away from page.
    window.addEventListener("beforeunload", function (event) {
        console.log("OnBeforeUnload: Shutting down MatsSocket [" + that.url + "] due to [" + event.type + "]");
        that.closeSession("'window.onbeforeunload'");
    });

    /**
     * Sends pipelined messages if pipelining is not engaged.
     */
    var evaluatePipelineSend = function () {
        // ?: Are we currently pipelining?
        if (_pipelining) {
            // -> Yes, so nothing to do.
            return;
        }
        // ?: Do we have authorization?!
        if (_authorization === undefined) {
            // -> Yes, authorization is expired.
            console.log("Authorization was not present. Need this to continue. Invoking callback..");
            var e1 = {
                currentAuthorizationExpirationTime: _expirationTimeMillisSinceEpoch
            };
            _authorizationExpiredCallback(e1);
            return;
        }
        // ?: Check whether we have expired authorization
        if ((_expirationTimeMillisSinceEpoch !== undefined) && (_expirationTimeMillisSinceEpoch !== -1)
            && ((_expirationTimeMillisSinceEpoch - _roomForLatencyMillis) < Date.now())) {
            // -> Yes, authorization is expired.
            console.log("Authorization was expired. Need new to continue. Invoking callback..");
            var e2 = {
                currentAuthorizationExpirationTime: _expirationTimeMillisSinceEpoch
            };
            _authorizationExpiredCallback(e2);
            return;
        }
        // E-> Not pipelining, and auth is present.

        // :: Get WebSocket open. NOTE: Opening WebSocket is async...
        ensureWebSocket();
        // ...so WebSocket might not be open right afterwards.

        // ?: Is the WebSocket open? (Otherwise, the opening of the WebSocket will re-invoke this function)
        if (_socketOpen) {
            // -> Yes, WebSocket is open, so send any outstanding messages
            // ?: Have we sent HELLO?
            if (!_helloSent) {
                // -> No, HELLO not sent, so we create it now (auth is OK)
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
                    // -> Evidently yes, so add the requested reconnect-to-sessionId.
                    connectMsg.sessionId = _sessionId;
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
            // Send messages
            _websocket.send(JSON.stringify(_pipeline));
            // Clear pipeline
            _pipeline = [];
        }
    };

    // Private method to ensure socket
    var ensureWebSocket = function () {
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
        _websocket = new WebSocket(url);
        _websocket.onopen = function (event) {
            console.log("onopen");
            console.log(event);
            // Socket is now ready for business
            _socketOpen = true;
            // Fire off any waiting messages
            evaluatePipelineSend();
        };
        _websocket.onmessage = function (event) {
            var receivedTimestamp = Date.now();
            var data = event.data;
            var parsed = JSON.parse(data);
            if (parsed.t === "WELCOME") {
                // TODO: Handle WELCOME message better.
                _sessionId = parsed.sid;
                console.log("We're WELCOME! Session:"+parsed.st+", SessionId:" + _sessionId);
            } else {
                // -> Assume message that contains EndpointId
                var endpoint = _endpoints[parsed.eid];
                var eventToCallback = {
                    data: parsed.msg,
                    traceId: parsed.tid,
                    correlationId: parsed.cid,
                    // Timestamps
                    clientMessageCreated: parsed.cmcts,
                    clientMessageReceived: parsed.cmrts,
                    matsMessageSent: parsed.mmsts,
                    matsMessageReceived: parsed.mmrts,
                    replyMessageToClient: parsed.rmcts,
                    messageReceivedOnClient: receivedTimestamp,
                };
                endpoint(eventToCallback);
            }
        };
        _websocket.onclose = function (event) {
            console.log("onclose");
            console.log(event);
            // Ditch this WebSocket
            _websocket = undefined;
            // .. and thus it is most definitely not open anymore.
            _socketOpen = false;
        };
        _websocket.onerror = function (event) {
            console.log("onerror");
            console.log(event);
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
    }, correlationId);
};

/**
 * Sends a 'CLOSE_SESSION' message - authorization expiration check is not performed (the server does not evaluate
 * auth for CLOSE_SESSION), and if there is a pipeline, this will be dropped (i.e. messages deleted). The effect
 * is to cleanly shut down the Session and MatsSocket (closing session on server side), which will reply by shutting
 * down the underlying WebSocket.
 * <p />
 * Notice: Afterwards, the MatsSocket is as clean as if it was newly instantiated and all initializations was run
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

