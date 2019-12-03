function MatsSocket(url, appName, appVersion) {

    // PUBLIC:

    this.url = url;
    this.appName = appName;
    this.appVersion = appVersion;

    /**
     * Only set this if you explicitly want to continue a previous session. In an SPA, you probably do not want to set this,
     * as you rather want the new sessionId provided by the server. In case of connection failures, the MatsSocket will
     * reconnect on its own.
     *
     * @param sessionId
     */
    this.setSessionId = function (sessionId) {
        _sessionId = sessionId;
    };

    /**
     * @param endpointId
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
    this.addMessageToPipeline = function (msg, correlationOrReplyId) {
        // TODO: if second arg is function, set 'reid' and 'correlationId'.
        _pipeline.push(msg);
        this.evaluatePipelineSend();
    };

    /**
     * Sends pipelined messages if pipelining is not engaged.
     */
    this.evaluatePipelineSend = function () {
        // ?: Are we currently pipelining?
        if (_pipelining) {
            // -> Yes, so nothing to do.
            return;
        }
        // E-> Not pipelining
        // Get WebSocket open. NOTE: Opening WebSocket is async...
        ensureWebSocket();
        // ...so WebSocket might not be open right afterwards.

        // ?: Is the WebSocket open? (Otherwise, the opening of the WebSocket will ensure send)
        if (_socketOpen) {
            // -> Yes, WebSocket is open, so send any outstanding messages
            // Send messages
            _websocket.send(JSON.stringify(_pipeline));
            // Clear pipeline
            _pipeline = [];
        }
    };

    /**
     * Turn on pipelining, must invoke 'ship()' to turn off and send the messages.
     */
    this.pipeline = function () {
        _pipelining = true;
    };

    /**
     * Ship any pipelined messages. If the underlying WebSocket is not yet open, it will first be opened and a CONNECT
     * message is put first in the pipeline before it is shipped.
     */
    this.ship = function () {
        _pipelining = false;
        this.evaluatePipelineSend();
    };

    // PRIVATE

    // fields
    var _sessionId = undefined;
    var _pipeline = [];
    var _endpoints = {};

    var _websocket = undefined;

    var _pipelining = false;
    var _socketOpen = false;

    // Add "onbeforeunload" event listener to shut down the MatsSocket cleanly (closing session) when user navigates
    // away from page.
    var that = this;
    window.addEventListener("beforeunload", function (event) {
        console.log("OnBeforeUnload: Shutting down MatsSocket [" + that.url + "] due to [" + event.type + "]");
        that.shutdown();
    });

    // Private method to ensure socket
    var ensureWebSocket = function () {
        // ?: Do we have the WebSocket open?
        if (_websocket !== undefined) {
            // -> Yes, WebSocket is open, so nothing to do.
            return;
        }
        // E-> No, WebSocket ain't open, so fire it up
        // :: Stick the CONNECT message in front of the pipeline.
        var connectMsg = {
            t: "HELLO",
            ts: Date.now(),
            auth: "DummyAuth",
            tid: "MatsSocket_Start"
        };
        // ?: Do we have requested a reconnect?
        if (_sessionId !== undefined) {
            // -> Evidently yes, so add the requested reconnect-to-sessionId.
            connectMsg.sessionId = _sessionId;
        }
        // Add to front
        _pipeline.unshift(connectMsg);
        // Start the WebSocket, forwarding the lambda
        startWebSocket();
    };

    // Private method to start WebSocket
    var startWebSocket = function () {
        _websocket = new WebSocket(url);
        _websocket.onopen = function (event) {
            console.log("onopen");
            console.log(event);
            // Socket is now ready for business
            _socketOpen = true;
            // Fire off any waiting messages
            that.evaluatePipelineSend();
        };
        _websocket.onmessage = function (event) {
            // console.log("onmessage");
            var data = event.data;
            var parsed = JSON.parse(data);
            if (parsed.t === "WELCOME") {
                // TODO: Handle WELCOME message better.
                _sessionId = parsed.sid;
                console.log("We're WELCOME! SessionId:" + _sessionId);
            } else {
                // -> Assume message that contains EndpointId
                var endpoint = _endpoints[parsed.eid];
                endpoint(parsed.msg, parsed.cid);
            }
        };
        _websocket.onclose = function (event) {
            console.log("onclose");
            console.log(event);
        };
        _websocket.onerror = function (event) {
            console.log("onerror");
            console.log(event);
        };
    }
}

MatsSocket.prototype.send = function (endpointId, traceId, message, callback) {
    this.addMessageToPipeline({
        t: "SEND",
        eid: endpointId,
        tid: traceId,
        cmcts: Date.now(),
        msg: message
    }, callback);
};

MatsSocket.prototype.request = function (endpointId, traceId, message, callback) {
    this.addMessageToPipeline({
        t: "REQUEST",
        eid: endpointId,
        tid: traceId,
        cmcts: Date.now(),
        msg: message
    }, callback);
};

MatsSocket.prototype.requestReplyTo = function (endpointId, traceId, message, replyToEndpointId, correlationId) {
    this.addMessageToPipeline({
        t: "REQUEST",
        eid: endpointId,
        reid: replyToEndpointId,
        cid: correlationId,
        tid: traceId,
        cmcts: Date.now(),
        msg: message
    }, correlationId);
};

/**
 * Sends a 'DISCONNECT' message, cleanly shutting down the Session and MatsSocket (killing session on server side),
 * which will reply by shutting down the underlying WebSocket.
 * Note: Invokes 'ship()'.
 *
 * <b>Note: An 'onBeforeUnload' event handler is registered on 'window', which invokes this method.</b>
 */
MatsSocket.prototype.shutdown = function (reason) {
    this.addMessageToPipeline({
        t: "DISCONNECT",
        cmcts: Date.now(),
        tid: "MatsSocket[" + reason + "]"
    });
    this.ship();
};

