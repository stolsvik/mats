// Register as an UMD module - source: https://github.com/umdjs/umd/blob/master/templates/commonjsStrict.js
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        // AMD. Register as an anonymous module.
        define(['chai', 'sinon', 'ws', 'mats', 'env'], factory);
    } else if (typeof exports === 'object' && typeof exports.nodeName !== 'string') {
        // CommonJS
        const chai = require('chai');
        const sinon = require('sinon');
        const ws = require('ws');
        const mats = require('../lib/MatsSocket');

        factory(chai, sinon, ws, mats, process.env);
    } else {
        // Browser globals
        factory(chai, sinon, WebSocket, mats, {});
    }
}(typeof self !== 'undefined' ? self : this, function (chai, sinon, ws, mats, env) {
    const MatsSocket = mats.MatsSocket;

    describe('MatsSocket integration tests, basics', function () {
        let matsSocket;

        function setAuth(userId = "standard", duration = 20000, roomForLatencyMillis = 10000) {
            const now = Date.now();
            const expiry = now + duration;
            matsSocket.setCurrentAuthorization("DummyAuth:" + userId + ":" + expiry, expiry, roomForLatencyMillis);
        }

        const urls = env.MATS_SOCKET_URLS || "ws://localhost:8080/matssocket,ws://localhost:8081/matssocket";

        beforeEach(() => {
            matsSocket = new MatsSocket("TestApp", "1.2.3", urls.split(","));
            matsSocket.logging = false;
        });

        afterEach(() => {
            // :: Chill the close slightly, so as to get the final "ACK2" envelope over to delete server's inbox.
            // NOTE: This is done async, so for the fast tests, the closings will come in "behind".
            let toClose = matsSocket;
            setTimeout(function () {
                toClose.close("Test done");
            }, 250);
        });

        function standardStateAssert() {
            chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
            chai.assert.strictEqual(matsSocket.state, mats.ConnectionState.SESSION_ESTABLISHED, "MatsSocket should have been in ConnectionState.SESSION_ESTABLISHED!");
        }

        describe('simple sends', function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            // NOTE: There used to be a "fire-and-forget" variant here. The problem is that when matsSocket.close() is
            // invoked, it rejects all outstanding messages - and since this happens earlier than the acknowledge actually
            // coming back, Node gets angry with the following information:
            //
            // (node:30412) UnhandledPromiseRejectionWarning: Unhandled promise rejection. This error originated either by
            //              throwing inside of an async function without a catch block, or by rejecting a promise which was
            //              not handled with .catch(). (rejection id: 1)
            // (node:30412) [DEP0018] DeprecationWarning: Unhandled promise rejections are deprecated. In the future,
            //              promise rejections that are not handled will terminate the Node.js process with a non-zero exit
            //              code.

            it('Should have a promise that resolves when received', function () {
                // Return a promise, that mocha will watch and resolve
                return matsSocket.send("Test.single", "SEND_" + matsSocket.id(6), {
                    string: "The String",
                    number: Math.PI
                });
            });

            it('Send to non-existing endpoint should NACK from Server and reject Promise', function (done) {
                let promise = matsSocket.send("NON EXISTING!", "SEND_NonExisting" + matsSocket.id(6), {
                    string: "The String",
                    number: Math.PI
                });
                promise.catch(receivedEvent => {
                    standardStateAssert();
                    chai.assert.strictEqual(receivedEvent.type, mats.ReceivedEventType.NACK);
                    done();
                });
            });
        });

        describe("simple requests", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            it("Request should resolve Promise", function () {
                // Return a promise, that mocha will watch and resolve
                return matsSocket.request("Test.single", "REQUEST-with-Promise_" + matsSocket.id(6), {
                    string: "Request String",
                    number: Math.E
                });
            });

            it("Request should invoke both the ack callback and resolve Promise", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.single", "REQUEST-with-Promise-and-receivedCallback_" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.then(reason => {
                    standardStateAssert();
                    chai.assert(received, "The received-callback should have been invoked.");
                    done();
                });
            });

            it('Request to non-existing endpoint should NACK from Server, invoking receivedCallback and reject Promise', function (done) {
                let received = false;
                let promise = matsSocket.request("NON EXISTING!", "REQUEST_NonExisting" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.catch(reason => {
                    standardStateAssert();
                    chai.assert(received, "The received-callback should have been invoked.");
                    done();
                });
            });
        });

        describe("requestReplyTo", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            it("Should reply to our own endpoint", function (done) {
                matsSocket.terminator("ClientSide.testTerminator", () => done());
                matsSocket.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_" + matsSocket.id(6), {
                    string: "Request String",
                    number: Math.E
                }, "ClientSide.testTerminator");
            });

            it("Should reply to our own endpoint with our correlation data", function (done) {
                const correlationInformation = matsSocket.id(5);
                matsSocket.terminator("ClientSide.testTerminator", ({correlationInformation: correlationInformation}) => {
                    standardStateAssert();
                    chai.assert.equal(correlationInformation, correlationInformation);
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                });
                matsSocket.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_withCorrelationInfo_" + matsSocket.id(6), {
                    string: "Request String",
                    number: Math.E
                }, "ClientSide.testTerminator", correlationInformation);
            });
        });

        describe("timeout on request", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            it("request with timeout earlier than slow endpoint", function (done) {
                // :: The actual test
                function test() {
                    let req = {
                        string: "test",
                        number: -1,
                        sleepIncoming: 50, // Sleeptime before acknowledging
                        sleepTime: 50 // Sleeptime before replying
                    };
                    let receivedCallbackInvoked = false;
                    matsSocket.request("Test.slow", "Timeout_request_" + matsSocket.id(6), req, {
                        // Low timeout to NOT get ReceivedEventType.ACK.
                        timeout: 10,
                        receivedCallback: function (event) {
                            chai.assert.strictEqual(event.type, mats.ReceivedEventType.TIMEOUT);
                            receivedCallbackInvoked = true;
                        }
                    }).catch(event => {
                        standardStateAssert();
                        chai.assert.strictEqual(event.type, mats.MessageEventType.TIMEOUT);
                        if (receivedCallbackInvoked) {
                            done();
                        }
                    });
                    matsSocket.flush();
                }

                // :: .. but must first wait for SESSION_ESTABLISHED to run test
                matsSocket.addConnectionEventListener(event => {
                    if (event.type === mats.ConnectionEventType.SESSION_ESTABLISHED) {
                        standardStateAssert();
                        test();
                    }
                });
                // MatsSocket does not start until first message.
                matsSocket.send("Test.ignoreInIncomingHandler", "Timeout_request_start", {});
            });

            it("requestReplyTo with timeout earlier than slow endpoint", function (done) {
                let correlationInformation = matsSocket.jid(100);

                /**
                 * NOTICE: We should FIRST get the rejection on the Received Promise from requestReplyTo, and THEN
                 * we should get a reject on the Terminator.
                 */

                let receivedResolveInvoked = false;
                let receivedRejectInvoked = false;

                // :: The actual test
                function test() {
                    let req = {
                        string: "test",
                        number: -2,
                        sleepIncoming: 50, // Sleeptime before acknowledging
                        sleepTime: 50 // Sleeptime before replying
                    };
                    matsSocket.requestReplyTo("Test.slow", "Timeout_requestReplyTo_" + matsSocket.id(6), req, "Test.terminator", correlationInformation, {
                        // Low timeout to NOT get ReceivedEventType.ACK.
                        timeout: 5
                    }).then(_ => {
                        receivedResolveInvoked = true;
                    }).catch(event => {
                        standardStateAssert();
                        chai.assert.strictEqual(event.type, mats.ReceivedEventType.TIMEOUT);
                        receivedRejectInvoked = true;
                    });
                    matsSocket.flush();
                }

                // Create Terminator to receive the return
                let messageCallbackInvoked = false;
                matsSocket.terminator("Test.terminator", messageEvent => {
                    // We do NOT expect a message!
                    messageCallbackInvoked = true;
                }, event => {
                    standardStateAssert();
                    chai.assert.strictEqual(event.type, mats.ReceivedEventType.TIMEOUT);
                    chai.assert.strictEqual(event.correlationInformation, correlationInformation);
                    // NOTE! The ReceivedReject should already have been invoked.
                    chai.assert.isTrue(receivedRejectInvoked);

                    // Just wait a tad to see that we don't also get the messageCallbackInvoked
                    setTimeout(function () {
                        standardStateAssert();
                        chai.assert.isFalse(receivedResolveInvoked);
                        chai.assert.isFalse(messageCallbackInvoked);
                        done();
                    }, 5);
                });

                // :: .. but must first wait for SESSION_ESTABLISHED to run test
                matsSocket.addConnectionEventListener(event => {
                    if (event.type === mats.ConnectionEventType.SESSION_ESTABLISHED) {
                        standardStateAssert();
                        test();
                    }
                });
                // MatsSocket does not start until first message.
                matsSocket.send("Test.ignoreInIncomingHandler", "Timeout_requestReplyTo_start", {});
            });
        });

        describe("pipeline", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            it("Pipeline should reply to our specified Terminator", function (done) {
                let replyCount = 0;
                matsSocket.terminator("ClientSide.testEndpoint", function (e) {
                    standardStateAssert();
                    replyCount += 1;
                    if (replyCount === 3) {
                        done();
                    }
                });

                // These three will be "autopipelined".

                matsSocket.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_Pipeline_1_" + matsSocket.id(6),
                    {string: "Messge 1", number: 100.001, requestTimestamp: Date.now()},
                    "ClientSide.testEndpoint", "pipeline_1_" + matsSocket.id(10));
                matsSocket.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_Pipeline_2_" + matsSocket.id(6),
                    {string: "Message 2", number: 200.002, requestTimestamp: Date.now()},
                    "ClientSide.testEndpoint", "pipeline_2_" + matsSocket.id(10));
                matsSocket.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_Pipeline_3_" + matsSocket.id(6),
                    {string: "Message 3", number: 300.003, requestTimestamp: Date.now()},
                    "ClientSide.testEndpoint", "pipeline_3_" + matsSocket.id(10));
                matsSocket.flush();
            });
        });

        describe("requests handled in IncomingAuthorizationAndAdapter", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            // FOR ALL: Both the received callback should be invoked, and the Promise resolved/rejected

            it("Ignored (handler did nothing), should NACK when REQUEST (and thus reject Promise) since it is not allowed to ignore a Request (must either deny, insta-settle or forward)", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.ignoreInIncomingHandler", "REQUEST_ignored_in_incomingHandler" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.catch(function () {
                    standardStateAssert();
                    chai.assert(received, "The received-callback should have been invoked.");
                    done();
                });
            });

            it("context.deny() should NACK (and thus reject Promise)", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.denyInIncomingHandler", "REQUEST_denied_in_incomingHandler" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.catch(function () {
                    standardStateAssert();
                    chai.assert(received, "The received-callback should have been invoked.");
                    done();
                });
            });

            it("context.resolve(..) should ACK received, and RESOLVE the Promise.", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.resolveInIncomingHandler", "REQUEST_resolved_in_incomingHandler" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.then(function () {
                    standardStateAssert();
                    chai.assert(received, "The received-callback should have been invoked.");
                    done();
                });

            });

            it("context.reject(..) should ACK received, and REJECT the Promise", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.rejectInIncomingHandler", "REQUEST_rejected_in_incomingHandler" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.catch(function () {
                    standardStateAssert();
                    chai.assert(received, "The received-callback should have been invoked.");
                    done();
                });
            });

            it("Exception in incomingAdapter should NACK (and thus reject Promise)", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.throwsInIncomingHandler", "REQUEST_throws_in_incomingHandler" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.catch(function () {
                    standardStateAssert();
                    chai.assert(received, "The received-callback should have been invoked.");
                    done();
                });
            });
        });

        describe("sends handled in IncomingAuthorizationAndAdapter", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            // FOR ALL: Both the received callback should be invoked, and the Promise resolved/rejected

            it("Ignored (handler did nothing) should ACK when SEND (thus resolve Promise)", function (done) {
                let promise = matsSocket.send("Test.ignoreInIncomingHandler", "SEND_ignored_in_incomingHandler" + matsSocket.id(6), {});
                promise.then(function () {
                    standardStateAssert();
                    done();
                });
            });

            it("context.deny() should NACK (reject Promise)", function (done) {
                let promise = matsSocket.send("Test.denyInIncomingHandler", "SEND_denied_in_incomingHandler" + matsSocket.id(6), {});
                promise.catch(function () {
                    standardStateAssert();
                    done();
                });
            });

            it("context.resolve(..) should NACK (reject Promise) since it is not allowed to resolve/reject a send", function (done) {
                let promise = matsSocket.send("Test.resolveInIncomingHandler", "SEND_resolved_in_incomingHandler" + matsSocket.id(6), {});
                promise.catch(function () {
                    standardStateAssert();
                    done();
                });

            });

            it("context.reject(..) should NACK (reject Promise) since it is not allowed to resolve/reject a send", function (done) {
                let promise = matsSocket.send("Test.rejectInIncomingHandler", "SEND_rejected_in_incomingHandler" + matsSocket.id(6), {});
                promise.catch(function () {
                    standardStateAssert();
                    done();
                });
            });

            it("Exception in incomingAdapter should NACK (reject Promise)", function (done) {
                let promise = matsSocket.send("Test.throwsInIncomingHandler", "SEND_throws_in_incomingHandler" + matsSocket.id(6), {});
                promise.catch(function () {
                    done();
                });
            });
        });

        describe("requests handled in replyAdapter", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            // FOR ALL: Both the received callback should be invoked, and the Promise resolved/rejected

            it("Ignored (handler did nothing) should NACK when Request handled in adaptReply(..) (thus nack receivedCallback, and reject Promise)", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.ignoreInReplyAdapter", "REQUEST_ignored_in_replyAdapter" + matsSocket.id(6), {},
                    function () {
                        standardStateAssert();
                        received = true;
                    });
                promise.catch(function () {
                    standardStateAssert();
                    if (received) {
                        done();
                    }
                });
            });

            it("context.resolve(..)", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.resolveInReplyAdapter", "REQUEST_resolved_in_replyAdapter" + matsSocket.id(6), {},
                    function () {
                        standardStateAssert();
                        received = true;
                    });
                promise.then(function () {
                    standardStateAssert();
                    if (received) {
                        done();
                    }
                });
            });

            it("context.reject(..)", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.rejectInReplyAdapter", "REQUEST_rejected_in_replyAdapter" + matsSocket.id(6), {},
                    function () {
                        standardStateAssert();
                        received = true;
                    });
                promise.catch(function () {
                    standardStateAssert();
                    if (received) {
                        done();
                    }
                });
            });

            it("Exception in replyAdapter should reject", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.throwsInReplyAdapter", "REQUEST_throws_in_replyAdapter" + matsSocket.id(6), {},
                    function () {
                        standardStateAssert();
                        received = true;
                    });
                promise.catch(function () {
                    standardStateAssert();
                    if (received) {
                        done();
                    }
                });
            });
        });

        describe("debug object", function () {
            function testDebugOptionsSend(done, config, assert, userId = 'enableAllDebugOptions') {
                // Set special userId that gives us all DebugOptions
                setAuth(userId);

                let receivedEventReceived;
                config.receivedCallback = function (receivedEvent) {
                    standardStateAssert();
                    receivedEventReceived = receivedEvent;
                };
                matsSocket.request("Test.single", "Request_DebugObject_" + config.debug + "_" + matsSocket.debug + "_" + matsSocket.id(6), {}, config)
                    .then(function (messageEvent) {
                        standardStateAssert();
                        chai.assert.isDefined(receivedEventReceived);

                        chai.assert.isDefined(messageEvent.debug);

                        assert(messageEvent.debug);

                        done();
                    });

            }

            function assertNodes(debug) {
                chai.assert.isString(debug.clientMessageReceivedNodename);
                chai.assert.isString(debug.matsMessageReplyReceivedNodename);
                chai.assert.isString(debug.messageSentToClientNodename);
            }

            function assertNodesUndefined(debug) {
                chai.assert.isUndefined(debug.clientMessageReceivedNodename);
                chai.assert.isUndefined(debug.matsMessageReplyReceivedNodename);
                chai.assert.isUndefined(debug.messageSentToClientNodename);
            }

            function assertTimestamps(debug) {
                chai.assert.isNumber(debug.clientMessageSent);
                chai.assert.isNumber(debug.clientMessageReceived);
                chai.assert.isNumber(debug.matsMessageSent);
                chai.assert.isNumber(debug.matsMessageReplyReceived);
                chai.assert.isNumber(debug.messageSentToClient);
                chai.assert.isNumber(debug.messageReceived);
            }

            function assertTimestampsFromServerAreUndefined(debug) {
                chai.assert.isNumber(debug.clientMessageSent);
                chai.assert.isUndefined(debug.clientMessageReceived);
                chai.assert.isUndefined(debug.matsMessageSent);
                chai.assert.isUndefined(debug.matsMessageReplyReceived);
                chai.assert.isUndefined(debug.messageSentToClient);
                chai.assert.isNumber(debug.messageReceived);
            }

            it("When the user is allowed to debug, and request all via matsSocket.debug, the debug object should be present and all filled", function (done) {
                matsSocket.debug = mats.DebugOption.NODES | mats.DebugOption.TIMESTAMPS;
                testDebugOptionsSend(done, {}, function (debug) {
                    assertNodes(debug);
                    assertTimestamps(debug);
                });
            });

            it("When the user is allowed to debug, and request DebugOption.NODES via matsSocket.debug, the debug object should be present and filled with just nodes, not timestamps", function (done) {
                matsSocket.debug = mats.DebugOption.NODES;
                testDebugOptionsSend(done, {}, function (debug) {
                    assertNodes(debug);
                    assertTimestampsFromServerAreUndefined(debug);
                });
            });

            it("When the user is allowed to debug, and request DebugOption.TIMINGS via matsSocket.debug, the debug object should be present and filled with just timestamps, not nodes", function (done) {
                matsSocket.debug = mats.DebugOption.TIMESTAMPS;
                testDebugOptionsSend(done, {}, function (debug) {
                    assertNodesUndefined(debug);
                    assertTimestamps(debug);
                });
            });

            it("When the user is allowed to debug, and request all via message-specific config, while matsSocket.debug='undefined', the debug object should be present and all filled", function (done) {
                matsSocket.debug = undefined;
                testDebugOptionsSend(done, {debug: mats.DebugOption.NODES | mats.DebugOption.TIMESTAMPS}, function (debug) {
                    assertNodes(debug);
                    assertTimestamps(debug);
                });
            });

            it("When the user is NOT allowed to debug, and request all via matsSocket.debug, the debug object should just have the Client-side filled stuff", function (done) {
                matsSocket.debug = mats.DebugOption.NODES | mats.DebugOption.TIMESTAMPS;
                testDebugOptionsSend(done, {}, function (debug) {
                    assertNodesUndefined(debug);
                    assertTimestampsFromServerAreUndefined(debug);
                }, "userWithoutDebugOptions");
            });

            it("When the user is allowed to debug, but do not request any debug (undefined), there should not be a debug object", function (done) {
                // Set special userId that gives us all DebugOptions
                setAuth('enableAllDebugOptions');

                let receivedEventReceived;
                matsSocket.request("Test.single", "Request_DebugObject_non_requested_" + matsSocket.id(6), {},
                    function (receivedEvent) {
                        standardStateAssert();
                        receivedEventReceived = receivedEvent;
                    })
                    .then(function (messageEvent) {
                        standardStateAssert();
                        chai.assert.isDefined(receivedEventReceived);
                        chai.assert.isUndefined(messageEvent.debug);
                        done();
                    });
            });

            it("When the user is allowed to debug, but do not request any debug (0), there should not be a debug object", function (done) {
                // Set special userId that gives us all DebugOptions
                setAuth('enableAllDebugOptions');

                matsSocket.debug = 0;

                let receivedEventReceived;
                matsSocket.request("Test.single", "Request_DebugObject_non_requested_" + matsSocket.id(6), {},
                    function (receivedEvent) {
                        standardStateAssert();
                        receivedEventReceived = receivedEvent;
                    })
                    .then(function (messageEvent) {
                        standardStateAssert();
                        chai.assert.isDefined(receivedEventReceived);
                        chai.assert.isUndefined(messageEvent.debug);
                        done();
                    });
            });
        });
    });
}));