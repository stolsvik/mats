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

    let logging = false;

    let matsSocket;

    function createMatsSocket() {
        matsSocket = new MatsSocket("TestApp", "1.2.3", availableUrls);
        matsSocket.logging = logging;
    }

    function closeMatsSocket() {
        // :: Chill the close slightly, so as to get the final "ACK2" envelope over to delete server's inbox.
        // NOTE: This is done async, so for the fast tests, the closings will come in "behind".
        let toClose = matsSocket;
        setTimeout(function () {
            toClose.close("Test done");
        }, 500);
    }

    function setAuth(userId = "standard", duration = 20000, roomForLatencyMillis = 10000) {
        const now = Date.now();
        const expiry = now + duration;
        matsSocket.setCurrentAuthorization("DummyAuth:" + userId + ":" + expiry, expiry, roomForLatencyMillis);
    }

    const availableUrls = (env.MATS_SOCKET_URLS || "ws://localhost:8080/matssocket,ws://localhost:8081/matssocket").split(",");


    describe('MatsSocket integration tests of connect, reconnect and close', function () {
        /*
         * NOTE!!! This test runs a scenario where two MatsSockets connect using the same MatsSocketSessionId. This is
         * NOT a situation that shall occur in actual use - you are NOT supposed to use the sessionId outside of the
         * MatsSocket instance - i.e. any given MatsSocket instance shall have a different sessionId than any other
         * MatsSocket instance.
         *
         * The test tries to emulate a situation where the MatsSocket client instance realizes that
         * the connection is broken, and starts to reconnect - while at the same time the Server has not realized this,
         * and thus still have the WebSocket connection open. In this situation, the Server will experience this as a
         * new WebSocket session firing up, and which tries to do a MatsSocket HELLO "Reconnect to existing SessionId",
         * while the Server still has a WebSocket session that already claims to be that MatsSocketSessionId. What the
         * Server is programmed to do, is to kill the existing socket with close code DISCONNECT, and then register the
         * existing MatsSocketSessionId to this new connection.
         *
         * What this test fails to actually exercise, is what happens if this scenario actually occurs *within a single
         * MatsSocket instance*: Does an outstanding Request that takes some time to finish actually resolve when the
         * situation has settled (i.e. when the new WebSocket connection has taken over, and the old killed)?
         *
         * If you have an idea of how to test this scenario in a better way, then please do so! Maybe using the unit
         * test harness?
         */
        describe('connect twice to same MatsSocketSessionId', function () {

            function connectTwice(url1, url2, done) {
                // Create the first MatsSocket
                let matsSocket_A = new MatsSocket("TestApp", "1.2.3", url1);
                matsSocket_A.logging = logging;
                const now = Date.now();
                const expiry = now + 20000;
                matsSocket_A.setCurrentAuthorization("DummyAuth:standard:" + expiry, expiry, 5000);

                let matsSocket_A_SessionClosed = 0;
                matsSocket_A.addSessionClosedEventListener(function (closeEvent) {
                    matsSocket_A_SessionClosed++;
                });

                let matsSocket_A_LostConnection = undefined;
                let matsSocket_A_LostConnection_Count = 0;
                matsSocket_A.addConnectionEventListener(function (connectionEvent) {
                    if (connectionEvent.type === mats.ConnectionEventType.LOST_CONNECTION) {
                        matsSocket_A_LostConnection_Count++;
                        matsSocket_A_LostConnection = connectionEvent;
                    }
                });


                // Create a second MatsSocket, that will get the same MatsSocketSessionId as the first.
                let matsSocket_B = new MatsSocket("TestApp", "1.2.3", url2);
                matsSocket_B.logging = matsSocket_A.logging;
                matsSocket_B.setCurrentAuthorization("DummyAuth:standard:" + expiry, expiry, 5000);

                let matsSocket_B_SessionClosed = 0;
                matsSocket_B.addSessionClosedEventListener(function (closeEvent) {
                    matsSocket_B_SessionClosed++;
                });

                let matsSocket_B_LostConnection_Count = 0;
                matsSocket_B.addConnectionEventListener(function (connectionEvent) {
                    if (connectionEvent.type === mats.ConnectionEvent.LOST_CONNECTION) {
                        matsSocket_B_LostConnection_Count++;
                    }
                });


                // First SEND a message to instance A, so as to perform HELLO and get a WELCOME with sessionId
                function firstStep() {
                    matsSocket_A.send("Test.ignoreInIncomingHandler", "SEND_twiceConnect_ensureAssignedSessionId_" + matsSocket_A.id(6), {})
                        .then(function (receivedEvent) {
                            // Assert that the Resolve is MessageEventType.REPLY
                            chai.assert.strictEqual(receivedEvent.type, mats.ReceivedEventType.ACK, "xxx");
                            // Assert that the SessionClosedEvent listener was NOT invoked
                            chai.assert.strictEqual(matsSocket_A_SessionClosed, 0, "SessionClosedEvent listener should NOT have been invoked!");
                            // Do next step outside this handler - after a brief delay, so as to let our "delayed ACK/NACK/ACK2 compaction" work its magic.
                            setTimeout(secondStep, 150);
                        });
                }

                // Now, give this sessionId to instance B, and then send a REQUEST with this instance
                // This leads to HELLO->WELCOME, and that the server closes instance A with a DISCONNECT
                // NOTE: This shall happen both if the second connection is to the same node (server instance) as the first, or to a different node.
                function secondStep() {
                    // The MatsSocket has now gotten its SessionId, which we've sneakily made available on matsSocket_A.sessionId
                    // .. Set the existing sessionId on this new MatsSocket
                    matsSocket_B.sessionId = matsSocket_A.sessionId;

                    /* Now perform a request using matsSocket_B, which will "start" that MatsSocket instance,
                     * and hence perform a "HELLO(RECONNECT)" to the existing MatsSocketSessionId
                     * This should lead to the existing matsSocket_A being thrown out.
                     */
                    // :: Request to a MatsSocket->Mats service, ASAP reply
                    let req = {
                        string: "test twice connect",
                        number: 42,
                        sleepTime: 0
                    };
                    let receivedCallbackInvoked = 0;
                    matsSocket_B.request("Test.simpleMats", "REQUEST_twiceConnect_requestOnNewMatsSocket" + matsSocket_A.id(6), req,
                        function () {
                            receivedCallbackInvoked++;
                        })
                        .then(reply => {
                            let data = reply.data;
                            // Assert that we got receivedCallback ONCE
                            chai.assert.strictEqual(receivedCallbackInvoked, 1, "Should have gotten one, and only one, receivedCallback.");
                            // Assert data, with the changes from Server side.
                            chai.assert.strictEqual(data.string, req.string + ":FromSimpleMats");
                            chai.assert.strictEqual(data.number, req.number);
                            chai.assert.strictEqual(data.sleepTime, req.sleepTime);
                            thirdStep_Asserts();
                        });
                }

                // Finally, assert lots of state and events that shall and shall not have happened.
                function thirdStep_Asserts() {
                    setTimeout(function () {
                        // instance A should NOT be connected
                        chai.assert.isFalse(matsSocket_A.connected, "Instance A should not be connected now!");
                        // .. but it should however still be in state ConnectionState.SESSION_ESTABLISHED (which is a bit weird, but think of the situation where this happens *within a single MatsSocket instance*.
                        chai.assert.strictEqual(matsSocket_A.state, mats.ConnectionState.SESSION_ESTABLISHED, "Instance A should still be in ConnectionState.SESSION_ESTABLISHED");
                        // .. and it should NOT have received a SessionClosedEvent (because the MatsSocketSession is NOT closed, it is just the WebSocket connection that has gone down).
                        chai.assert.strictEqual(matsSocket_A_SessionClosed, 0, "SessionClosedEvent listener for instance A should NOT have been invoked!");
                        // .. but is SHOULD have received a single ConnectionEventType.LOST_CONNECTION (because that is definitely what has happened)
                        chai.assert.strictEqual(matsSocket_A_LostConnection_Count, 1, "Instance A: ConnectionEventType.LOST_CONNECTION should have come, once");
                        // .. that has the webSocketEvent set.
                        chai.assert.strictEqual(matsSocket_A_LostConnection.webSocketEvent.code, mats.MatsSocketCloseCodes.DISCONNECT, "Instance A: ConnectionEventType.LOST_CONNECTION should have webSocketEvent, and its code should be MatsSocketCloseCodes.DISCONNECT.");
                        chai.assert(matsSocket_A_LostConnection.webSocketEvent.reason.includes("same MatsSocketSessionId"), "Instance A: ConnectionEventType.LOST_CONNECTION should have webSocketEvent, and should say something 'same MatsSocketSessionId'.");

                        // instance B SHOULD be connected
                        chai.assert.isTrue(matsSocket_B.connected, "Instance B should be connected now!");
                        // .. and it should have state ConnectionState.SESSION_ESTABLISHED
                        chai.assert.strictEqual(matsSocket_B.state, mats.ConnectionState.SESSION_ESTABLISHED, "Instance B should be in ConnectionState.SESSION_ESTABLISHED");
                        // .. and it should NOT have received a SessionClosedEvent
                        chai.assert.strictEqual(matsSocket_B_SessionClosed, 0, "SessionClosedEvent listener for instance B should NOT have been invoked!");
                        // .. and it should NOT have received a ConnectionEventType.LOST_CONNECTION
                        chai.assert.strictEqual(matsSocket_B_LostConnection_Count, 0, "Instance B: ConnectionEventType.LOST_CONNECTION should NOT have come");

                        // Now close instance B - after a brief delay, so as to let our "delayed ACK/NACK/ACK2 compaction" work its magic.
                        setTimeout(function () {
                            matsSocket_B.close("Twice-connect test done - instance A!");

                            // .. which should NOT fire SessionClosed (since client side close())
                            chai.assert.strictEqual(matsSocket_B_SessionClosed, 0, "SessionClosedEvent listener for instance B should NOT have been invoked!");
                            // .. but it should NOT be connected anymore
                            chai.assert.isFalse(matsSocket_B.connected, "Instance A should not be connected now!");
                            // .. and state should be NO_SESSION
                            chai.assert.strictEqual(matsSocket_B.state, mats.ConnectionState.NO_SESSION, "Instance B should, after close, be in ConnectionState.NO_SESSION");

                            // Just to clean up, also close instance A - but this instance is pretty much dead anyway, and the MatsSocketSession is closed on server after close of instance B
                            matsSocket_A.close("Twice-connect test done - instance A!");

                            done();
                        }, 150);
                    });
                }

                // Kick it off
                firstStep();
            }

            it("Connect twice to same Server - the second connection should result in the first being killed.", function (done) {
                connectTwice(availableUrls[0], availableUrls[0], done);
            });

            it("Connect twice to different Server - the second connection should result in the first being killed.", function (done) {
                connectTwice(availableUrls[0], availableUrls[1], done);
            });
        });


        describe('reconnect', function () {
            // Create Socket before each request
            beforeEach(() => {
                createMatsSocket();
            });
            afterEach(() => {
                closeMatsSocket();
            });

            function reconnectTest(done, whichTest, serverEndpointId, expectedServerChange) {
                setAuth();

                let reconnectDone = false;
                matsSocket.addConnectionEventListener(function (connectionEvent) {
                    // Waiting for state transition. "CONNECTED" is too early, as it doesn't get to send any messages at all
                    // but with SESSION_ESTABLISHED, it is pretty spot on. (MatsSocket authorized and fully established).
                    if (!reconnectDone && (connectionEvent.type === mats.ConnectionEventType.SESSION_ESTABLISHED)) {
                        matsSocket.reconnect("Integration-test, testing reconnects " + whichTest);
                        reconnectDone = true;
                    }
                });

                let req = {
                    string: "test",
                    number: 15,
                    sleepTime: 150
                };
                let receivedCallbackInvoked = 0;
                // Request to a service that will reply either AFTER A DELAY or IMMEDIATELY
                matsSocket.request(serverEndpointId, "REQUEST_reconnect_" + whichTest + "_" + matsSocket.id(6), req,
                    function () {
                        receivedCallbackInvoked++;
                    })
                    .then(reply => {
                        let data = reply.data;
                        // Assert that we got receivedCallback ONCE
                        chai.assert.strictEqual(receivedCallbackInvoked, 1, "Should have gotten one, and only one, receivedCallback.");
                        // Assert that the reconnect() was done.
                        chai.assert(reconnectDone, "Should have run reconnect().");
                        // Assert data, with the changes from Server side.
                        chai.assert.strictEqual(data.string, req.string + expectedServerChange);
                        chai.assert.strictEqual(data.number, req.number);
                        chai.assert.strictEqual(data.sleepTime, req.sleepTime);
                        done();
                    });
            }

            it('request to "slow endpoint", then immediate reconnect() upon SESSION_ESTABLISHED. Tests that we get the RESOLVE when we reconnect.', function (done) {
                reconnectTest(done, "slowEndpoint", "Test.slow", ":FromSlow");
            });

            it('request with immediate resolve in handleIncoming(..), then immediate reconnect() upon SESSION_ESTABLISHED. Tests that we get the RESOLVE when we reconnect.', function (done) {
                reconnectTest(done, "replyHandleIncoming", "Test.resolveInIncomingHandler", ":From_resolveInIncomingHandler");
            });

            it('Connect the MatsSocket, then DISCONNECT it (i.e. not starting reconnect), then send message to force reconnect anyway, then done.', function (done) {
                setAuth();
                this.timeout(20000);

                /**
                 * Note: This test was created to test the introspection of matsSocketServer.getMatsSocketSessions():
                 * First a session should appear having NodeName set, then the session should still exist, but have 'null' NodeName,
                 * and then the same session should come back with NodeName - and finally it should close and thus not exist anymore.
                 *
                 * Set CHILLTIME to 2000 or so, run the test, and then repeatedly reload the introspection view to observe this.
                 */

                let CHILLTIME = 100;

                matsSocket.logging = false;

                // Run a SEND just to get the show going
                matsSocket.send("Test.ignoreInIncomingHandler", "SEND_to_" + matsSocket.id(6), {})
                    .then(_ => {
                        step2A_Chill();
                    });

                function step2A_Chill() {
                    setTimeout(step2B_Disconnect, CHILLTIME);
                }

                function step2B_Disconnect() {
                    matsSocket.reconnect("Running DISCONNECT", true);
                    step3A_Chill();
                }

                function step3A_Chill() {
                    setTimeout(step3B_SendToReconnect, CHILLTIME);
                }

                function step3B_SendToReconnect() {
                    let promise = matsSocket.send("Test.ignoreInIncomingHandler", "SEND_to_" + matsSocket.id(6), {});
                    promise.then(_ => {
                        step4A_Chill();
                    });
                }

                function step4A_Chill() {
                    setTimeout(step4B_Done, CHILLTIME);
                }

                function step4B_Done() {
                    done();
                }
            });


            it('reconnect with a different resolved userId should fail', function (done) {
                // MatsSocket emits an error upon SessionClose. Annoying when testing this, so send to /dev/null.
                // (Restored right before done())
                let originalConsoleError = console.error;
                console.error = function (msg, obj) { /* ignore */
                };
                // First authorize with 'Endre' as userId - in the ConnectionEvent listener right below we change this to "Stølsvik".
                setAuth("Endre");

                // :: We add a ConnectionEventListener that listens for SESSION_ESTABLISHED (fully open, authorized MatsSocket) and then does immediate reconnect().
                matsSocket.addConnectionEventListener(function (connectionEvent) {
                    // ?: Is this the SESSION_ESTABLISHED event?
                    if (connectionEvent.type === mats.ConnectionEventType.SESSION_ESTABLISHED) {
                        // -> Yes, so change the authentication, and then reconnect - which should fail.
                        setAuth("Stølsvik");
                        matsSocket.reconnect("Integration-test, testing reconnects with different user");
                    }
                });

                // :: We add a SessionClosedEvent listener, which should be invoked once the changed authorization from above fails on Server side.
                let sessionClosed = 0;
                matsSocket.addSessionClosedEventListener(function (closeEvent) {
                    // Note: The CloseEvent here is WebSocket's own CloseEvent
                    // This reason for this should be VIOLATED_POLICY, i.e. "authentication failures".
                    chai.assert.strictEqual(closeEvent.code, mats.MatsSocketCloseCodes.VIOLATED_POLICY);
                    // .. also assert our prototype-extension of WebSocket CloseEvent with property "codeName"
                    if (typeof CloseEvent !== 'undefined') {
                        // ^ guard against Node.js, which evidently does not have CloseEvent..
                        chai.assert.strictEqual(closeEvent.codeName, "VIOLATED_POLICY");
                    }
                    // Assert that the reason string has interesting prose
                    chai.assert(closeEvent.reason.includes("does not match"), "Reason string should something about existing user 'does not match' the new user.");
                    sessionClosed++;
                });

                // When we get a SessionClosed from the Server, outstanding initiations are NACKed / Rejected
                let receivedCallbackInvoked = 0;
                matsSocket.request("Test.resolveInIncomingHandler", "REQUEST_reconnect1_" + matsSocket.id(6), {},
                    function (event) {
                        receivedCallbackInvoked++;
                    })
                    .catch(reply => {
                        // Assert that we got the Received-callback for the Request
                        chai.assert.strictEqual(receivedCallbackInvoked, 1, "ReceivedCallback should have been invoked, once.");
                        // Assert that the Session was closed
                        chai.assert.strictEqual(sessionClosed, 1, "SessionClosedEvent listener should have been invoked, once.");
                        // Restore console's error.
                        console.error = originalConsoleError;
                        // This went well.
                        done();
                    });
            });
        });

        describe("client close", function () {
            // Create MatsSocket and set a valid authorization before each request
            beforeEach(() => {
                createMatsSocket();
                setAuth();
            });

            it("send: returned Promise should reject with SESSION_CLOSED if closed before ack. Also, SessionClosedEvent listener should NOT be invoked.", function (done) {
                // :: We add a SessionClosedEvent listener, which should NOT be invoked when we close from Client side
                let sessionClosed = 0;
                matsSocket.addSessionClosedEventListener(function (closeEvent) {
                    sessionClosed++;
                });

                matsSocket.send("Test.single", "SEND_should_reject_on_close_" + matsSocket.id(6), {})
                    .catch(function (messageEvent) {
                        // Assert that the Reject is MessageEventType.SESSION_CLOSED
                        chai.assert.strictEqual(messageEvent.type, mats.MessageEventType.SESSION_CLOSED, "Send's Promise's Reject Event should be a MessageEventType.SESSION_CLOSED.");
                        // Assert that the SessionClosedEvent listener was NOT invoked
                        chai.assert.strictEqual(sessionClosed, 0, "SessionClosedEvent listener should NOT have been invoked!");
                        done();
                    });
                // Immediate close
                matsSocket.close("testing that client.close() rejects send()'s Promise");
            });

            it("request: receivedCallback should give SESSION_CLOSED, and returned Promise should reject with SESSION_CLOSED, if closed before ack. Also, SessionClosedEvent listener should NOT be invoked.", function (done) {
                // :: We add a SessionClosedEvent listener, which should NOT be invoked when we close from Client side
                let sessionClosed = 0;
                matsSocket.addSessionClosedEventListener(function (closeEvent) {
                    sessionClosed++;
                });

                // When we close session from Client side, receivedCallback should be invoked with SESSION_CLOSED, and Promise should reject with SESSION_CLOSED
                let receivedCallbackInvoked = 0;
                matsSocket.request("Test.single", "REQUEST_should_reject_on_close_" + matsSocket.id(6), {},
                    function (receivedEvent) {
                        chai.assert.strictEqual(receivedEvent.type, mats.ReceivedEventType.SESSION_CLOSED, "receivedCallback should get a ReceivedEventType.SESSION_CLOSED.");
                        receivedCallbackInvoked++;
                    })
                    .catch(function (messageEvent) {
                        // Assert that the Reject is MessageEventType.SESSION_CLOSED
                        chai.assert.strictEqual(messageEvent.type, mats.MessageEventType.SESSION_CLOSED, "Request's Promise's Reject Event should be a MessageEventType.SESSION_CLOSED, not [" + messageEvent.type + "].");
                        // Assert that we got the Received-callback for the Request
                        chai.assert.strictEqual(receivedCallbackInvoked, 1, "ReceivedCallback should have been invoked, once.");
                        // Assert that the SessionClosedEvent listener was NOT invoked
                        chai.assert.strictEqual(sessionClosed, 0, "SessionClosedEvent listener should NOT have been invoked!");
                        done();
                    });
                // Immediate close
                matsSocket.close("testing that client.close() rejects request()'s Promise");
            });
        });

        describe("server close", function () {
            // Create MatsSocket and set a valid authorization before each request
            beforeEach(() => {
                createMatsSocket();
                setAuth();
            });

            it("Sends a message to a Server endpoint, which in a Thread closes this session - which should issue a SessionClosedEvent, and the closeEvent.code should be MatsSocketCloseCodes.CLOSE_SESSION.", function (done) {
                // :: We add a SessionClosedEvent listener, which will be invoked when we MatsSocket endpoint closes us.
                let sessionClosed = 0;
                matsSocket.addSessionClosedEventListener(function (closeEvent) {
                    chai.assert.equal(closeEvent.code, mats.MatsSocketCloseCodes.CLOSE_SESSION, "WebSocket CloseEvent's CloseCode should be MatsSocketCloseCodes.CLOSE_SESSION");
                    chai.assert.equal(closeEvent.codeName, "CLOSE_SESSION", "WebSocket CloseEvent should have 'codeName'==\"CLOSE_SESSION\".");
                    chai.assert.isFalse(matsSocket.connected, "MatsSocket should NOT be connected");
                    chai.assert.equal(matsSocket.state, mats.ConnectionState.NO_SESSION, "MatsSocket.state should be ConnectionState.NO_SESSION");
                    sessionClosed++;
                });

                let sendReceivedAck = false;
                matsSocket.send("Test.closeThisSession", "SEND_server.CloseThisSession_" + matsSocket.id(6), {})
                    .then(receivedEvent => {
                        chai.assert.strictEqual(receivedEvent.type, mats.ReceivedEventType.ACK, "Send's Promise's should be resolved with ACK.");
                        sendReceivedAck = true;
                    });

                // Make a Request that should be aborted
                let req = {
                    string: "test",
                    number: -1,
                    sleepTime: 500 // Sleeptime before replying - *more* than the wait on server side to server.closeSession(..)
                };
                let receivedCallbackInvoked = false;
                matsSocket.request("Test.slow", "REQUEST_server.CloseThisSession_" + matsSocket.id(6), req, {
                    receivedCallback: function (event) {
                        chai.assert.strictEqual(event.type, mats.ReceivedEventType.ACK);
                        receivedCallbackInvoked = true;
                    }
                }).catch(messageEvent => {
                    chai.assert.equal(messageEvent.type, mats.MessageEventType.SESSION_CLOSED);
                    chai.assert.isTrue(receivedCallbackInvoked);
                    chai.assert.isTrue(sendReceivedAck);
                    chai.assert.equal(sessionClosed, 1, "SessionCloseEvent should have come 1 time.");
                    done();
                });
            });
        });
    });
}));