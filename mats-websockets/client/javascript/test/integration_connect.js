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

    describe('MatsSocket integration tests of connect, reconnect and close', function () {
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
            // :: Chill the close slightly, so as to get the final "ACKACK" envelope over to delete server's inbox.
            let toClose = matsSocket;
            setTimeout(function () {
                toClose.close("Test done");
            }, 25);
        });

        describe('reconnect', function () {
            it('request to "slow endpoint", then immediate reconnect() upon SESSION_ESTABLISHED. Tests that we get the RESOLVE when we reconnect.', function (done) {
                setAuth();

                let firstTime = true;
                let killSocket = function (connectionEvent) {
                    // Waiting for state. "CONNECTED" is too early, as it doesn't get to send any messages at all
                    // but with SESSION_ESTABLISHED, it is pretty spot on.
                    if (firstTime && (connectionEvent.state === mats.ConnectionState.SESSION_ESTABLISHED)) {
                        matsSocket.reconnect("Integration-test, testing reconnects A");
                        firstTime = false;
                    }
                };

                matsSocket.addConnectionEventListener(killSocket);

                let req = {
                    string: "test",
                    number: 15,
                    sleepTime: 250
                };
                // Request to a service that will reply AFTER A DELAY
                matsSocket.request("Test.slow", "REQUEST_reconnect_A_" + matsSocket.id(6), req)
                    .then(reply => {
                        let data = reply.data;
                        chai.assert.strictEqual(data.string, req.string + ":FromSlow");
                        chai.assert.strictEqual(data.number, req.number);
                        chai.assert.strictEqual(data.sleepTime, req.sleepTime);
                        done();
                    });
            });

            it('request that resolves in handleIncoming(..), then immediate reconnect() upon SESSION_ESTABLISHED. Tests that we get the RESOLVE when we reconnect..', function (done) {
                setAuth();

                let firstTime = true;
                let killSocket = function (connectionEvent) {
                    // Waiting for state. "CONNECTED" is too early, as it doesn't get to send any messages at all
                    // but with SESSION_ESTABLISHED, it is pretty spot on.
                    if (firstTime && (connectionEvent.state === mats.ConnectionState.SESSION_ESTABLISHED)) {
                        matsSocket.reconnect("Integration-test, testing reconnects B");
                        firstTime = false;
                    }
                };

                matsSocket.addConnectionEventListener(killSocket);

                let req = {
                    string: "test",
                    number: 15,
                    sleepTime: -1
                };
                // Request to a service that will reply immediately (in handleIncoming)
                matsSocket.request("Test.resolveInIncomingHandler", "REQUEST_reconnectB_" + matsSocket.id(6), req)
                    .then(reply => {
                        let data = reply.data;
                        chai.assert.strictEqual(data.string, req.string + ":From_resolveInIncomingHandler");
                        chai.assert.strictEqual(data.number, req.number);
                        chai.assert.strictEqual(data.sleepTime, req.sleepTime);
                        done();
                    });
            });

            it('reconnect with a different resolved userId should fail', function (done) {
                // MatsSocket emits an error upon SessionClose. Annoying when testing this, so send to /dev/null.
                // (Restored right before done())
                let originalConsoleError = console.error;
                console.error = function (msg, obj) { /* ignore */
                };
                // First authorize with 'Endre' as userId - in the ConnectionEvent listener we change this.
                setAuth("Endre");

                let firstTime = true;
                let killSocket = function (connectionEvent) {
                    if (firstTime && (connectionEvent.state === mats.ConnectionState.SESSION_ESTABLISHED)) {
                        setAuth("Stølsvik");
                        matsSocket.reconnect("Integration-test, testing reconnects with different user");
                        firstTime = false;
                    }
                };

                matsSocket.addConnectionEventListener(killSocket);

                let sessionClosed = false;

                // The CloseEvent here is WebSocket's own CloseEvent
                matsSocket.addSessionClosedEventListener(function (closeEvent) {
                    chai.assert.strictEqual(closeEvent.code, mats.MatsSocketCloseCodes.VIOLATED_POLICY);
                    chai.assert(closeEvent.reason.includes("does not match"), "Reason string should something about existing user 'does not match' the new user.");
                    sessionClosed = true;
                });

                let receivedCallbackInvoked = false;

                // When we get a SessionClosed from the Server, outstanding initiations are NACKed / Rejected
                matsSocket.request("Test.resolveInIncomingHandler", "REQUEST_reconnect1_" + matsSocket.id(6), {},
                    function (event) {
                        receivedCallbackInvoked = true;
                    })
                    .catch(reply => {
                        chai.assert(receivedCallbackInvoked, "ReceivedCallback should have been invoked.");
                        chai.assert(sessionClosed, "SessionClosedEvent listener should have been invoked.");
                        // Restore console's error.
                        console.error = originalConsoleError;
                        done();
                    });
            });


            // TODO: Test two MatsSockets to same SessionId - the first one should be closed. Which close code?
        });

        describe("client close", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            it("send: Promise should reject if closed before ack", function (done) {
                let promise = matsSocket.send("Test.single", "SEND_should_reject_on_close_" + matsSocket.id(6), {
                    string: "The String",
                    number: Math.PI
                });
                promise.catch(function (event) {
                    done();
                });
                matsSocket.close("testing close rejects");
            });

            it("request: Promise should reject if closed before ack", function (done) {
                let promise = matsSocket.request("Test.single", "REQUEST_should_reject_on_close_" + matsSocket.id(6), {
                    string: "The String",
                    number: Math.PI
                });
                promise.catch(function (event) {
                    done();
                });
                matsSocket.close("testing close rejects");
            });
        });
    });
}));