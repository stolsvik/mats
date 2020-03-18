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

    describe('MatsSocket integration tests of Authentication & Authorization', function () {
        let matsSocket;

        function setAuth(userId = "standard", expirationTimeMillisSinceEpoch = 20000, roomForLatencyMillis = 10000) {
            const now = Date.now();
            const expiry = now + expirationTimeMillisSinceEpoch;
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

        describe('authorization callbacks', function () {
            it('Should invoke authorization callback before making calls', function (done) {
                let authCallbackCalled = false;

                matsSocket.setAuthorizationExpiredCallback(function (event) {
                    authCallbackCalled = true;
                    setAuth();
                });
                matsSocket.send("Test.single", "SEND_" + matsSocket.id(6), {})
                    .then(reply => {
                        chai.assert(authCallbackCalled);
                        done();
                    });
            });

            it('Should not invoke authorization callback if authorization present', function (done) {
                let authCallbackCalled = false;
                setAuth();
                matsSocket.setAuthorizationExpiredCallback(function (event) {
                    authCallbackCalled = true;
                });
                matsSocket.send("Test.single", "SEND_" + matsSocket.id(6), {})
                    .then(reply => {
                        chai.assert(!authCallbackCalled);
                        done();
                    });
            });

            it('Should invoke authorization callback when expired', function (done) {
                let authCallbackCalled = false;
                setAuth("standard", -20000);
                matsSocket.setAuthorizationExpiredCallback(function (event) {
                    authCallbackCalled = true;
                    setAuth();
                });
                matsSocket.send("Test.single", "SEND_" + matsSocket.id(6), {})
                    .then(reply => {
                        chai.assert(authCallbackCalled);
                        done();
                    });

            });

            it('Should invoke authorization callback when room for latency expired', function (done) {
                let authCallbackCalled = false;
                // Immediately timed out.
                setAuth("standard", 1000, 10000);
                matsSocket.setAuthorizationExpiredCallback(function (event) {
                    authCallbackCalled = true;
                    setAuth();
                });
                matsSocket.send("Test.single", "SEND_" + matsSocket.id(6), {})
                    .then(reply => {
                        chai.assert(authCallbackCalled);
                        done();
                    });
            });
        });

        function testIt(userId, done) {
            setAuth(userId, 2000, 0);

            let authCallbackCalledCount = 0;
            let authCallbackCalledEventType = undefined;
            matsSocket.setAuthorizationExpiredCallback(function (event) {
                authCallbackCalledCount ++;
                authCallbackCalledEventType = event.type;
                // This standard auth does not fail reevaluateAuthentication.
                setAuth();
            });
            let req = {
                string: "test",
                number: 15,
                sleepTime: 10
            };
            let receivedCallbackInvoked = 0;
            // Request to a service that will reply AFTER A DELAY that is long enough that auth shall be expired!
            matsSocket.request("Test.slow", "REQUEST_authentication_from_server_" + matsSocket.id(6), req,
                function () {
                    receivedCallbackInvoked++;
                })
                .then(reply => {
                    let data = reply.data;
                    // Assert that we got receivedCallback ONCE
                    chai.assert.strictEqual(receivedCallbackInvoked, 1, "Should have gotten one, and only one, receivedCallback.");
                    // Assert that we got AuthorizationExpiredEventType.REAUTHENTICATE, and only one call to Auth.
                    chai.assert.strictEqual(authCallbackCalledEventType, mats.AuthorizationRequiredEventType.REAUTHENTICATE, "Should have gotten AuthorizationRequiredEventType.REAUTHENTICATE authorizationExpiredCallback.");
                    chai.assert.strictEqual(authCallbackCalledCount, 1, "authorizationExpiredCallback should only have been invoked once");
                    // Assert data, with the changes from Server side.
                    chai.assert.strictEqual(data.string, req.string + ":FromSlow");
                    chai.assert.strictEqual(data.number, req.number);
                    chai.assert.strictEqual(data.sleepTime, req.sleepTime);
                    done();
                });
        }

        describe('authorization invalid when Server about to receive or send out information bearing message', function () {
            it('Receive: Using special userId which DummyAuthenticator fails on step reevaluateAuthentication(..), Server should ask for REAUTH when we perform Request, and when gotten, resolve w/o retransmit (server "holds" message).', function (done) {
                // Using special "userId" for DummySessionAuthenticator that specifically fails @ reevaluateAuthentication(..) step
                testIt("fail_reevaluateAuthentication", done);
            });

            it('Send: Request with delay long enough before Reply so that Authorization expires shall have Server require REAUTH from Client', function (done) {
                // Using special "userId" for DummySessionAuthenticator that specifically fails @ reevaluateAuthenticationForOutgoingMessage(..) step
                testIt("fail_reevaluateAuthenticationForOutgoingMessage", done);
            });
        });
    });
}));