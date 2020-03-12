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
                    })
            });
        });

        // TODO: Check for auth coming from server

        describe('authorization serverside', function () {
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
        });
    });
}));