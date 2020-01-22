// Register as an UMD module - source: https://github.com/umdjs/umd/blob/master/templates/commonjsStrict.js
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        // AMD. Register as an anonymous module.
        define(['chai', 'sinon', 'ws', 'mats'], factory);
    } else if (typeof exports === 'object' && typeof exports.nodeName !== 'string') {
        // CommonJS
        const chai = require('chai');
        const sinon = require('sinon');
        const ws = require('ws');
        const mats = require('../lib/MatsSocket');

        factory(chai, sinon, ws, mats);
    } else {
        // Browser globals
        factory(chai, sinon, WebSocket, mats);
    }
}(typeof self !== 'undefined' ? self : this, function (chai, sinon, ws, mats) {
    const MatsSocket = mats.MatsSocket;

    describe('MatsSocket unit-tests', function () {
        describe('constructor', function () {
            it('Should fail on no arg invocation', function () {
                chai.assert.throws(() => new MatsSocket());
            });

            it('Should fail if appVersion and urls are missing', function () {
                chai.assert.throws(() => new MatsSocket("Test"));
            });

            it('Should fail if urls are missing', function () {
                chai.assert.throws(() => new MatsSocket("Test", "1.0"));
            });
            it('Should not invoke the WebSocket constructor', function () {
                const callback = sinon.spy(ws);
                new MatsSocket("Test", "1.0", ["ws://localhost:8080/"]);
                chai.assert(!callback.called);
                sinon.reset();
            });
        });

        describe('authorization', function () {
            // :: Make mock WebSocket
            const webSocket = {};
            // Make send function:
            webSocket.send = function (payload) {
                JSON.parse(payload).forEach(({t, cmseq}, idx) => {
                    if (t === 'HELLO') {
                        setTimeout(() => {
                            webSocket.onmessage({data: JSON.stringify([{t: "WELCOME"}])});
                        }, idx);
                    }
                    if (cmseq !== undefined) {
                        setTimeout(() => {
                            webSocket.onmessage({data: JSON.stringify([{t: "RECEIVED", cmseq: cmseq, st: "ACK"}])});
                        }, idx);
                    }
                });
            };
            webSocket.close = function() {};

            // Make 'ononpen' property, which is set twice by MatsSocket: Once when it waits for it to open, and when this happens, it is "unset" (set to undefined)
            Object.defineProperty(webSocket, "onopen", {
                set(callback) {
                    // When callback is set, immediately invoke it on next tick (fast opening times on this mock WebSockets..!)
                    if (callback !== undefined) {
                        setTimeout(() => callback({}), 0);
                    }
                }
            });

            it('Should invoke authorization callback before making calls', async function () {
                const matsSocket = new MatsSocket("Test", "1.0", ["ws://localhost:8080/"], () => webSocket);

                let authCallbackCalled = false;

                matsSocket.setAuthorizationExpiredCallback(function (event) {
                    authCallbackCalled = true;
                    matsSocket.setCurrentAuthorization("Test", Date.now() + 20000, 0);
                });
                await matsSocket.send("Test.authCallback", "SEND_" + matsSocket.id(6), {});

                chai.assert(authCallbackCalled);
            });

            it('Should not invoke authorization callback if authorization present', async function () {
                const matsSocket = new MatsSocket("Test", "1.0", ["ws://localhost:8080/"], () => webSocket);

                let authCallbackCalled = false;
                matsSocket.setCurrentAuthorization("Test", Date.now() + 20000, 0);
                matsSocket.setAuthorizationExpiredCallback(function (event) {
                    authCallbackCalled = true;
                });
                await matsSocket.send("Test.authCallback", "SEND_" + matsSocket.id(6), {});

                chai.assert(!authCallbackCalled);
            });

            it('Should invoke authorization callback when expired', async function () {
                const matsSocket = new MatsSocket("Test", "1.0", ["ws://localhost:8080/"], () => webSocket);

                let authCallbackCalled = false;

                matsSocket.setCurrentAuthorization("Test", Date.now() - 20000, 0);
                matsSocket.setAuthorizationExpiredCallback(function (event) {
                    authCallbackCalled = true;
                    matsSocket.setCurrentAuthorization("Test", Date.now() + 20000, 0);
                });
                await matsSocket.send("Test.authCallback", "SEND_" + matsSocket.id(6), {});

                chai.assert(authCallbackCalled);
            });

            it('Should invoke authorization callback when room for latency expired', async function () {
                const matsSocket = new MatsSocket("Test", "1.0", ["ws://localhost:8080/"], () => webSocket);

                let authCallbackCalled = false;

                matsSocket.setCurrentAuthorization("Test", Date.now() + 1000, 2000);
                matsSocket.setAuthorizationExpiredCallback(function (event) {
                    authCallbackCalled = true;
                    matsSocket.setCurrentAuthorization("Test", Date.now() + 20000, 0);
                });
                await matsSocket.send("Test.authCallback", "SEND_" + matsSocket.id(6), {});

                chai.assert(authCallbackCalled);
            });
        });
    });

}));